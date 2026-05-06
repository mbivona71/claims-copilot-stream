/* eslint-disable no-undef */
/**
 * Twilio Media Streams → Deepgram → Base44 (via ingestTranscript function)
 * Deploy on Render as a Web Service (Node.js)
 *
 * Required environment variables:
 *   DEEPGRAM_API_KEY   - from console.deepgram.com
 *   BASE44_INGEST_URL  - URL of the Base44 ingestTranscript backend function
 *   INGEST_SECRET      - shared secret to authenticate calls to Base44
 *   PORT               - set automatically by Render
 */

const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const { createClient } = require('@deepgram/sdk');

const app = express();
const server = http.createServer(app);

const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const BASE44_INGEST_URL = process.env.BASE44_INGEST_URL;
const INGEST_SECRET = process.env.INGEST_SECRET;
const PORT = process.env.PORT || 3000;

if (!DEEPGRAM_API_KEY) throw new Error('Missing DEEPGRAM_API_KEY');
if (!BASE44_INGEST_URL) throw new Error('Missing BASE44_INGEST_URL');

const deepgramClient = createClient(DEEPGRAM_API_KEY);

// ── Health check ───────────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'twilio-deepgram-bridge' });
});

// ── Base44 ingest helper ───────────────────────────────────────────────────────
async function ingest(action, data) {
  const res = await fetch(BASE44_INGEST_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(INGEST_SECRET ? { 'x-ingest-secret': INGEST_SECRET } : {}),
    },
    body: JSON.stringify({ action, data }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`ingest(${action}) → ${res.status}: ${text}`);
  }
  return res.json();
}

async function findOrCreateCallSession(callSid, from, to) {
  const result = await ingest('createSession', { call_sid: callSid, from_number: from, to_number: to });
  console.log(`[Base44] CallSession id: ${result.id}`);
  return result.id;
}

async function saveTranscriptChunk({ sessionId, callSid, text, isFinal, confidence, speaker, timestampMs }) {
  await ingest('saveChunk', {
    call_session_id: sessionId,
    call_sid: callSid,
    text,
    is_final: isFinal,
    confidence: confidence ?? null,
    speaker: speaker ?? '',
    timestamp_ms: timestampMs,
  });
  console.log(`[Base44] Saved chunk (final=${isFinal}): "${text.substring(0, 60)}"`);
}

async function markSessionCompleted(sessionId) {
  await ingest('completeSession', { call_session_id: sessionId });
  console.log(`[Base44] Marked CallSession ${sessionId} completed`);
}

// ── WebSocket server at /media-stream ─────────────────────────────────────────
const wss = new WebSocketServer({ server, path: '/media-stream' });

wss.on('connection', (twilioWs) => {
  console.log('[Twilio] Client connected');

  let callSid = null;
  let streamSid = null;
  let from = null;
  let to = null;
  let sessionId = null;
  let dgLive = null;
  let dgConnected = false;
  let audioBuffer = [];

  // Open Deepgram live transcription session
  function connectDeepgram() {
    const live = deepgramClient.listen.live({
      encoding: 'mulaw',
      sample_rate: 8000,
      channels: 1,
      model: 'nova-2',
      language: 'en-US',
      punctuate: true,
      interim_results: true,
      diarize: true,
    });

    live.on('open', () => {
      dgConnected = true;
      console.log('[Deepgram] Connected');
      // Flush any buffered audio
      if (audioBuffer.length > 0) {
        console.log(`[Deepgram] Flushing ${audioBuffer.length} buffered audio chunks`);
        for (const chunk of audioBuffer) {
          try { live.send(chunk); } catch (_) {}
        }
        audioBuffer = [];
      }
    });

    live.on('transcript', async (data) => {
      const alt = data?.channel?.alternatives?.[0];
      const transcript = alt?.transcript;
      if (!transcript || transcript.trim() === '') return;

      const isFinal = data.is_final === true;
      const confidence = alt.confidence ?? null;
      const speaker = data?.channel?.alternatives?.[0]?.words?.[0]?.speaker;
      const speakerLabel = speaker !== undefined ? `Speaker ${speaker}` : '';

      console.log(`[Deepgram] Transcript (final=${isFinal}): "${transcript}"`);

      if (!sessionId) {
        console.log('[Deepgram] No sessionId yet, skipping save');
        return;
      }

      try {
        await saveTranscriptChunk({
          sessionId,
          callSid,
          text: transcript,
          isFinal,
          confidence,
          speaker: speakerLabel,
          timestampMs: Date.now(),
        });
      } catch (err) {
        console.error('[Base44] Error saving chunk:', err.message);
      }
    });

    live.on('error', (err) => {
      console.error('[Deepgram] Error:', err.message || err);
    });

    live.on('close', () => {
      dgConnected = false;
      console.log('[Deepgram] Disconnected');
    });

    return live;
  }

  dgLive = connectDeepgram();

  // Handle messages from Twilio
  twilioWs.on('message', async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (_) {
      return;
    }

    switch (msg.event) {
      case 'connected':
        console.log('[Twilio] Event: connected');
        break;

      case 'start': {
        callSid = msg.start?.callSid || null;
        streamSid = msg.start?.streamSid || null;
        from = msg.start?.customParameters?.from || '';
        to = msg.start?.customParameters?.to || '';
        console.log(`[Twilio] Event: start | callSid=${callSid} streamSid=${streamSid}`);

        try {
          sessionId = await findOrCreateCallSession(callSid, from, to);
        } catch (err) {
          console.error('[Base44] Error creating session:', err.message);
        }
        break;
      }

      case 'media': {
        const payload = msg.media?.payload;
        if (!payload) break;

        const audioBytes = Buffer.from(payload, 'base64');
        if (dgConnected && dgLive) {
          try {
            dgLive.send(audioBytes);
          } catch (_) {
            console.error('[Twilio] Error forwarding audio');
          }
        } else {
          // Buffer audio until Deepgram is ready
          audioBuffer.push(audioBytes);
        }
        break;
      }

      case 'stop':
        console.log('[Twilio] Event: stop');

        if (dgLive) {
          try { dgLive.finish(); } catch (_) {}
        }

        if (sessionId) {
          try {
            await markSessionCompleted(sessionId);
          } catch (err) {
            console.error('[Base44] Error completing session:', err.message);
          }
        }
        break;

      default:
        break;
    }
  });

  twilioWs.on('close', () => {
    console.log('[Twilio] Client disconnected');
    if (dgLive) {
      try { dgLive.finish(); } catch (_) {}
    }
  });

  twilioWs.on('error', (err) => {
    console.error('[Twilio] WebSocket error:', err.message);
  });
});

server.listen(PORT, () => {
  console.log(`[Server] Listening on port ${PORT}`);
});
