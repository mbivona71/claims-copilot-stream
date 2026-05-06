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
    let audioChunksSent = 0;

    console.log('[Deepgram] Initializing live transcription...');
    const live = deepgramClient.listen.live({
      encoding: 'mulaw',
      sample_rate: 8000,
      channels: 1,
      model: 'nova-2',
      language: 'en-US',
      punctuate: true,
      interim_results: true,
      diarize: true,
      smart_format: true,
      endpointing: 300,
      vad_events: true,
    });

    live.on('open', () => {
      dgConnected = true;
      console.log('[Deepgram] Connected and ready to receive audio');
      // Flush any buffered audio
      if (audioBuffer.length > 0) {
        console.log(`[Deepgram] Flushing ${audioBuffer.length} buffered audio chunks`);
        for (const chunk of audioBuffer) {
          try {
            live.send(chunk);
            audioChunksSent++;
            if (audioChunksSent % 50 === 0) {
              console.log(`[Deepgram] Sent ${audioChunksSent} chunks to Deepgram`);
            }
          } catch (e) {
            console.error('[Deepgram] Error flushing buffer:', e.message);
          }
        }
        audioBuffer = [];
      }
    });

    // Transcript event (try both naming conventions for SDK compatibility)
    const handleTranscript = async (data) => {
      console.log('[Deepgram] ✓ TRANSCRIPT event received (raw):', JSON.stringify(data, null, 2));

      const alt = data?.channel?.alternatives?.[0];
      const transcript = alt?.transcript || '';

      if (!transcript.trim()) {
        console.log('[Deepgram] Empty transcript, skipping save');
        return;
      }

      const isFinal = data.is_final === true;
      const confidence = alt.confidence ?? null;
      const speaker = data?.channel?.alternatives?.[0]?.words?.[0]?.speaker;
      const speakerLabel = speaker !== undefined ? `Speaker ${speaker}` : 'Unknown Speaker';

      console.log(`[Deepgram] Transcript (final=${isFinal}, confidence=${confidence}, speaker=${speakerLabel}): "${transcript}"`);

      if (!sessionId) {
        console.log('[Deepgram] ⚠ No sessionId yet, cannot save');
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
        console.error('[Base44] ✗ Error saving chunk:', err.message);
      }
    };

    // Register transcript listener (support both naming styles)
    live.on('transcript', handleTranscript);
    live.on('Transcript', handleTranscript);

    // Speech started event
    const handleSpeechStarted = () => {
      console.log('[Deepgram] 🎤 Speech started');
    };
    live.on('speech_started', handleSpeechStarted);
    live.on('SpeechStarted', handleSpeechStarted);

    // Utterance end event
    const handleUtteranceEnd = (data) => {
      console.log('[Deepgram] 📝 Utterance end:', JSON.stringify(data, null, 2));
    };
    live.on('utterance_end', handleUtteranceEnd);
    live.on('UtteranceEnd', handleUtteranceEnd);

    // Metadata event
    const handleMetadata = (data) => {
      console.log('[Deepgram] 📊 METADATA:', JSON.stringify(data, null, 2));
    };
    live.on('metadata', handleMetadata);
    live.on('Metadata', handleMetadata);

    // Error event
    const handleError = (err) => {
      console.error('[Deepgram] ✗ ERROR:', err.message || JSON.stringify(err));
    };
    live.on('error', handleError);
    live.on('Error', handleError);

    // Close event
    live.on('close', () => {
      dgConnected = false;
      console.log(`[Deepgram] ✗ Disconnected (sent ${audioChunksSent} total chunks)`);
    });
    live.on('Close', () => {
      dgConnected = false;
      console.log(`[Deepgram] ✗ Disconnected (Close event, sent ${audioChunksSent} chunks)`);
    });

    return live;
  }

  dgLive = connectDeepgram();

  // Handle messages from Twilio
  let mediaChunksReceived = 0;
  let audioChunksSentThisSession = 0;

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
        mediaChunksReceived = 0;
        audioChunksSentThisSession = 0;
        break;

      case 'start': {
        callSid = msg.start?.callSid || null;
        streamSid = msg.start?.streamSid || null;
        from = msg.start?.customParameters?.from || '';
        to = msg.start?.customParameters?.to || '';
        console.log(`[Twilio] Event: start | callSid=${callSid} streamSid=${streamSid} from=${from} to=${to}`);

        try {
          sessionId = await findOrCreateCallSession(callSid, from, to);
          console.log(`[Twilio] ✓ Created Base44 CallSession: ${sessionId}`);
        } catch (err) {
          console.error('[Base44] ✗ Error creating session:', err.message);
        }
        break;
      }

      case 'media': {
        mediaChunksReceived++;
        const payload = msg.media?.payload;
        if (!payload) {
          console.warn('[Twilio] Media event missing payload');
          break;
        }

        if (mediaChunksReceived % 50 === 0) {
          console.log(`[Twilio] ✓ Received ${mediaChunksReceived} media chunks (payload len: ${payload.length}, dgConnected: ${dgConnected})`);
        }

        const audioBytes = Buffer.from(payload, 'base64');
        if (dgConnected && dgLive) {
          try {
            dgLive.send(audioBytes);
            audioChunksSentThisSession++;
            if (audioChunksSentThisSession % 50 === 0) {
              console.log(`[Twilio→Deepgram] ✓ Sent ${audioChunksSentThisSession} audio chunks to Deepgram (${audioBytes.length} bytes each)`);
            }
          } catch (e) {
            console.error('[Twilio→Deepgram] ✗ Error forwarding audio:', e.message);
          }
        } else {
          // Buffer audio until Deepgram is ready
          audioBuffer.push(audioBytes);
          if (audioBuffer.length % 20 === 0) {
            console.log(`[Twilio] ⏳ Buffering (${audioBuffer.length} chunks) - Deepgram connected: ${dgConnected}`);
          }
        }
        break;
      }

      case 'stop':
        console.log(`[Twilio] Event: stop (received ${mediaChunksReceived} total media chunks, sent ${audioChunksSentThisSession} to Deepgram)`);

        if (dgLive) {
          try {
            console.log('[Deepgram] Finishing stream...');
            dgLive.finish();
          } catch (e) {
            console.error('[Deepgram] Error finishing stream:', e.message);
          }
        }

        if (sessionId) {
          try {
            await markSessionCompleted(sessionId);
            console.log(`[Base44] ✓ Marked CallSession ${sessionId} as completed`);
          } catch (err) {
            console.error('[Base44] ✗ Error completing session:', err.message);
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
