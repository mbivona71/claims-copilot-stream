/* eslint-disable no-undef */
/**
 * Twilio Media Streams → Deepgram → Base44
 * Deploy on Render as a Web Service (Node.js)
 *
 * Required environment variables:
 *   DEEPGRAM_API_KEY   - from console.deepgram.com
 *   BASE44_APP_ID      - from your Base44 app settings
 *   BASE44_API_KEY     - a Base44 service API key
 *   PORT               - set automatically by Render
 */

const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const { createClient } = require('@deepgram/sdk');

const app = express();
const server = http.createServer(app);

const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
const BASE44_APP_ID = process.env.BASE44_APP_ID;
const BASE44_API_KEY = process.env.BASE44_API_KEY;
const PORT = process.env.PORT || 3000;

if (!DEEPGRAM_API_KEY) throw new Error('Missing DEEPGRAM_API_KEY');
if (!BASE44_APP_ID) throw new Error('Missing BASE44_APP_ID');
if (!BASE44_API_KEY) throw new Error('Missing BASE44_API_KEY');

const deepgramClient = createClient(DEEPGRAM_API_KEY);

// ── Health check ───────────────────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'twilio-deepgram-bridge' });
});

// ── Base44 helper ──────────────────────────────────────────────────────────────
const BASE44_BASE = `https://api.base44.app/api/apps/${BASE44_APP_ID}/entities`;

async function base44Request(method, path, body) {
  const res = await fetch(`${BASE44_BASE}${path}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      'api-key': BASE44_API_KEY,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Base44 ${method} ${path} → ${res.status}: ${text}`);
  }
  return res.json();
}

async function findOrCreateCallSession(callSid, from, to) {
  const existing = await base44Request('GET', `/CallSession?call_sid=${encodeURIComponent(callSid)}&_limit=1`);
  if (existing && existing.length > 0) {
    console.log(`[Base44] Found existing CallSession: ${existing[0].id}`);
    return existing[0].id;
  }
  const session = await base44Request('POST', '/CallSession', {
    call_sid: callSid,
    from_number: from || '',
    to_number: to || '',
    status: 'active',
    started_at: new Date().toISOString(),
  });
  console.log(`[Base44] Created CallSession: ${session.id}`);
  return session.id;
}

async function saveTranscriptChunk({ sessionId, callSid, text, isFinal, confidence, speaker, timestampMs }) {
  await base44Request('POST', '/TranscriptChunk', {
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
  await base44Request('PUT', `/CallSession/${sessionId}`, {
    status: 'completed',
    ended_at: new Date().toISOString(),
  });
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

        if (dgConnected && dgLive) {
          try {
            const audioBytes = Buffer.from(payload, 'base64');
            dgLive.send(audioBytes);
          } catch (_) {
            console.error('[Twilio] Error forwarding audio');
          }
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