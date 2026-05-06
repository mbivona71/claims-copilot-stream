# Deploy to Render

## Step 1 — GitHub repo structure
Push these 3 files to the ROOT of a new GitHub repo:
  server.js
  package.json
  DEPLOY.md

## Step 2 — Create a Render Web Service
1. render.com → New → Web Service
2. Connect your GitHub repo
3. Settings:
   - Runtime: Node
   - Build Command: npm install
   - Start Command: node server.js

## Step 3 — Environment Variables in Render
| Key              | Value                        |
|------------------|------------------------------|
| DEEPGRAM_API_KEY | Your Deepgram API key        |
| BASE44_APP_ID    | Your Base44 App ID           |
| BASE44_API_KEY   | Your Base44 service API key  |

## Step 4 — Your WebSocket URL
wss://YOUR-APP-NAME.onrender.com/media-stream

## Step 5 — Add secret to Base44
Add secret:  BRIDGE_WS_URL = wss://YOUR-APP-NAME.onrender.com/media-stream
The twilioInboundCallHandler will use it automatically.

## Step 6 — Test
Call your Twilio number → watch Call Co-Pilot for live transcripts.

## Notes
- Free Render tier sleeps after 15min idle (first call may lag ~30s). Upgrade to Starter ($7/mo) for always-on.
- Uses Deepgram nova-2 with diarization (speaker labels).