# Bitdo — bitchord-tido-addon

**Bring Tido / Tidal Hi-Res music, Dolby Atmos and music videos into BitChord, self-hosted and free.**

A tiny zero-dependency Node.js server that speaks the **BitChord addon protocol** (the same protocol as [rairulyle/bitchord-selfhosted-addon](https://github.com/rairulyle/bitchord-selfhosted-addon), mirrored exactly) and answers every search and stream from up to three layers of backends with automatic failover:

| Order | Backend | What it gives you | Setup |
| --- | --- | --- | --- |
| 1 | **Tido** (your deployment, e.g. `https://tido.tido.tido.rickyaddons.dpdns.org/<token>/manifest.json`) | Hi-Res FLAC up to 24-bit/192 kHz, Dolby Atmos (EAC3 JOC), full music videos | paste your Tido manifest URL on `/setup` |
| 2 | **hifi-api** ([binimum/hifi-api](https://github.com/binimum/hifi-api)) — any instances, ideally your own | Hi-Res FLAC, Atmos, music videos through the full Tidal playbackinfo flow (BTS + DASH manifests, 202 playback queue) | run your own (see [docs/hifi-api.md](docs/hifi-api.md)) and paste the URL |
| 3 | **Monochrome tracks** ([tracks.monochrome.st](https://tracks.monochrome.st) — the backend behind [monochrome.tf](https://monochrome.tf), see [monochrome-music](https://github.com/monochrome-music/monochrome)) | free lossless FLAC search + direct streaming, no account needed | enabled by default, nothing to do |

Because layer 3 is built in, **the addon returns working lossless results out of the box** even when Tido's shared hifi-api instances are down (which happens — Tidal periodically blocks shared accounts). Layers are tried top to bottom on every search and stream; a failing backend is cooled down for 45 s, and stream resolution can even fall back mid-track from one backend to another.

**Tido 403s and manifest regeneration.** When Tido fails, the addon can mint a fresh manifest by itself: set `TIDO_REGEN_URL` (or the generator URL field on `/setup`) to Tido's `generate` endpoint and a dead token gets replaced automatically — rate-limited to one regen per 10 minutes, plus a *Regenerate manifest now* button. Measured reality check (2026-11): while Tido's whole upstream is down, *every* token 403s `{"error":"Unavailable"}` identically — a fresh manifest changes nothing then, and the failover ladder carries your requests instead. Regeneration is the fix for the other case: your specific token being rate-limited or banned while the upstream is alive. The `/setup → Backend status` page tells the two apart — it probes both the manifest and the data path, so "manifest ok but data 403" reads as *upstream down*, not *token dead*.

## How it plays in BitChord

BitChord (Android/PC) prefers sources you add over its built-in ones. Add the link from `/setup` under **Sources** and drag it to the top:

```
https://your-addon.example.com/tidal-1/<secret>/
```

That link is your password — it contains the addon secret. Rotate it any time on `/setup`.

## What the addon implements

Front-end protocol, byte-for-byte compatible with the official self-hosted addon:

- `GET /{slug}/{secret}/manifest.json` — addon descriptor (`resources: ["search","stream"]`, `types: ["track"]`, `contentType: "music"`)
- `GET /{slug}/{secret}/search?q=…` — `{tracks: [...], albums: [], artists: [], playlists: []}` with `id / title / artist / album / duration / artworkURL / format / audioQuality` (empty arrays always present, like the Go addon)
- `GET /{slug}/{secret}/stream/{id}` — descriptor with `url / format / quality / codec / container / manifest / sampleRate / bitDepth / bitrate` (bitrate in **bits per second**, like BitChord expects)
- `GET /{slug}/{secret}/file/{id}` — byte proxy with `Range` / `If-Range` forwarded and `Content-Range` answers, so seeking works; CDN links that expired are re-resolved transparently
- `GET /{slug}/{secret}/art/{id}` — artwork proxy with an in-memory LRU cache
- `POST /{slug}/{secret}/sync` — cache-reset webhook
- `OPTIONS` preflight + CORS `*` with `Range, If-Range, Content-Type, X-BitChord-Token, X-BitChord-Proof` allowed
- Wrong secret or slug → a bare `404` that leaks nothing (same as upstream)
- Music videos appear in search as `[Video] …` entries (toggle) and stream as muxed MP4 at your chosen 1080p/720p/480p

Backend machinery:

- **Tido**: Eclipse-protocol calls (`/search?q=`, `/stream/{id}`) with your quality settings appended per request; the official video attached to a stream response becomes the `[Video]` entry
- **hifi-api**: the full ladder — `ATMOS (immersiveaudio)` → `HI_RES_LOSSLESS` → `LOSSLESS` → `HIGH` → `LOW` (exact-quality mode stops after the first tier); `/trackManifests/` JSON:API + MPD parsing that extracts single-file `BaseURL` streams (adaptive-only MPDs are handed to BitChord as `manifest: "dash"`); v1 BTS manifests base64-decoded to direct URLs; the **202 playback queue** is polled through `/playback/requests/{id}` until it completes; `/video/` for music videos; DRM-protected manifests are skipped honestly
- **Monochrome**: `/search/tracks?q=` for results, `/track/{id}` for direct FLAC — one ranged GET doubles as probe + **FLAC STREAMINFO parser**, so the stream descriptor reports the *real* sample rate and bit depth (e.g. `lossless 16-bit 44.1kHz`) and a bitrate computed from the actual file size; 429 rate-limit responses are retried with the server's `Retry-After` hint
- Search cache (60 s), stream cache (10 min), metadata cache (1 h), artwork cache (24 h), backend cooldowns, secret/token redaction in every log line

## Quick start — Render (free)

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?url=https://raw.githubusercontent.com/YOUR-USERNAME/bitchord-tido-addon/main/render.yaml)
*(public repo: swap in your username after pushing — one click after login)*

The supported cookie-free flow (no dashboard scraping, no session juggling):

1. **Push this folder to GitHub** — it is already a git repo with an initial commit, so after creating an empty repo on github.com it is just:
   ```bash
   cd bitchord-tido-addon
   git remote add origin git@github.com:YOU/bitchord-tido-addon.git
   git push -u origin main
   ```
   No git? Create the repo on github.com and drag the folder's files into the *"uploading an existing file"* page — same result.
2. **On [Render](https://render.com)**: *New → Blueprint*, pick the repo. `render.yaml` fills everything: Node runtime, **free plan**, `node server.js`, health check `/health`, and the keep-alive self-ping (every 4 minutes, via `RENDER_EXTERNAL_URL`) so the free instance never idles out — the same "Self-Ping / Infinite Uptime" approach as [PerzaanGaming/Perzaan-AFK-Bot](https://github.com/PerzaanGaming/Perzaan-AFK-Bot). Apply.
3. Open `https://<your-app>.onrender.com/setup`, set a password, paste your Tido manifest URL, save, and copy the BitChord link.

**Prefer the API?** `scripts/render-api-deploy.sh` creates the service through Render's official REST API — run it on your own machine so the key stays local:
```bash
RENDER_API_KEY=rnd_…  REPO_URL=https://github.com/you/bitchord-tido-addon \
  TIDO_URL='https://tido…/<token>/manifest.json' \
  bash scripts/render-api-deploy.sh
```

No build step and no npm dependencies — the first deploy takes seconds. Render free has no persistent disk, so the config is re-seeded from env vars on each deploy (`TIDO_URL`, `AUDIO_QUALITY`, …) or just re-entered on `/setup` (takes 10 seconds). Full guide: [docs/deploy-render.md](docs/deploy-render.md).

## Quick start — Wispbyte (free)

Wispbyte gives you free Node.js hosting (512 MB RAM) with no daily renewal clicks:

1. Create a Node.js application on [wispbyte.com](https://wispbyte.com/).
2. Upload this folder as a zip (or via git), set the startup file to `server.js`.
3. Wispbyte injects `PORT` — the addon picks it up automatically.
4. Open the public URL `/setup` and configure as above.

Details: [docs/deploy-wispbyte.md](docs/deploy-wispbyte.md). On Wispbyte the app never sleeps, so keep-alive is off by default.

## Quick start — Docker / VPS

```yaml
services:
  bitchord-tido-addon:
    build: .
    ports: ["8080:8080"]
    volumes: ["./data:/data"]      # config persists across restarts
    restart: unless-stopped
```

Put your reverse proxy (Caddy/Traefik/nginx) or a Cloudflare Tunnel in front for HTTPS, exactly like the upstream addon. The container runs as user 65532 with `/data` for state, mirroring the official image's conventions.

## Quality settings

| Setting | Values | Effect |
| --- | --- | --- |
| Audio quality | `MAX` / `HIRES` / `LOSSLESS` / `HIGH` / `LOW` | Same tiers as Tido: Max+Atmos, Max, Lossless FLAC 16/44.1, AAC 320, AAC 96. Passed through to Tido per request; drives the hifi-api ladder; monochrome always serves FLAC. |
| Exact quality only | on/off | Off: fall back down the tiers when a track has no hi-res copy. On: skip instead. |
| Music videos in search | on/off | Adds `[Video] …` entries. |
| Video quality | 1080p / 720p / 480p | Resolution requested for videos. |
| Hide explicit | on/off | Filters explicit tracks at search time on every backend. |

## Testing

```bash
bash test/run-tests.sh     # 87 end-to-end checks against the bundled mock upstream
# live failover + streaming against real backends (your token stays in your shell):
TIDO_URL='https://tido…/<token>/manifest.json' bash test/run-live-tests.sh
```

The mock upstream simulates every backend flavor (BTS manifests, the 202 playback queue, single-file and adaptive MPDs, Atmos, music videos, monochrome FLAC with a real STREAMINFO header, a dead Tido token plus the `/generate` endpoint), so the whole pipeline — including manifest auto-regeneration — is verified without touching any real service.

## Security notes

- The `/setup` page is password-protected (scrypt-hashed, login throttling, 12 h signed sessions); the first visit sets the password.
- Secrets and Tido tokens never appear in logs (tested).
- Stream links are guarded by the URL secret; rotate it on `/setup` any time.
- Your Tido token stays between your addon and your Tido deployment; your hifi-api credentials stay on your own hifi-api instance.

## Legal

For use with your own Tidal/Tido accounts and instances, for personal streaming. The authors of the referenced projects (Tido, hifi-api, Monochrome, BitChord) are not affiliated with this bridge. Respect their terms and your local laws.

## Project layout

```
server.js              the whole addon (zero npm dependencies, Node >= 18)
setup.html             configuration web UI served at /setup
render.yaml            one-click Render deployment
Dockerfile             self-host/VPS image (user 65532, /data state)
compose.example.yml    docker compose example
.env.example           every environment variable, documented
scripts/               deploy helpers (Render API deploy)
test/mock-upstream.js  simulated hifi-api + Tido (incl. /generate) + monochrome upstream
test/run-tests.sh      87-check end-to-end suite
test/run-live-tests.sh live failover + streaming proof (token via TIDO_URL env)
docs/                  deployment and backend guides
```
