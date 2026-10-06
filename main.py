"""
Downlvibes API  -  yt-dlp powered video resolver / downloader.
Supports: YouTube, TikTok, Instagram, Facebook, X (Twitter).

Endpoints
  GET /                      health check
  GET /api/v1/resolve        ?url=...&key=...   -> title, thumbnail, formats
  GET /api/v1/download       ?url=...&q=720|best|mp3&type=video|audio&key=...
"""
import asyncio
import hmac
import os
import re
import shutil
import tempfile
import time
from collections import defaultdict, deque
from urllib.parse import quote, urlparse

import yt_dlp
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from starlette.background import BackgroundTask
from starlette.exceptions import HTTPException as StarletteHTTPException

# ---------------------------------------------------------------- config ---
API_KEYS = {k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()}
ALLOWED_ORIGINS = [o.strip() for o in os.getenv(
    "ALLOWED_ORIGINS", "https://downlvibes.blogspot.com").split(",") if o.strip()]
COOKIES_FILE = os.getenv("COOKIES_FILE", "")          # optional cookies.txt (Netscape format)
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")  # e.g. https://my-api.up.railway.app
MAX_DURATION = int(os.getenv("MAX_DURATION", "3600"))  # seconds
MAX_FILESIZE = os.getenv("MAX_FILESIZE", "500M")      # yt-dlp size syntax
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "2"))
RATE_LIMIT = int(os.getenv("RATE_LIMIT", "20"))       # requests / minute / IP

SITES = {
    "youtube.com": "YouTube", "youtu.be": "YouTube",
    "tiktok.com": "TikTok",
    "instagram.com": "Instagram",
    "facebook.com": "Facebook", "fb.watch": "Facebook",
    "x.com": "X", "twitter.com": "X",
}

app = FastAPI(title="Downlvibes API", docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET"],
    allow_headers=["*"],
)
_slots = asyncio.Semaphore(MAX_CONCURRENT)
_hits = defaultdict(deque)


# ---------------------------------------------------------------- errors ---
@app.exception_handler(StarletteHTTPException)
async def _http_err(_, exc):
    return JSONResponse({"error": str(exc.detail)}, status_code=exc.status_code)


@app.exception_handler(RequestValidationError)
async def _val_err(_, exc):
    return JSONResponse({"error": "Invalid request."}, status_code=400)


# --------------------------------------------------------------- helpers ---
def _client_ip(req: Request) -> str:
    fwd = req.headers.get("x-forwarded-for", "")
    return fwd.split(",")[0].strip() if fwd else (req.client.host if req.client else "?")


def _guard(req: Request, key: str):
    # 1) API key
    if API_KEYS:
        supplied = key or req.headers.get("x-api-key", "")
        if not any(hmac.compare_digest(supplied, k) for k in API_KEYS):
            raise HTTPException(401, "Invalid API key.")
    # 2) rate limit
    now = time.time()
    q = _hits[_client_ip(req)]
    while q and now - q[0] > 60:
        q.popleft()
    if len(q) >= RATE_LIMIT:
        raise HTTPException(429, "Too many requests. Please wait a minute.")
    q.append(now)


def _site_of(url: str):
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        return None
    host = p.hostname.lower()
    for dom, name in SITES.items():
        if host == dom or host.endswith("." + dom):
            return name
    return None


def _clean(msg: str) -> str:
    msg = re.sub(r"\x1b\[[0-9;]*m", "", str(msg))
    msg = re.sub(r"^ERROR:\s*(\[[^\]]+\]\s*[\w\-]*:?\s*)?", "", msg).strip()
    low = msg.lower()
    if "login" in low or "cookie" in low or "private" in low:
        return "This video is private or needs a login, so it can't be downloaded."
    if "unsupported url" in low:
        return "This link is not supported."
    return (msg[:180] + "...") if len(msg) > 180 else (msg or "Could not fetch this video.")


def _base_opts():
    o = {"quiet": True, "no_warnings": True, "noplaylist": True, "socket_timeout": 20}
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        o["cookiefile"] = COOKIES_FILE
    return o


def _extract(url: str):
    with yt_dlp.YoutubeDL({**_base_opts(), "skip_download": True}) as ydl:
        return ydl.extract_info(url, download=False)


def _safe_name(title: str, ext: str) -> str:
    t = re.sub(r"[^\w\- ]+", "", title or "video", flags=re.ASCII).strip() or "video"
    return f"{t[:60]}.{ext}"


def _base(req: Request) -> str:
    return PUBLIC_URL or str(req.base_url).rstrip("/")


# ------------------------------------------------------------- endpoints ---
@app.get("/")
async def health():
    return {"status": "ok", "service": "Downlvibes API"}


@app.get("/api/v1/resolve")
async def resolve(request: Request, url: str = "", key: str = ""):
    _guard(request, key)
    url = url.strip()
    site = _site_of(url)
    if not site:
        raise HTTPException(400, "Supported sites: YouTube, TikTok, Instagram, Facebook and X.")
    try:
        info = await asyncio.to_thread(_extract, url)
    except yt_dlp.utils.DownloadError as e:
        raise HTTPException(422, _clean(e))
    except Exception:
        raise HTTPException(500, "Could not fetch this video.")

    if info.get("_type") == "playlist" and info.get("entries"):
        info = next((e for e in info["entries"] if e), info)

    duration = info.get("duration") or 0
    if duration and duration > MAX_DURATION:
        raise HTTPException(413, "This video is too long to download.")

    heights = sorted({f["height"] for f in info.get("formats", [])
                      if f.get("vcodec") not in (None, "none") and f.get("height")
                      and f["height"] >= 144}, reverse=True)

    enc = quote(url, safe="")
    k = quote(key, safe="")
    base = _base(request)

    def link(q, t):
        return f"{base}/api/v1/download?url={enc}&q={q}&type={t}&key={k}"

    video = [{"label": f"{h}p", "quality": f"{h}p", "ext": "mp4", "url": link(h, "video")}
             for h in heights[:6]]
    if not video:
        video = [{"label": "Best quality", "quality": "best", "ext": "mp4",
                  "url": link("best", "video")}]
    audio = [{"label": "MP3 audio", "quality": "mp3", "ext": "mp3", "url": link("mp3", "audio")}]

    return {
        "title": info.get("title") or "Video",
        "site": site,
        "duration": duration,
        "thumbnail": info.get("thumbnail") or "",
        "formats": {"video": video, "audio": audio},
    }


@app.get("/api/v1/download")
async def download(request: Request, url: str = "", q: str = "best",
                   type: str = "video", key: str = ""):
    _guard(request, key)
    url = url.strip()
    if not _site_of(url):
        raise HTTPException(400, "This link is not supported.")

    opts = {
        **_base_opts(),
        "max_filesize": yt_dlp.utils.parse_filesize(MAX_FILESIZE),
        "match_filter": yt_dlp.utils.match_filter_func(f"duration <= {MAX_DURATION}"),
        "restrictfilenames": True,
    }
    if type == "audio" or q == "mp3":
        opts["format"] = "ba/b"
        opts["postprocessors"] = [{"key": "FFmpegExtractAudio",
                                   "preferredcodec": "mp3", "preferredquality": "192"}]
        ext = "mp3"
    else:
        h = int(q) if q.isdigit() else 0
        opts["format"] = (f"bv*[height<={h}]+ba/b[height<={h}]/b" if h else "bv*+ba/b")
        opts["merge_output_format"] = "mp4"
        ext = "mp4"

    tmp = tempfile.mkdtemp(prefix="dlv_")
    opts["outtmpl"] = os.path.join(tmp, "file.%(ext)s")

    def run():
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=True)

    try:
        async with _slots:
            info = await asyncio.to_thread(run)
    except yt_dlp.utils.DownloadError as e:
        shutil.rmtree(tmp, ignore_errors=True)
        raise HTTPException(422, _clean(e))
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise HTTPException(500, "Download failed.")

    files = [f for f in os.listdir(tmp) if not f.endswith((".part", ".ytdl"))]
    if not files:
        shutil.rmtree(tmp, ignore_errors=True)
        raise HTTPException(413, "This video is too large or too long to download.")

    path = os.path.join(tmp, files[0])
    real_ext = os.path.splitext(path)[1].lstrip(".") or ext
    return FileResponse(
        path,
        filename=_safe_name((info or {}).get("title", "video"), real_ext),
        background=BackgroundTask(shutil.rmtree, tmp, True),
    )
