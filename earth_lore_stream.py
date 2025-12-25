# earth_lore_stream.py
# Requirements: Python 3.9+, ffmpeg, pip install pillow pandas
# OS: Linux/macOS (uses a named pipe at /tmp/earth_audio.pcm)

import os
import sys
import time
import math
import threading
import subprocess
import json
import requests
from queue import Queue, Empty, Full
from io import BytesIO
from typing import Optional

import pandas as pd
from PIL import Image, ImageDraw, ImageFont

import re
from functools import lru_cache

def _norm(s: str) -> str:
    """Normalize an era/file name for matching: lowercase, strip, remove non-alnum."""
    s = (s or "").strip().casefold()
    s = re.sub(r"[^\w]+", "", s)  # remove spaces, hyphens, punctuation
    return s

@lru_cache(maxsize=1)
def _music_index():
    """Return dict of normalized_name -> absolute_path for all mp3s in MUSIC_DIR."""
    idx = {}
    if os.path.isdir(MUSIC_DIR):
        for fn in os.listdir(MUSIC_DIR):
            if fn.lower().endswith(".mp3"):
                idx[_norm(os.path.splitext(fn)[0])] = os.path.join(MUSIC_DIR, fn)
    print(
    "[music] tracks indexed:",
    sorted(os.path.basename(p) for p in idx.values()),
    "from", MUSIC_DIR,
    flush=True,
    )
    return idx

_missing_eras_reported = set()

# =========================
# CONFIG
# =========================
WIDTH, HEIGHT = 1920, 1080
FPS = 30
SLIDE_DURATION = 4.0          # seconds per slide
CROSSFADE_DURATION = 0.5      # seconds
INTRO_OUTRO_DURATION = 6.0    # seconds

# ========== DISCORD WEBHOOK ==========
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
# "all" = post every slide, "special" = post only rows where isSpecial is true, "none" = disabled
DISCORD_POST_MODE = os.getenv("DISCORD_POST_MODE", "all").strip().lower()
DISCORD_ENABLE = bool(DISCORD_WEBHOOK_URL) and DISCORD_POST_MODE != "none"

# Paths / Assets
CSV_PATH = "/root/earth_lore_video/assets/full_years.csv"  # columns: Year, Label, Era, Image, Fact (order preserved)
FONT_SEMIBOLD = "/root/earth_lore_video/assets/fonts/Fredoka-SemiBold.ttf"
FONT_REGULAR  = "/root/earth_lore_video/assets/fonts/Fredoka-Regular.ttf"
FONT_MEDIUM   = "/root/earth_lore_video/assets/fonts/Fredoka-Medium.ttf"

ERA_DURATIONS_PATH = "/root/earth_lore_video/assets/era_durations.txt"
SPECIAL_MIN_DURATION = 5.0

MUSIC_DIR = "/root/earth_lore_video/assets/era_music"         # contains "<Era>.mp3"
DEFAULT_MUSIC = "/root/earth_lore_video/assets/background_loop.mp3"

# Output: set your YouTube RTMP URL here (or "output.mp4" for local test)
YOUTUBE_RTMP_URL = os.getenv("YOUTUBE_RTMP_URL", "").strip() or "output.mp4"

# Visual spec
BG_FALLBACK = "#0D3F84"   # deep blue background
WHITE_100 = (255, 255, 255, 255)
WHITE_80  = (255, 255, 255, int(255*0.8))
BLACK_100 = (0, 0, 0, 255)

# Top text (Year then Label)
YEAR_FONT_SIZE  = 72    # Semibold
LABEL_FONT_SIZE = 56    # Semibold
TOP_STROKE      = 4    # px black stroke

# Era text (under the Year)
ERA_FONT_SIZE   = 40    # Medium
ERA_STROKE      = 4     # px
ERA_GAP         = 16    # px from the year line above
ERA_PADDING     = 8     # extra padding to avoid stroke/descender clipping
ERA_FILL_RGBA   = (255, 255, 255, int(255*0.60))  # white @ 90%
ERA_STROKE_RGBA = BLACK_100

# Bottom overlay
OVERLAY_ENABLE = True
OVERLAY_HEADING_TEXT = "Canon Event of the Year"
OVERLAY_WIDTH   = 1200
OVERLAY_PADDING = 32
OVERLAY_GAP     = 12
OVERLAY_BG_ALPHA = int(255 * 0.60)  # 60%
OVERLAY_RADIUS   = 24

HEADING_SIZE   = 32   # "Event of the Year"
HEADING_COLOR  = WHITE_80
HEADING_STROKE = 4

FACT_MAX_SIZE   = 48  # auto-fit down to min
FACT_MIN_SIZE   = 22
FACT_COLOR      = WHITE_100
FACT_STROKE     = 4

# ===== Corner badge overlay (baked into slides) =====
CORNER_OVERLAY_ENABLE = True
CORNER_OVERLAY_PROMOS_DIR   = "/root/earth_lore_video/assets/overlay/promos"
CORNER_OVERLAY_SPONSORS_DIR = "/root/earth_lore_video/assets/overlay/sponsors"

# slide-based scheduling
CORNER_OVERLAY_EVERY_N_SLIDES = 50
CORNER_OVERLAY_SHOW_M_SLIDES  = 4     # …and keep it for 4 slides

# visuals
CORNER_OVERLAY_W   = 300
CORNER_OVERLAY_H   = 300
CORNER_OVERLAY_X   = 1480
CORNER_OVERLAY_Y   = 180
CORNER_OVERLAY_ROT = -12.0  # degrees

# Resume file
LAST_INDEX_FILE = "last_index.txt"

# Named pipe for audio (Linux/macOS)
AUDIO_FIFO_PATH = "/tmp/earth_audio.pcm"  # s16le, 48kHz, stereo

# =========================
# GLOBALS
# =========================
ffmpeg_proc: Optional[subprocess.Popen] = None
fonts_loaded = False
FONT_YEAR = None
FONT_LABEL = None
FONT_HEADING = None
FONT_FACT_MAX = None
FONT_ERA = None

_current_era = None
_audio_thread = None
_audio_stop = threading.Event()
_audio_switch_lock = threading.Lock()
_audio_decoder_proc: Optional[subprocess.Popen] = None

_discord_q = Queue(maxsize=50)
_discord_thread = None
_discord_stop = threading.Event()

_overlay_promos = []
_overlay_sponsors = []
_overlay_cache = {}  # path -> prepared 300x300 RGBA

# =========================
# UTIL
# =========================
def load_font(path, size):
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        # graceful fallback to default PIL font (no crash)
        return ImageFont.load_default()

def ensure_fonts():
    global fonts_loaded, FONT_YEAR, FONT_LABEL, FONT_HEADING, FONT_FACT_MAX, FONT_ERA
    if fonts_loaded:
        return
    FONT_YEAR    = load_font(FONT_SEMIBOLD, YEAR_FONT_SIZE)
    FONT_LABEL   = load_font(FONT_SEMIBOLD, LABEL_FONT_SIZE)
    FONT_HEADING = load_font(FONT_REGULAR,  HEADING_SIZE)
    FONT_FACT_MAX= load_font(FONT_REGULAR,  FACT_MAX_SIZE)
    # Era uses Fredoka Medium; if missing, load() will gracefully fall back.
    FONT_ERA     = load_font(FONT_MEDIUM,   ERA_FONT_SIZE)
    fonts_loaded = True

def textbbox(draw, text, font, stroke_w=0):
    return draw.textbbox((0,0), text, font=font, stroke_width=stroke_w)

def draw_stroked(draw, xy, text, font, fill, stroke_w, stroke_fill):
    draw.text(xy, text, font=font, fill=fill, stroke_width=stroke_w, stroke_fill=stroke_fill)

def fit_wrap(draw, text, font, max_w):
    words = (text or "").split()
    if not words:
        return [""]
    lines, cur = [], words[0]
    for w in words[1:]:
        test = f"{cur} {w}"
        if draw.textlength(test, font=font) <= max_w:
            cur = test
        else:
            lines.append(cur)
            cur = w
    lines.append(cur)
    return lines

def auto_fit_font(draw, text, max_w, max_size, min_size, font_path):
    size = max_size
    while size >= min_size:
        f = load_font(font_path, size)
        lines = fit_wrap(draw, text, f, max_w)
        if lines:
            longest = max(draw.textlength(ln, font=f) for ln in lines)
        else:
            longest = 0
        if longest <= max_w or size == min_size:
            return f, lines
        size -= 2
    f = load_font(font_path, min_size)
    return f, fit_wrap(draw, text, f, max_w)

def sanitize(s):
    if s is None:
        return ""
    return str(s)

def is_blank(s):
    return sanitize(s).strip() == ""

# =========================
# CORNER OVERLAY
# =========================
def _list_overlay_images(folder):
    if not folder or not os.path.isdir(folder):
        return []
    exts = (".png", ".jpg", ".jpeg", ".webp")
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.lower().endswith(exts)]
    files.sort()
    return files

def corner_overlay_prepare(path):
    """Load, rotate to -12°, fit into 300x300, center on transparent canvas. Returns RGBA."""
    if not path:
        return None
    if path in _overlay_cache:
        return _overlay_cache[path]
    try:
        img = Image.open(path).convert("RGBA")
        rot = img.rotate(CORNER_OVERLAY_ROT, expand=True, resample=Image.BICUBIC)
        rw, rh = rot.size
        sc = min(CORNER_OVERLAY_W / rw, CORNER_OVERLAY_H / rh)
        new_sz = (max(1, int(rw*sc)), max(1, int(rh*sc)))
        scaled = rot.resize(new_sz, Image.LANCZOS)
        canvas = Image.new("RGBA", (CORNER_OVERLAY_W, CORNER_OVERLAY_H), (0,0,0,0))
        offx = (CORNER_OVERLAY_W - new_sz[0]) // 2
        offy = (CORNER_OVERLAY_H - new_sz[1]) // 2
        canvas.paste(scaled, (offx, offy), scaled)
        _overlay_cache[path] = canvas
        return canvas
    except Exception as e:
        print(f"[corner-overlay] failed to load '{path}': {e}", flush=True)
        _overlay_cache[path] = None
        return None

def corner_overlay_init():
    """Read promos/sponsors once."""
    global _overlay_promos, _overlay_sponsors
    _overlay_promos = _list_overlay_images(CORNER_OVERLAY_PROMOS_DIR)
    _overlay_sponsors = _list_overlay_images(CORNER_OVERLAY_SPONSORS_DIR)
    print(f"[corner-overlay] promos={len(_overlay_promos)} | sponsors={len(_overlay_sponsors)}", flush=True)

def corner_overlay_pick(cycle_idx):
    """
    Pattern per overlay 'block':
      0: promo-1
      1: sponsor-1
      2: promo-2
      3: sponsor-2 (or sponsor-1 if not enough)
      4: promo-3
      ...
    Sponsors empty -> always fall back to promos.
    """
    if not _overlay_promos and not _overlay_sponsors:
        return None
    n = cycle_idx // 2  # which index within each group
    if (cycle_idx % 2) == 0:
        # promo-n (loop)
        if _overlay_promos:
            return _overlay_promos[n % len(_overlay_promos)]
        # no promos? fall back to sponsors looping
        return _overlay_sponsors[n % len(_overlay_sponsors)] if _overlay_sponsors else None
    else:
        # sponsor-n with "pin to sponsor-1" when we run out
        if _overlay_sponsors:
            return _overlay_sponsors[n] if n < len(_overlay_sponsors) else _overlay_sponsors[0]
        # sponsors empty -> fall back to promos looping
        return _overlay_promos[n % len(_overlay_promos)] if _overlay_promos else None

def corner_overlay_paste(base_img_rgb, overlay_rgba):
    """Paste prepared 300x300 RGBA onto the RGB slide at the fixed position."""
    if overlay_rgba is None:
        return base_img_rgb
    frame = base_img_rgb.convert("RGBA")
    frame.paste(overlay_rgba, (CORNER_OVERLAY_X, CORNER_OVERLAY_Y), overlay_rgba)
    return frame.convert("RGB")

def _discord_worker():
    """Background poster so the stream never blocks on HTTP."""
    if not DISCORD_ENABLE:
        return
    session = requests.Session()
    while not _discord_stop.is_set():
        try:
            img_bytes, year, label = _discord_q.get(timeout=0.2)
        except Empty:
            continue

        files = {"file": ("slide.jpg", img_bytes, "image/jpeg")}
        data  = {"content": f"📜 **{label or 'Slide'}** — Year: {year}"}

        try:
            r = session.post(DISCORD_WEBHOOK_URL, data=data, files=files, timeout=10)
            if r.status_code >= 400:
                print(f"[discord] post failed: {r.status_code} {r.text}", flush=True)
            else:
                print(f"[discord] posted: {year} | {label}", flush=True)
        except Exception as e:
            print(f"[discord] error: {e}", flush=True)
        finally:
            _discord_q.task_done()

def start_discord_thread():
    if not DISCORD_ENABLE:
        print("[discord] disabled (no DISCORD_WEBHOOK_URL or mode=none)", flush=True)
        return
    global _discord_thread
    _discord_thread = threading.Thread(target=_discord_worker, daemon=True)
    _discord_thread.start()
    print("[discord] worker started", flush=True)

def stop_discord_thread():
    _discord_stop.set()

def enqueue_discord_slide(img: Image.Image, year: str, label: str):
    """JPEG-compress and enqueue without blocking the stream loop."""
    if not DISCORD_ENABLE:
        return
    try:
        buf = BytesIO()
        # Keep it small but clean; under Discord 8MB free-file limit by far
        img.save(buf, format="JPEG", quality=85)
        _discord_q.put_nowait((buf.getvalue(), str(year), str(label)))
    except Full:
        print("[discord] queue full; dropping slide post", flush=True)
    except Exception as e:
        print(f"[discord] enqueue failed: {e}", flush=True)


# =========================
# IMAGE COMPOSITION
# =========================
def compose_slide(year_val, label_val, era_val, image_path, fact_text):
    ensure_fonts()
    # background canvas
    canvas = Image.new("RGB", (WIDTH, HEIGHT), BG_FALLBACK)
    draw = ImageDraw.Draw(canvas)

    # try to load local image (absolute or relative). If fail, keep blue bg.
    img_loaded = False
    if not is_blank(image_path):
        candidate_paths = [image_path]
        if not os.path.isabs(image_path):
            candidate_paths.append(os.path.join("assets", image_path))
        for p in candidate_paths:
            if os.path.exists(p):
                try:
                    img = Image.open(p).convert("RGB")
                    # cover-fit to HEIGHT, center horizontally
                    scale = HEIGHT / img.height
                    new_w = int(img.width * scale)
                    img = img.resize((new_w, HEIGHT))
                    x_off = (WIDTH - new_w) // 2
                    canvas.paste(img, (x_off, 0))
                    img_loaded = True
                    break
                except Exception as e:
                    print(f"[image] Failed to open '{p}': {e}", flush=True)
    if not img_loaded and not is_blank(image_path):
        print(f"[image] Not found or unreadable: '{image_path}'. Using fallback background.", flush=True)

    # --- Top stack: literal "Year", then {Label} from CSV, then Era
    top_text    = "Year"
    bottom_text = sanitize(label_val)
    era_text    = sanitize(era_val)

    y_top_margin = 40
    top_bbox = textbbox(draw, top_text,  FONT_YEAR,  stroke_w=TOP_STROKE)
    bot_bbox = textbbox(draw, bottom_text, FONT_LABEL, stroke_w=TOP_STROKE) if bottom_text else (0,0,0,0)

    top_x = (WIDTH - (top_bbox[2]-top_bbox[0])) // 2
    top_y = y_top_margin
    draw_stroked(draw, (top_x, top_y), top_text, FONT_YEAR, WHITE_100, TOP_STROKE, BLACK_100)

    if bottom_text:
        bot_x = (WIDTH - (bot_bbox[2]-bot_bbox[0])) // 2
        bot_y = top_y + (top_bbox[3]-top_bbox[1]) + 8
        draw_stroked(draw, (bot_x, bot_y), bottom_text, FONT_LABEL, WHITE_100, TOP_STROKE, BLACK_100)

    # Draw Era (centered) 16px below the year line, with 80% opacity
    if era_text:
        era_bbox = textbbox(draw, era_text, FONT_ERA, stroke_w=ERA_STROKE)
        era_w = era_bbox[2]-era_bbox[0]
        era_h = era_bbox[3]-era_bbox[1]
        # padded overlay prevents clipping from stroke/descenders
        ov_w = era_w + 2*ERA_PADDING
        ov_h = era_h + 2*ERA_PADDING
        era_layer = Image.new("RGBA", (ov_w, ov_h), (0,0,0,0))
        era_draw  = ImageDraw.Draw(era_layer)
        era_draw.text((ERA_PADDING, ERA_PADDING), era_text, font=FONT_ERA, fill=ERA_FILL_RGBA, stroke_width=ERA_STROKE, stroke_fill=ERA_STROKE_RGBA)
        era_x = (WIDTH - ov_w) // 2
        # place after "Year" + (optional) label; maintain 16px gap beneath the year number line
        after_year_y = top_y + (top_bbox[3]-top_bbox[1]) + (8 if bottom_text else 0) + (bot_bbox[3]-bot_bbox[1])
        era_y = after_year_y + ERA_GAP
        canvas.paste(era_layer, (era_x, era_y), era_layer)

    # Bottom overlay if Fact exists
    if OVERLAY_ENABLE and not is_blank(fact_text):
        overlay_w = OVERLAY_WIDTH
        overlay_x = (WIDTH - overlay_w) // 2
        content_max_w = overlay_w - 2*OVERLAY_PADDING

        # Heading
        heading_bbox = textbbox(draw, OVERLAY_HEADING_TEXT, FONT_HEADING, stroke_w=HEADING_STROKE)
        heading_h = heading_bbox[3]-heading_bbox[1]

        # Fact text auto-fit
        fact_font, lines = auto_fit_font(draw, sanitize(fact_text), content_max_w,
                                         FACT_MAX_SIZE, FACT_MIN_SIZE, FONT_REGULAR)
        # PIL's ImageFont.load_default() has no path; use provided FONT_REGULAR constant instead
        #if fact_font is None or isinstance(fact_font, ImageFont.ImageFont) and fact_font == ImageFont.load_default():
            # ensure we still have a usable font
            #fact_font = load_font(FONT_REGULAR, FACT_MIN_SIZE)

        line_heights = []
        for ln in lines:
            bb = textbbox(draw, ln, fact_font, stroke_w=FACT_STROKE)
            line_heights.append(bb[3]-bb[1])
        content_h = sum(line_heights) + (len(lines)-1)*6

        overlay_h = OVERLAY_PADDING + heading_h + OVERLAY_GAP + content_h + OVERLAY_PADDING

        # Draw overlay (rounded rectangle) on separate RGBA layer
        ov = Image.new("RGBA", (overlay_w, overlay_h), (0,0,0,0))
        od = ImageDraw.Draw(ov)
        od.rounded_rectangle([(0,0),(overlay_w, overlay_h)],
                             radius=OVERLAY_RADIUS, fill=(0,0,0,OVERLAY_BG_ALPHA))
        ov_y = HEIGHT - overlay_h - 40  # bottom margin
        canvas.paste(ov, (overlay_x, ov_y), ov)

        # Draw heading + fact lines onto the main canvas
        heading_x = overlay_x + OVERLAY_PADDING
        heading_y = ov_y + OVERLAY_PADDING
        draw_stroked(draw, (heading_x, heading_y), OVERLAY_HEADING_TEXT, FONT_HEADING,
                     HEADING_COLOR, HEADING_STROKE, (0,0,0,255))

        cur_y = heading_y + heading_h + OVERLAY_GAP
        for ln in lines:
            draw_stroked(draw, (heading_x, cur_y), ln, fact_font, FACT_COLOR, FACT_STROKE, (0,0,0,255))
            bb = textbbox(draw, ln, fact_font, stroke_w=FACT_STROKE)
            cur_y += (bb[3]-bb[1]) + 6

    return canvas

def compose_center_two_line(top_text, bottom_text, top_size=72, bottom_size=56, stroke=10):
    ensure_fonts()
    # Make dedicated fonts for this slide to ensure sizes
    top_font    = load_font(FONT_SEMIBOLD, top_size)
    bottom_font = load_font(FONT_SEMIBOLD, bottom_size)

    canvas = Image.new("RGB", (WIDTH, HEIGHT), BG_FALLBACK)
    draw = ImageDraw.Draw(canvas)

    top_bbox = textbbox(draw, top_text, top_font, stroke_w=stroke)
    bot_bbox = textbbox(draw, bottom_text, bottom_font, stroke_w=stroke) if bottom_text else (0,0,0,0)

    total_h = (top_bbox[3]-top_bbox[1]) + (8 if bottom_text else 0) + (bot_bbox[3]-bot_bbox[1])
    start_y = (HEIGHT - total_h) // 2

    top_x = (WIDTH - (top_bbox[2]-top_bbox[0])) // 2
    top_y = start_y
    draw_stroked(draw, (top_x, top_y), top_text, top_font, WHITE_100, stroke, BLACK_100)

    if bottom_text:
        bot_x = (WIDTH - (bot_bbox[2]-bot_bbox[0])) // 2
        bot_y = top_y + (top_bbox[3]-top_bbox[1]) + 8
        draw_stroked(draw, (bot_x, bot_y), bottom_text, bottom_font, WHITE_100, stroke, BLACK_100)

    return canvas

# =========================
# AUDIO HANDLING (NO FFMPEG RESTART)
# =========================
def ensure_fifo(path):
    try:
        if os.path.exists(path):
            if not stat_is_fifo(path=True, path_str=path):
                os.remove(path)
        if not os.path.exists(path):
            os.mkfifo(path)
    except Exception as e:
        print(f"[audio] Failed to create FIFO at {path}: {e}", flush=True)
        sys.exit(1)

def stat_is_fifo(path=False, path_str=None):
    # helper: check if existing path is FIFO
    try:
        st = os.stat(path_str)
        return (st.st_mode & 0o170000) == 0o010000  # stat.S_IFIFO
    except Exception:
        return False

def pick_music_for_era(era_name: Optional[str]) -> str:
    """
    Robust era→music resolver.
    Tries:
      - full raw era
      - base era without trailing " (…)" range
      - with/without leading "The "
      - without a trailing " Era"
    Uses same normalization as _music_index().
    """
    idx = _music_index()
    if not era_name or not era_name.strip():
        return DEFAULT_MUSIC

    raw = era_name.strip()
    base = raw.split(" (", 1)[0].strip()  # "Early Hominins (…)" -> "Early Hominins"

    def _add(v, bag):
        k = _norm(os.path.splitext(v)[0])
        if k and k not in bag:
            bag.append(k)

    # Build candidate normalized keys (in priority order)
    cands = []
    _add(raw, cands)               # exact era as-is
    _add(base, cands)              # strip ranges
    if base.startswith("The "):
        _add(base[4:], cands)      # drop "The "
    else:
        _add(f"The {base}", cands) # add "The "
    if base.endswith(" Era"):
        _add(base[:-4].strip(), cands)  # drop " Era"
    else:
        _add(f"{base} Era", cands)      # add " Era"

    # DEBUG: see what we’re trying and what’s available
    # print(f"[music] era='{era_name}' candidates -> {cands}", flush=True)

    # Try to match any candidate against indexed filenames
    for k in cands:
        if k in idx:
            # print(f"[music] match -> {idx[k]}", flush=True)
            return idx[k]

    # Not found: warn once per distinct raw era
    if raw not in _missing_eras_reported:
        _missing_eras_reported.add(raw)
        suggestions = ", ".join(sorted(os.path.basename(p) for p in idx.values())) or "(none found)"
        print(
            f"[music] Era track not found for '{raw}'. Using default.\n"
            f"        Tried (normalized): {cands}\n"
            f"        Tip: name a file like '{base}.mp3' or 'The {base}.mp3'. Available:\n"
            f"        {suggestions}",
            flush=True,
        )
    return DEFAULT_MUSIC

def stop_decoder_proc():
    global _audio_decoder_proc
    if _audio_decoder_proc is not None:
        try:
            _audio_decoder_proc.terminate()
            _audio_decoder_proc.wait(timeout=2)
        except Exception:
            pass
        _audio_decoder_proc = None

def audio_feeder_loop():
    """
    Keeps ffmpeg's audio input fed from a named pipe, decoding current era MP3
    into s16le/stereo/48k PCM. Switches tracks on demand without restarting the main ffmpeg.
    """
    global _current_era, _audio_decoder_proc

    # Open FIFO for writing (blocking until the reader—the main ffmpeg—opens it)
    with open(AUDIO_FIFO_PATH, "wb", buffering=0) as fifo_out:
        current_file = None

        while not _audio_stop.is_set():
            # Pick the correct file for current era
            with _audio_switch_lock:
                era = _current_era
            music_file = pick_music_for_era(era)

            # If different from current_file, switch decoder
            if music_file != current_file:
                print(f"[music] switching to: {music_file}", flush=True)
                stop_decoder_proc()
                # Start decoder ffmpeg -> stdout (raw PCM)
                _audio_decoder_proc = subprocess.Popen([
                    "ffmpeg",
                    "-v", "quiet",
                    "-stream_loop", "-1", "-i", music_file,
                    "-f", "s16le",
                    "-ar", "48000",
                    "-ac", "2",
                    "pipe:1"
                ], stdout=subprocess.PIPE)
                current_file = music_file

            # Pump bytes from decoder stdout into FIFO
            if _audio_decoder_proc and _audio_decoder_proc.stdout:
                chunk = _audio_decoder_proc.stdout.read(8192)
                if not chunk:
                    # decoder ended unexpectedly; restart decoder for the same file
                    stop_decoder_proc()
                    current_file = None
                    continue
                try:
                    fifo_out.write(chunk)
                except BrokenPipeError:
                    # main ffmpeg may have died; wait and retry
                    time.sleep(0.2)
            else:
                time.sleep(0.05)

def set_current_era(era_name: Optional[str]):
    global _current_era
    with _audio_switch_lock:
        _current_era = (era_name or "").strip()

def start_audio_thread():
    ensure_fifo(AUDIO_FIFO_PATH)
    global _audio_thread
    _audio_thread = threading.Thread(target=audio_feeder_loop, daemon=True)
    _audio_thread.start()

def stop_audio_thread():
    _audio_stop.set()
    stop_decoder_proc()
    # don't join forever; it's daemon

# =========================
# FFMPEG (SINGLE PROCESS)
# =========================
def start_ffmpeg():
    """
    One ffmpeg process that ingests:
      - video frames via stdin (image2pipe)
      - audio via named pipe (/tmp/earth_audio.pcm), raw PCM s16le 48k stereo
    """
    global ffmpeg_proc
    cmd = [
        "ffmpeg",
        "-loglevel", "warning",
        # Video input
        "-re",
        "-f", "image2pipe",
        "-framerate", str(FPS),
        "-i", "-",
        # Audio input (raw PCM from FIFO)
        "-f", "s16le",
        "-ar", "48000",
        "-ac", "2",
        "-i", AUDIO_FIFO_PATH,
        # Encoding
        "-c:v", "libx264",
        "-b:v", "6000k",
        "-g", "120",
        "-preset", "veryfast",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "192k",
        "-f", "flv",
        YOUTUBE_RTMP_URL
    ]
    print("[ffmpeg] starting main process...", flush=True)
    ffmpeg_proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)

def stop_ffmpeg():
    global ffmpeg_proc
    if ffmpeg_proc:
        try:
            if ffmpeg_proc.stdin:
                ffmpeg_proc.stdin.close()
            ffmpeg_proc.wait(timeout=3)
        except Exception:
            pass
        ffmpeg_proc = None

def restart_ffmpeg():
    print("[ffmpeg] restarting...", flush=True)
    try:
        stop_ffmpeg()
    except Exception:
        pass
    start_ffmpeg()


# =========================
# STREAM I/O
# =========================
def send_frame(img: Image.Image, duration: float):
    """Send a still frame repeatedly for 'duration' seconds."""
    if not ffmpeg_proc or not ffmpeg_proc.stdin:
        return
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=90)
    frame = buf.getvalue()
    total = int(round(FPS * duration))
    sent = 0
    while sent < total:
        try:
            ffmpeg_proc.stdin.write(frame)
            sent += 1
        except (BrokenPipeError, OSError) as e:
            print(f"[ffmpeg] write error: {e}, restarting and retrying remaining frames...", flush=True)
            restart_ffmpeg()
            # small grace so ffmpeg is ready to read
            time.sleep(0.05)
            # continue loop to send the remaining frames
            continue

def send_crossfade(img_a: Image.Image, img_b: Image.Image, duration: float):
    if not ffmpeg_proc or not ffmpeg_proc.stdin:
        return
    steps = max(1, int(FPS * duration))
    i = 0
    while i < steps:
        alpha = i / float(steps)
        blended = Image.blend(img_a, img_b, alpha)
        buf = BytesIO()
        blended.save(buf, format="JPEG", quality=90)
        try:
            ffmpeg_proc.stdin.write(buf.getvalue())
            i += 1
        except (BrokenPipeError, OSError) as e:
            print(f"[ffmpeg] crossfade error: {e}, restarting and resuming...", flush=True)
            restart_ffmpeg()
            time.sleep(0.05)
            continue

# =========================
# MAIN
# =========================
def main():
    # Load CSV (order preserved)
    try:
        df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8-sig")
    except Exception as e:
        print(f"[csv] Failed to read '{CSV_PATH}': {e}", flush=True)
        sys.exit(1)

    df = df.fillna("")  # ensure strings

    # Prepare overlays (promo/sponsor)
    if CORNER_OVERLAY_ENABLE:
        corner_overlay_init()

    # Load per-era durations (JSON: { "Early Hominins": 2.0, ... })
    try:
        with open(ERA_DURATIONS_PATH, "r", encoding="utf-8") as f:
            era_durations_map = json.load(f)
    except Exception as e:
        print(f"[durations] Could not load {ERA_DURATIONS_PATH}: {e}", flush=True)
        era_durations_map = {}

    # Resume point
    start_idx = 0
    if os.path.exists(LAST_INDEX_FILE):
        try:
            with open(LAST_INDEX_FILE) as f:
                start_idx = int(f.read().strip())
        except Exception:
            start_idx = 0

    # Prepare audio, ffmpeg & discord worker
    start_audio_thread()
    start_ffmpeg()
    start_discord_thread()

    # Intro: "Earth Lore" / "- 4.5M Years Ranked Chronologically" (6s)
    intro = compose_center_two_line("Earth Lore", "- 4.5M Years Ranked Chronologically",
                                    top_size=72, bottom_size=56, stroke=4)
    send_frame(intro, INTRO_OUTRO_DURATION)

    prev_img = None
    total = len(df)
    overlay_cycle_idx = 0              # which promo/sponsor to use next
    overlay_block_remaining = 0        # slides left to keep showing current overlay
    overlay_current_rgba = None
    for i in range(start_idx, total):
        row = df.iloc[i]
        year  = sanitize(row.get("Year", ""))
        label = sanitize(row.get("Label", ""))
        era   = sanitize(row.get("Era", ""))
        img_p = sanitize(row.get("Image", ""))
        fact  = sanitize(row.get("Fact", ""))

        print(f"[slide {i+1}/{total}] Year: {year} | Label: {label} | Era: {era} | Image: {img_p}", flush=True)

        set_current_era(era)

                # isSpecial flag (TRUE/true/1/yes/y/t -> True)
        is_special = str(row.get("isSpecial", "")).strip().lower() in {"true", "1", "yes", "y", "t"}

        # Only show the bottom overlay on special rows by passing the fact; otherwise pass empty string
        slide = compose_slide(year, label, era, img_p, fact if is_special else "")

        # ---- Corner overlay by slide count (every N slides for M slides) ----
        if CORNER_OVERLAY_ENABLE:
            slide_no = i + 1  # 1-based

            # Start a new overlay block at 101, 201, 301, ... (not at slide 1)
            if slide_no % CORNER_OVERLAY_EVERY_N_SLIDES == 1 and slide_no != 1:
                path = corner_overlay_pick(overlay_cycle_idx)
                overlay_cycle_idx += 1
                overlay_current_rgba = corner_overlay_prepare(path) if path else None
                overlay_block_remaining = CORNER_OVERLAY_SHOW_M_SLIDES
                print(f"[corner-overlay] block start @ slide {slide_no} -> {os.path.basename(path) if path else 'None'}", flush=True)

            if overlay_block_remaining > 0 and overlay_current_rgba is not None:
                slide = corner_overlay_paste(slide, overlay_current_rgba)
                overlay_block_remaining -= 1

        # Post to Discord depending on mode
        if DISCORD_POST_MODE == "all" or (DISCORD_POST_MODE == "special" and is_special):
            enqueue_discord_slide(slide, year, label)


        # Crossfade from prev
        if prev_img is not None:
            send_crossfade(prev_img, slide, CROSSFADE_DURATION)

        # Duration selection:
        # - JSON keys are short era names (no ranges), so take the part before any " (".
        # - Also tolerate "The Bronze Age" vs "Bronze Age" by trying both.
        base_era = era.split(" (", 1)[0].strip()  # "Early Hominins (…)" -> "Early Hominins"
        era_secs = (
            era_durations_map.get(base_era)
            or era_durations_map.get(f"The {base_era}")
            or SLIDE_DURATION
        )
        era_secs = float(era_secs)

        dur = max(era_secs, SPECIAL_MIN_DURATION) if is_special else era_secs
        print(f"[duration] era='{base_era}' -> {era_secs:.3f}s | special={is_special} -> using {dur:.3f}s", flush=True)

        send_frame(slide, dur)
        prev_img = slide

        # persist progress
        try:
            with open(LAST_INDEX_FILE, "w") as f:
                f.write(str(i))
        except Exception as e:
            print(f"[state] Could not write last index: {e}", flush=True)

    # Outro: "Thanks for Watching" (6s)
    outro = compose_center_two_line("Thanks for Watching", "", top_size=72, bottom_size=0, stroke=4)
    if prev_img is not None:
        send_crossfade(prev_img, outro, CROSSFADE_DURATION)
    send_frame(outro, INTRO_OUTRO_DURATION)

    # cleanup
    stop_ffmpeg()
    stop_audio_thread()
    stop_discord_thread()

if __name__ == "__main__":
    main()
