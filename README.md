# Earth Lore Stream

A Python-based live streaming application that generates and streams chronological video content from a CSV dataset, featuring dynamic slide generation, era-based music, and Discord integration.

## Overview

This project creates a live video stream that displays historical events chronologically, with each slide showing:
- Year and label
- Era information
- Background images
- Special event facts (for marked entries)
- Dynamic corner overlays (promos/sponsors)
- Era-appropriate background music

The stream can be output to YouTube RTMP or saved as a local video file.

## Features

- **Dynamic Slide Generation**: Creates video slides from CSV data with custom fonts and styling
- **Era-Based Music**: Automatically matches and plays music tracks based on the current era
- **Crossfade Transitions**: Smooth transitions between slides
- **Discord Integration**: Optional posting of slides to Discord webhook
- **Resume Capability**: Automatically resumes from the last processed slide
- **Corner Overlays**: Rotating promo and sponsor overlays at configurable intervals
- **Special Event Handling**: Extended duration and fact display for special events

## Requirements

### System Requirements
- **OS**: Linux or macOS (uses named pipes for audio)
- **Python**: 3.9 or higher
- **FFmpeg**: Must be installed and available in PATH

### Python Dependencies
```bash
pip install pillow pandas requests
```

## Project Structure

```
.
├── earth_lore_stream.py    # Main streaming script
├── run_earth_lore.sh       # Bash wrapper with auto-restart
├── earth_lore.env          # Environment configuration (credentials removed)
└── assets/
    ├── fonts/              # Font files (Fredoka family)
    ├── era_music/          # Era-specific music tracks (.mp3)
    ├── images/             # Background images
    │   └── special_years/ # Special year images
    ├── overlay/            # Corner overlay images
    │   ├── promos/         # Promotional overlay images
    │   └── sponsors/       # Sponsor overlay images
    ├── full_years.csv      # Main dataset (Year, Label, Era, Image, Fact, isSpecial)
    ├── era_durations.txt   # JSON file with era-specific slide durations
    └── background_loop.mp3 # Default background music
```

## Setup

### 1. Install Dependencies

```bash
# Install Python packages
pip install pillow pandas requests

# Ensure FFmpeg is installed
ffmpeg -version
```

### 2. Configure Environment

Copy `earth_lore.env` and add your credentials:

```bash
# Required: YouTube RTMP URL for streaming
YOUTUBE_RTMP_URL="rtmp://a.rtmp.youtube.com/live2/YOUR_STREAM_KEY"

# Optional: Discord webhook for posting slides
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/YOUR_WEBHOOK_URL"

# Optional: Discord posting mode
# Options: "all" (post every slide), "special" (only special events), "none" (disabled)
DISCORD_POST_MODE="all"
```

### 3. Prepare Assets

Ensure the following asset folders exist (they can be empty initially):

- `assets/fonts/` - Place Fredoka font files here:
  - `Fredoka-SemiBold.ttf`
  - `Fredoka-Regular.ttf`
  - `Fredoka-Medium.ttf`

- `assets/era_music/` - Place era-specific music files here (e.g., `Early Hominins.mp3`, `The Bronze Age.mp3`)

- `assets/images/` - Place background images here
  - `assets/images/special_years/` - Place special year images here

- `assets/overlay/promos/` - Place promotional overlay images here (PNG/JPG/WebP)

- `assets/overlay/sponsors/` - Place sponsor overlay images here (PNG/JPG/WebP)

### 4. Configure Paths

Edit `earth_lore_stream.py` and update the path constants if your installation differs from `/root/earth_lore_video/`:

```python
CSV_PATH = "/path/to/your/assets/full_years.csv"
FONT_SEMIBOLD = "/path/to/your/assets/fonts/Fredoka-SemiBold.ttf"
# ... etc
```

## Usage

### Running the Stream

#### Option 1: Using the Bash Wrapper (Recommended)

The `run_earth_lore.sh` script provides automatic restart on crashes with exponential backoff:

```bash
chmod +x run_earth_lore.sh
./run_earth_lore.sh
```

The script will:
- Load environment variables from `earth_lore.env`
- Run the Python script with proper logging
- Automatically restart on crashes
- Handle cleanup of named pipes

#### Option 2: Direct Python Execution

```bash
# Load environment variables
export $(cat earth_lore.env | xargs)

# Run the script
python3 earth_lore_stream.py
```

### CSV Format

The `full_years.csv` file should contain the following columns:

- **Year**: The year to display
- **Label**: The label/description for that year
- **Era**: The era name (used for music matching)
- **Image**: Path to background image (relative to assets/images/ or absolute)
- **Fact**: Text to display in the bottom overlay (only shown for special events)
- **isSpecial**: Boolean flag (TRUE/true/1/yes/y/t) to mark special events

Example:
```csv
Year,Label,Era,Image,Fact,isSpecial
-4500000,First Hominins,Early Hominins,early_hominins.png,First evidence of hominin species,TRUE
-3000000,Stone Tools,Lower Paleolithic,lower_paleolithic.png,,FALSE
```

### Era Music Matching

The script automatically matches music files to eras using flexible name matching:
- Tries exact era name
- Strips date ranges (e.g., "Early Hominins (4.5M - 3M)" → "Early Hominins")
- Handles "The" prefix variations
- Handles "Era" suffix variations

Place music files in `assets/era_music/` with names like:
- `Early Hominins.mp3`
- `The Bronze Age.mp3`
- `Industrial Age.mp3`

If no match is found, `background_loop.mp3` is used as fallback.

### Era Durations

Edit `assets/era_durations.txt` (JSON format) to set custom slide durations per era:

```json
{
  "Early Hominins": 1.0,
  "Lower Paleolithic": 1.5,
  "The Bronze Age": 3.5,
  "Industrial Age": 6.0
}
```

Special events always use a minimum duration of 5 seconds (configurable via `SPECIAL_MIN_DURATION`).

### Corner Overlays

Corner overlays appear every 50 slides (configurable) and display for 4 slides:
- Alternates between promos and sponsors
- Images are automatically rotated -12° and scaled to 300x300
- Positioned at coordinates (1480, 180)

## Configuration

### Video Settings

Edit constants in `earth_lore_stream.py`:

```python
WIDTH, HEIGHT = 1920, 1080  # Output resolution
FPS = 30                     # Frame rate
SLIDE_DURATION = 4.0         # Default slide duration (seconds)
CROSSFADE_DURATION = 0.5     # Transition duration (seconds)
INTRO_OUTRO_DURATION = 6.0   # Intro/outro duration (seconds)
```

### Streaming Settings

```python
# Video encoding
"-b:v", "6000k",           # Video bitrate
"-preset", "veryfast",     # Encoding speed

# Audio encoding
"-b:a", "192k",            # Audio bitrate
```

## Resume Functionality

The script automatically saves progress to `last_index.txt`. If the script is interrupted, it will resume from the last processed slide on the next run.

To restart from the beginning, delete `last_index.txt`.

## Logging

When using `run_earth_lore.sh`, logs are written to `/root/stream.log` (configurable in the script).

The script outputs detailed information about:
- Slide processing progress
- Music file matching
- Discord posting status
- FFmpeg errors and restarts

## Troubleshooting

### FFmpeg Errors

If FFmpeg crashes, the script will automatically restart it. Check logs for specific error messages.

### Missing Music Files

The script will warn about missing era music files and use the default background loop. Check console output for suggestions.

### Named Pipe Issues

On Linux/macOS, the script creates `/tmp/earth_audio.pcm` as a named pipe. If you see errors:
- Ensure the directory exists and is writable
- Check that no stale pipe files exist (the script handles this automatically)

### Font Loading

If fonts fail to load, the script falls back to PIL's default font. Ensure font files are in the correct location and readable.

## Output Options

### YouTube Live Streaming

Set `YOUTUBE_RTMP_URL` in `earth_lore.env` to your YouTube RTMP stream key.

### Local File Output

Set `YOUTUBE_RTMP_URL` to `"output.mp4"` for local file output (useful for testing).

## Discord Integration

To enable Discord posting:
1. Create a Discord webhook in your server settings
2. Add `DISCORD_WEBHOOK_URL` to `earth_lore.env`
3. Set `DISCORD_POST_MODE`:
   - `"all"`: Post every slide
   - `"special"`: Post only slides marked as special
   - `"none"`: Disable Discord posting

## License

[Add your license information here]

## Support

[Add support/contact information here]

