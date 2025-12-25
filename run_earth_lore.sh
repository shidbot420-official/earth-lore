#!/usr/bin/env bash
set -Euo pipefail

# Load and auto-export everything from .env
set -a
source earth_lore.env
set +a

SCRIPT="earth_lore_stream.py"        # change if the filename differs
LOG="/root/stream.log"
FIFO="/tmp/earth_audio.pcm"

stop=0
trap 'stop=1; log "[Stop] signal received"; cleanup' INT TERM  # Ctrl+C or kill

# line-buffer Python output so tee doesn't batch logs
PY="stdbuf -oL -eL /root/earth_lore_video/venv/bin/python -u"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG" ; }

# ensure log exists
touch "$LOG"

# clean up fifo on exit (prevents pipe-open hangs on next start)
cleanup() {
  log "Cleanup triggered"
  if [[ -p "$FIFO" ]]; then rm -f "$FIFO"; fi
}
trap cleanup INT TERM EXIT

BACKOFF=3       # initial backoff seconds
MAX_BACKOFF=300 # cap at 5 minutes

while true; do
  log "[Start] launching $SCRIPT"
  # make sure stale fifo is gone before the script tries to recreate it
  [[ -p "$FIFO" ]] && rm -f "$FIFO"

  # run the stream
  $PY "$SCRIPT" 2>&1 | tee -a "$LOG"
  RC=${PIPESTATUS[0]}  # python exit code (not tee's)

  if [[ $RC -eq 130 || $RC -eq 143 || $stop -eq 1 ]]; then
    log "[Exit] user interrupt/term detected (RC=$RC)."
    break
  fi

  log "[Crash Detected] Exit code: $RC"
  log "[Restarting in ${BACKOFF}s]"
  sleep "$BACKOFF"

  # exponential backoff (prevents hot-restart loops if something is badly wrong)
  if (( BACKOFF < MAX_BACKOFF )); then
    BACKOFF=$(( BACKOFF * 2 ))
    (( BACKOFF > MAX_BACKOFF )) && BACKOFF=$MAX_BACKOFF
  fi
done
