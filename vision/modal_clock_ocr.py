#!/usr/bin/env python3
"""Modal-based parallel OCR pipeline for extracting game clock and quarter from
basketball footage stored in Cloudflare R2.

Architecture:
  1. Download video from R2 + extract 1 FPS frames via ffmpeg (CPU Modal function)
  2. Fan-out OCR over frame batches using GPU T4 instances (parallel Modal function)
  3. Aggregate, smooth, and export clock_timeline.json + game_state.json
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import modal
from dotenv import dotenv_values

# ---------------------------------------------------------------------------
# Load .env.local locally and build a Modal Secret from it explicitly.
# modal.Secret.from_dotenv() has proven unreliable with non-standard filenames;
# dotenv_values() gives us full control.
# ---------------------------------------------------------------------------
_DOTENV_PATH = Path(__file__).resolve().parent / ".env.local"
_env_vars: dict[str, str] = {
    k: v for k, v in dotenv_values(_DOTENV_PATH).items() if v is not None
}
r2_secret = modal.Secret.from_dict(_env_vars)

# ---------------------------------------------------------------------------
# Modal image
# ---------------------------------------------------------------------------

image = (
    modal.Image.debian_slim()
    .apt_install(["ffmpeg", "libsm6", "libxext6", "libgl1"])
    .pip_install(
        [
            "easyocr",
            "opencv-python-headless",
            "boto3",
            "pandas",
            "python-dotenv",
        ]
    )
)

app = modal.App("basketball-clock-ocr", image=image)

# Shared volume so the extraction function and OCR functions can both see frames
volume = modal.Volume.from_name("clock-ocr-frames", create_if_missing=True)
VOLUME_MOUNT = "/mnt/frames"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

R2_BUCKET = "gamefootage"
VIDEO_KEY = "uploads/1776649531479-067fdd79-3133-44d5-8897-e4861ea94bc2__lakers_warriors_christmas_first_minute.mp4"
LOCAL_VIDEO = "/tmp/video.mp4"
FRAMES_DIR = "/tmp/frames"
BATCH_SIZE = 30  # frames per GPU call

# ---------------------------------------------------------------------------
# Helper: build R2 boto3 client inside a container
# ---------------------------------------------------------------------------


def _r2_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


# ---------------------------------------------------------------------------
# Step 1 — Download video & extract frames
# ---------------------------------------------------------------------------


@app.function(
    secrets=[r2_secret],
    volumes={VOLUME_MOUNT: volume},
    timeout=1800,
    cpu=4,
)
def extract_frames() -> list[str]:
    """Download video from R2 and extract 1 FPS frames to the shared volume.

    Returns a sorted list of absolute frame paths (inside the container/volume).
    """
    import glob

    frames_dir = Path(VOLUME_MOUNT) / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)

    # --- download ---
    print(f"Downloading {VIDEO_KEY} from R2 …")
    s3 = _r2_client()
    s3.download_file(R2_BUCKET, VIDEO_KEY, LOCAL_VIDEO)
    print("Download complete.")

    # --- extract frames ---
    print("Extracting 1 FPS frames with ffmpeg …")
    cmd = [
        "ffmpeg",
        "-y",
        "-i", LOCAL_VIDEO,
        "-vf", "fps=1",
        "-q:v", "2",
        str(frames_dir / "frame_%06d.jpg"),
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    volume.commit()

    frame_paths = sorted(glob.glob(str(frames_dir / "frame_*.jpg")))
    print(f"Extracted {len(frame_paths)} frames.")
    return frame_paths


# ---------------------------------------------------------------------------
# Step 2 — Parallel GPU OCR
# ---------------------------------------------------------------------------


def _parse_clock(tokens: list[str]) -> str | None:
    """Return MM:SS from individual EasyOCR tokens, tolerating common colon misreads.

    Operates token-by-token (not on the joined string) to prevent cross-token
    digit collapsing from corrupting matches.

    Observed misreads of ':' in the ESPN scorebug font:
      '.'  → '11.46'  ';' → '11;53'  '*' → '11*47'
      '8'  → '11855' (colon pixel shape read as 8)
      ' '  → '1 1850' (EasyOCR splits the token mid-number)
    """
    for raw in tokens:
        # Collapse any internal spaces so '1 1850' → '11850'
        t = re.sub(r"(\d)\s+(\d)", r"\1\2", raw).strip()

        # Pass 1: standard + punctuation separators, anchored to the full token
        m = re.fullmatch(r"(\d{1,2})[:.;*]([0-5][0-9])", t)
        if m:
            return f"{m.group(1)}:{m.group(2)}"

        # Pass 2: colon misread as '8' → 5-char fused token e.g. '11846'
        m = re.fullmatch(r"(\d{1,2})8([0-5][0-9])", t)
        if m and int(m.group(1)) <= 12:
            return f"{m.group(1)}:{m.group(2)}"

    return None


def _parse_quarter(text: str) -> str | None:
    """Return a quarter string like '1ST', '2ND', '3RD', '4TH', 'OT' if found."""
    m = re.search(r"\b(1ST|2ND|3RD|4TH|OT\d*)\b", text.upper())
    return m.group(1) if m else None


@app.function(
    gpu="T4",
    secrets=[r2_secret],
    volumes={VOLUME_MOUNT: volume},
    timeout=600,
    max_containers=20,
)
def ocr_batch(frame_paths: list[str]) -> list[dict[str, Any]]:
    """Run EasyOCR on a batch of frame paths; crop to bottom third for scorebug.

    Args:
        frame_paths: Absolute paths to JPEG frames inside the shared volume.

    Returns:
        List of dicts with keys: frame_index, video_sec, quarter, clock.
    """
    import cv2
    import easyocr

    # Ensure GPU workers see the latest frames committed by extract_frames
    volume.reload()

    reader = easyocr.Reader(["en"], gpu=True, verbose=False)
    results: list[dict[str, Any]] = []

    for path in frame_paths:
        # Derive frame index from filename (frame_000001.jpg → 1)
        stem = Path(path).stem  # e.g. "frame_000001"
        frame_index = int(stem.split("_")[-1])
        video_sec = frame_index  # 1 FPS → frame_index == video_sec

        img = cv2.imread(path)
        if img is None:
            print(f"[WARN] frame {frame_index}: cv2.imread returned None for {path}")
            results.append(
                {
                    "frame_index": frame_index,
                    "video_sec": video_sec,
                    "quarter": None,
                    "clock": None,
                }
            )
            continue

        h, w = img.shape[:2]
        # ESPN scorebug occupies the bottom ~12% of the frame.
        scorebug = img[int(h * 0.88):, :]
        # Upscale 3x so small text is OCR-friendly.
        scorebug = cv2.resize(scorebug, None, fx=3, fy=3, interpolation=cv2.INTER_CUBIC)
        gray = cv2.cvtColor(scorebug, cv2.COLOR_BGR2GRAY)
        # CLAHE to boost contrast on the dark broadcast bar
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        gray = clahe.apply(gray)

        ocr_out = reader.readtext(gray, detail=0)
        combined = " ".join(ocr_out)

        clock = _parse_clock(ocr_out)   # token list → avoids cross-token corruption
        quarter = _parse_quarter(combined)

        # Log every frame so we can diagnose misses
        print(f"[OCR] frame {frame_index:04d} | raw={ocr_out!r} | clock={clock} | quarter={quarter}")

        results.append(
            {
                "frame_index": frame_index,
                "video_sec": video_sec,
                "quarter": quarter,
                "clock": clock,
            }
        )

    return results


# ---------------------------------------------------------------------------
# Step 3 — Time-series smoothing
# ---------------------------------------------------------------------------


def _clock_to_seconds(clock_str: str) -> float | None:
    """Convert 'MM:SS' → total seconds."""
    try:
        parts = clock_str.split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return None


def _seconds_to_clock(total: float) -> str:
    """Convert total seconds → 'MM:SS'."""
    mins = int(total) // 60
    secs = int(total) % 60
    return f"{mins:02d}:{secs:02d}"


def smooth_timeline(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fill gaps in the OCR results using linear interpolation.

    Where the quarter or clock is missing between two known good readings,
    interpolate so the output is a continuous, unbroken timeline.
    """
    if not raw:
        return raw

    # Sort by video_sec
    data = sorted(raw, key=lambda r: r["video_sec"])

    # Build a lookup for fast access
    by_sec: dict[int, dict] = {r["video_sec"]: dict(r) for r in data}

    min_sec = data[0]["video_sec"]
    max_sec = data[-1]["video_sec"]

    smoothed: list[dict[str, Any]] = []

    for sec in range(min_sec, max_sec + 1):
        if sec in by_sec and by_sec[sec]["clock"] is not None:
            smoothed.append(by_sec[sec])
            continue

        # Find the nearest known neighbours
        prev_entry = next(
            (by_sec[s] for s in range(sec - 1, min_sec - 1, -1) if s in by_sec and by_sec[s]["clock"]),
            None,
        )
        next_entry = next(
            (by_sec[s] for s in range(sec + 1, max_sec + 1) if s in by_sec and by_sec[s]["clock"]),
            None,
        )

        if prev_entry is None and next_entry is None:
            smoothed.append(
                {"frame_index": sec, "video_sec": sec, "quarter": None, "clock": None}
            )
            continue

        # Use the nearer neighbour for quarter (it doesn't change often)
        quarter = (prev_entry or next_entry)["quarter"]  # type: ignore[index]

        # Interpolate clock
        if prev_entry and next_entry:
            p_secs = _clock_to_seconds(prev_entry["clock"])
            n_secs = _clock_to_seconds(next_entry["clock"])
            if p_secs is not None and n_secs is not None:
                span = next_entry["video_sec"] - prev_entry["video_sec"]
                frac = (sec - prev_entry["video_sec"]) / span if span else 0
                interp = p_secs + frac * (n_secs - p_secs)
                clock = _seconds_to_clock(interp)
            else:
                clock = (prev_entry or next_entry)["clock"]  # type: ignore[index]
        elif prev_entry:
            # Extrapolate: assume clock counts down at 1 s/real-s
            p_secs = _clock_to_seconds(prev_entry["clock"])
            elapsed = sec - prev_entry["video_sec"]
            clock = _seconds_to_clock(p_secs - elapsed) if p_secs is not None else prev_entry["clock"]
        else:
            clock = next_entry["clock"]  # type: ignore[index]

        smoothed.append(
            {"frame_index": sec, "video_sec": sec, "quarter": quarter, "clock": clock}
        )

    return smoothed


# ---------------------------------------------------------------------------
# Step 4 — JSON generation & upload
# ---------------------------------------------------------------------------


def build_game_state(timeline: list[dict[str, Any]]) -> dict[str, Any]:
    game_state: dict[str, Any] = {}
    for entry in timeline:
        sec = entry["video_sec"]
        quarter = entry.get("quarter") or "UNK"
        clock = entry.get("clock") or "00:00"
        game_state[str(sec)] = {
            "game_time": f"{quarter}_{clock}",
            "stats": {},
            "recent_event": None,
        }
    return game_state


def save_and_upload(timeline: list[dict[str, Any]], game_state: dict[str, Any]) -> None:
    debug_dir = Path(__file__).parent / "debug_outputs"
    debug_dir.mkdir(exist_ok=True)

    timeline_path = debug_dir / "clock_timeline.json"
    game_state_path = debug_dir / "game_state.json"

    timeline_path.write_text(json.dumps(timeline, indent=2))
    game_state_path.write_text(json.dumps(game_state, indent=2))
    print(f"Saved locally:\n  {timeline_path}\n  {game_state_path}")

    # Upload to R2
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent / ".env.local")

    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )

    for local_path, r2_key in [
        (timeline_path, "clock_timeline.json"),
        (game_state_path, "game_state.json"),
    ]:
        s3.upload_file(str(local_path), R2_BUCKET, r2_key)
        print(f"Uploaded {r2_key} → R2:{R2_BUCKET}/{r2_key}")


# ---------------------------------------------------------------------------
# Local entrypoint — orchestrates the full pipeline
# ---------------------------------------------------------------------------


@app.local_entrypoint()
def main():
    print("=== Step 1: Download video & extract frames ===")
    frame_paths = extract_frames.remote()
    print(f"Total frames: {len(frame_paths)}")

    print("\n=== Step 2: Parallel GPU OCR ===")
    # Chunk frame_paths into batches
    batches = [
        frame_paths[i: i + BATCH_SIZE] for i in range(0, len(frame_paths), BATCH_SIZE)
    ]
    print(f"Fanning out {len(batches)} batches of up to {BATCH_SIZE} frames …")

    raw_results: list[dict[str, Any]] = []
    for batch_result in ocr_batch.map(batches, order_outputs=False):
        raw_results.extend(batch_result)

    print(f"Received {len(raw_results)} raw OCR results.")

    print("\n=== Step 3: Time-series smoothing ===")
    timeline = smooth_timeline(raw_results)
    print(f"Smoothed timeline: {len(timeline)} entries.")

    print("\n=== Step 4: Build game_state & persist ===")
    game_state = build_game_state(timeline)
    save_and_upload(timeline, game_state)

    print("\nDone.")
