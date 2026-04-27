#!/usr/bin/env python3
"""
player_clips.py
===============
Possession Detection Pipeline — Modal GPU orchestration.

Uses the Roboflow player-detection model's possession-relevant classes:

  player-in-possession  priority 3  continuous dribble signal
  player-jump-shot      priority 2  ball guaranteed in hand
  player-layup-dunk     priority 1  ball at rim — strongest signal
  player-shot-block     SKIP        fires on defender, not possessor

Depends on:
  vision/processed_game_state.json   (produced by build_game_state.py)
  vision/data/nba/player_boxscore.json

Output:
  vision/debug_outputs/possession_timeline.json
  vision/possession_game_state.json
  Both files are uploaded to R2.

Run with:
  modal run vision/player_clips.py
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import modal
from dotenv import dotenv_values

# ── credentials (same pattern as modal_clock_ocr.py) ─────────────────────────

_DOTENV_PATH = Path(__file__).resolve().parent / ".env.local"
_env_vars: dict[str, str] = {
    k: v for k, v in dotenv_values(_DOTENV_PATH).items() if v is not None
}
r2_secret = modal.Secret.from_dict(_env_vars)

# ── Modal image ───────────────────────────────────────────────────────────────

image = (
    modal.Image.debian_slim()
    .apt_install(["ffmpeg", "libsm6", "libxext6", "libgl1"])
    .pip_install(
        [
            "inference-sdk",          # Roboflow hosted-API client
            "opencv-python-headless",
            "boto3",
            "python-dotenv",
            "numpy",
        ]
    )
)

app    = modal.App("basketball-possession", image=image)
volume = modal.Volume.from_name("possession-frames", create_if_missing=True)

VOLUME_MOUNT = "/mnt/frames"

# ── constants ─────────────────────────────────────────────────────────────────

VISION_DIR        = Path(__file__).parent
DATA_DIR          = VISION_DIR / "data" / "nba"
DEBUG_DIR         = VISION_DIR / "debug_outputs"
PROCESSED_GS_PATH = VISION_DIR / "processed_game_state.json"
BOXSCORE_PATH     = DATA_DIR / "player_boxscore.json"
OUT_PATH          = VISION_DIR / "possession_game_state.json"

R2_BUCKET  = "gamefootage"
VIDEO_KEY  = (
    "uploads/1776649531479-067fdd79-3133-44d5-8897-e4861ea94bc2"
    "__lakers_warriors_christmas_first_minute.mp4"
)
LOCAL_VIDEO = "/tmp/video.mp4"
BATCH_SIZE  = 30

MIN_DWELL_SECONDS      = 2
MAX_FORWARD_FILL_SECONDS = 4

# Lower number = higher priority (stronger possession signal)
POSSESSION_PRIORITY: dict[str, int] = {
    "player-layup-dunk":    1,
    "player-jump-shot":     2,
    "player-in-possession": 3,
}

PLAYER_MODEL_ID = "basketball-player-detection-3-ycjdo/4"
JERSEY_MODEL_ID = "basketball-jersey-numbers-ocr/3"


# ── R2 client (identical to modal_clock_ocr.py) ───────────────────────────────

def _r2_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


# ── Step 1: frame extraction (CPU) ────────────────────────────────────────────

@app.function(
    secrets=[r2_secret],
    volumes={VOLUME_MOUNT: volume},
    timeout=1800,
    cpu=4,
)
def extract_frames() -> list[str]:
    """Download video from R2 and extract 1 FPS JPEG frames to the shared volume."""
    import glob

    frames_dir = Path(VOLUME_MOUNT) / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {VIDEO_KEY} …")
    s3 = _r2_client()
    s3.download_file(R2_BUCKET, VIDEO_KEY, LOCAL_VIDEO)
    print("Download complete.")

    print("Extracting 1 FPS frames …")
    subprocess.run(
        [
            "ffmpeg", "-y", "-i", LOCAL_VIDEO,
            "-vf", "fps=1", "-q:v", "2",
            str(frames_dir / "frame_%06d.jpg"),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    volume.commit()

    frame_paths = sorted(glob.glob(str(frames_dir / "frame_*.jpg")))
    print(f"Extracted {len(frame_paths)} frames.")
    return frame_paths


# ── Step 2: per-batch possession detection (GPU T4) ───────────────────────────

def _parse_jersey_from_response(response: Any) -> str | None:
    """Extract a 1–3 digit jersey number from a VLM text response."""
    text = ""
    if isinstance(response, str):
        text = response
    elif isinstance(response, dict):
        text = str(
            response.get("response")
            or response.get("result")
            or response.get("text")
            or ""
        )
    elif hasattr(response, "response"):
        text = str(response.response)
    else:
        text = str(response)

    text = text.strip()
    m = re.search(r"\b(\d{1,3})\b", text)
    return m.group(1) if m else None


@app.function(
    gpu="T4",
    secrets=[r2_secret],
    volumes={VOLUME_MOUNT: volume},
    timeout=600,
    max_containers=20,
)
def detect_possession_batch(frame_paths: list[str]) -> list[dict[str, Any]]:
    """Run possession detection on a batch of 1 FPS frames.

    For each frame:
      1. Call Roboflow player-detection model (hosted API).
      2. Filter detections to possession-relevant classes.
      3. Resolve conflicts within the frame (priority → confidence → bbox area).
      4. Crop the winning player bbox; run jersey OCR VLM on the crop.
      5. Return structured result per frame.
    """
    import cv2
    import numpy as np
    from inference_sdk import InferenceHTTPClient

    volume.reload()

    client = InferenceHTTPClient(
        api_url="https://detect.roboflow.com",
        api_key=os.environ["ROBOFLOW_API_KEY"],
    )

    results: list[dict[str, Any]] = []

    for path in frame_paths:
        stem = Path(path).stem                          # "frame_000021"
        frame_index = int(stem.split("_")[-1])
        video_sec   = frame_index                       # 1 FPS → frame == second

        img = cv2.imread(path)
        if img is None:
            print(f"[WARN] frame {frame_index}: imread returned None — skipping")
            results.append({
                "frame_index": frame_index,
                "video_sec":   video_sec,
                "jersey_num":  None,
                "class_name":  None,
                "confidence":  0.0,
            })
            continue

        # ── player detection ──────────────────────────────────────────────────
        try:
            det_result = client.infer(path, model_id=PLAYER_MODEL_ID)
            predictions = det_result.get("predictions", [])
        except Exception as exc:
            print(f"[WARN] frame {frame_index}: detection failed — {exc}")
            results.append({
                "frame_index": frame_index,
                "video_sec":   video_sec,
                "jersey_num":  None,
                "class_name":  None,
                "confidence":  0.0,
            })
            continue

        # ── filter to possession classes ──────────────────────────────────────
        candidates = [
            p for p in predictions
            if p.get("class") in POSSESSION_PRIORITY
        ]

        if not candidates:
            print(f"[OCR] frame {frame_index:04d} | no possession class detected")
            results.append({
                "frame_index": frame_index,
                "video_sec":   video_sec,
                "jersey_num":  None,
                "class_name":  None,
                "confidence":  0.0,
            })
            continue

        # ── conflict resolution: priority → confidence → bbox area ────────────
        def _sort_key(p: dict) -> tuple:
            area = p.get("width", 0) * p.get("height", 0)
            return (
                POSSESSION_PRIORITY[p["class"]],
                -float(p.get("confidence", 0)),
                -float(area),
            )

        winner = sorted(candidates, key=_sort_key)[0]

        # ── crop player bbox ──────────────────────────────────────────────────
        h, w = img.shape[:2]
        cx, cy = float(winner["x"]), float(winner["y"])
        bw, bh = float(winner["width"]), float(winner["height"])

        x1 = max(0, int(cx - bw / 2))
        y1 = max(0, int(cy - bh / 2))
        x2 = min(w, int(cx + bw / 2))
        y2 = min(h, int(cy + bh / 2))

        crop = img[y1:y2, x1:x2]

        # ── jersey OCR on crop ────────────────────────────────────────────────
        jersey_num: str | None = None
        if crop.size > 0:
            try:
                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                    cv2.imwrite(tmp.name, crop)
                    tmp_path = tmp.name

                ocr_result = client.infer(
                    tmp_path,
                    model_id=JERSEY_MODEL_ID,
                )
                Path(tmp_path).unlink(missing_ok=True)

                jersey_num = _parse_jersey_from_response(ocr_result)
            except Exception as exc:
                print(f"[WARN] frame {frame_index}: jersey OCR failed — {exc}")

        print(
            f"[POS] frame {frame_index:04d} | "
            f"class={winner['class']} conf={winner['confidence']:.2f} | "
            f"jersey={jersey_num}"
        )

        results.append({
            "frame_index": frame_index,
            "video_sec":   video_sec,
            "jersey_num":  jersey_num,
            "class_name":  winner["class"],
            "confidence":  float(winner.get("confidence", 0)),
        })

    return results


# ── Step 3: upload helper ──────────────────────────────────────────────────────

def save_and_upload(
    timeline_path: Path,
    gs_path: Path,
) -> None:
    """Write both artifacts locally then upload to R2."""
    from dotenv import load_dotenv
    import boto3

    load_dotenv(_DOTENV_PATH)

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )

    for local_path, r2_key in [
        (timeline_path, "possession_timeline.json"),
        (gs_path,       "possession_game_state.json"),
    ]:
        s3.upload_file(str(local_path), R2_BUCKET, r2_key)
        print(f"Uploaded {r2_key} → R2:{R2_BUCKET}/{r2_key}")


# ── local entrypoint ──────────────────────────────────────────────────────────

@app.local_entrypoint()
def main() -> None:
    # Import pure-function helpers — no Modal required
    from possession_utils import (
        aggregate_to_seconds,
        apply_dwell_filter,
        apply_forward_fill,
        apply_pbp_overrides,
        build_jersey_to_pid,
        jersey_to_personid,
        merge_with_game_state,
        possession_coverage_report,
        validate_oncourt,
    )

    # ── 1. Frame extraction ───────────────────────────────────────────────────
    print("=== Step 1: Extract frames ===")
    frame_paths = extract_frames.remote()
    print(f"  {len(frame_paths)} frames available")

    # ── 2. Parallel possession detection ─────────────────────────────────────
    print("\n=== Step 2: Detect possession (parallel GPU batches) ===")
    batches = [
        frame_paths[i: i + BATCH_SIZE]
        for i in range(0, len(frame_paths), BATCH_SIZE)
    ]
    print(f"  {len(batches)} batch(es) of ≤ {BATCH_SIZE} frames")

    raw: list[dict] = []
    for batch_result in detect_possession_batch.map(batches, order_outputs=False):
        raw.extend(batch_result)
    print(f"  {len(raw)} frame-level results received")

    # ── 3. Post-processing chain (local, pure functions) ─────────────────────
    print("\n=== Step 3: Post-processing ===")

    by_sec   = aggregate_to_seconds(raw)
    dwell    = apply_dwell_filter(by_sec, MIN_DWELL_SECONDS)
    filled   = apply_forward_fill(dwell, MAX_FORWARD_FILL_SECONDS)

    registry  = build_jersey_to_pid(BOXSCORE_PATH)
    with_pids = jersey_to_personid(filled, registry)
    validated = validate_oncourt(with_pids, PROCESSED_GS_PATH)
    final     = apply_pbp_overrides(validated, PROCESSED_GS_PATH, BOXSCORE_PATH)

    possession_coverage_report(final)

    # ── 4. Persist ────────────────────────────────────────────────────────────
    print("\n=== Step 4: Merge and save ===")

    DEBUG_DIR.mkdir(exist_ok=True)
    timeline_out = DEBUG_DIR / "possession_timeline.json"
    timeline_out.write_text(
        json.dumps({str(k): v for k, v in sorted(final.items())}, indent=2)
    )
    print(f"  ✓ possession_timeline.json  ({len(final)} entries)")

    merge_with_game_state(final, PROCESSED_GS_PATH, OUT_PATH)

    save_and_upload(timeline_out, OUT_PATH)

    print("\n✓ player_clips.py complete")
