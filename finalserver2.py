import base64
import m3u8
import math
import os
import subprocess
import hashlib
import urllib.request
import time
import shutil
import asyncio
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
import uvicorn

app = FastAPI()

# Master cache directory
BASE_DESTINATION_FOLDER = r"H:\m3u8\split_cache"
TOTAL_PARTS = 20
LOOKAHEAD = 2  # How many future parts to download

# Global set to track active downloads so we don't accidentally download the same part twice
active_downloads = set()

def download_segment(local_playlist_path, output_name, destination_folder, worker_count):
    """Downloads an m3u8 stream to a .ts file."""
    download_id = f"{destination_folder}_{output_name}"
    
    # Prevent duplicate downloads of the same part
    if download_id in active_downloads:
        return
        
    active_downloads.add(download_id)
    os.makedirs(destination_folder, exist_ok=True)
    print(f"[↓] Starting: {output_name} (Workers: {worker_count})")
    
    command = [
        "N_m3u8DL-RE", 
        local_playlist_path,
        "--save-dir", destination_folder,
        "--save-name", output_name,
        "--thread-count", str(worker_count),
        "-M", "format=ts",
        "--live-perform-as-vod"
    ]
    
    try:
        subprocess.run(command, check=True, text=True)
        print(f"[✓] Finished: {output_name}")
    except subprocess.CalledProcessError as e:
        print(f"[x] Error downloading {output_name}:")
        print(e.stderr)
    except FileNotFoundError:
        print(f"[x] Error: 'N_m3u8DL-RE' executable not found.")
    finally:
        # Free up the lock when done
        active_downloads.discard(download_id)

def ensure_next_parts(dest_folder: str, current_part: int, max_parts: int, lookahead: int):
    """Background task: Downloads the NEXT 2 parts based on where the user is."""
    for i in range(current_part + 1, current_part + lookahead + 1):
        if i <= max_parts:
            video_file = os.path.join(dest_folder, f"Video_Part_{i}.ts")
            playlist_file = os.path.join(dest_folder, f"playlist_part_{i}.m3u8")
            
            # If the video part doesn't exist, start downloading it
            if not os.path.exists(video_file) and os.path.exists(playlist_file):
                download_segment(playlist_file, f"Video_Part_{i}", dest_folder, 4)

async def delete_folder_later(folder_path: str, delay_seconds: int = 600):
    """Background task: Waits a specified time, then deletes the folder and all contents."""
    print(f"[!] Last part requested. Scheduling deletion of {folder_path} in {delay_seconds//60} minutes...")
    await asyncio.sleep(delay_seconds)
    try:
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            print(f"[🗑️] Auto-deleted finished video folder: {folder_path}")
    except Exception as e:
        print(f"[x] Failed to delete folder {folder_path}: {e}")

def split_m3u8_to_files(m3u8_path, dest_dir, num_parts, file_hash):
    """Splits the M3U8 and generates a master playlist for Jellyfin."""
    os.makedirs(dest_dir, exist_ok=True)
    
    # 1. Fetch Playlist Data
    if m3u8_path.startswith("http://") or m3u8_path.startswith("https://"):
        req = urllib.request.Request(m3u8_path, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            with urllib.request.urlopen(req) as response:
                m3u8_content = response.read().decode('utf-8')
            playlist = m3u8.loads(m3u8_content)
            playlist.base_uri = m3u8_path.rsplit('/', 1)[0] + '/'
        except Exception as e:
            print(f"[x] Failed to download remote m3u8: {e}")
            return False
    else:
        with open(m3u8_path, 'r', encoding='utf-8') as f:
            playlist = m3u8.loads(f.read())
    
    # 2. Setup Chunks
    segments = playlist.segments
    total_segments = len(segments)
    chunk_size = math.ceil(total_segments / num_parts)
    
    print(f"Total segments: {total_segments}. Dividing into {num_parts} parts...")

    part_durations = []
    max_duration = 0

    # 3. Create the individual M3U8 local files
    for i in range(num_parts):
        start = i * chunk_size
        end = min(start + chunk_size, total_segments)
        part_segments = segments[start:end]
        
        duration = sum(seg.duration for seg in part_segments if seg.duration)
        part_durations.append(duration)
        if duration > max_duration:
            max_duration = duration
        
        new_playlist = m3u8.M3U8()
        new_playlist.target_duration = playlist.target_duration
        new_playlist.version = playlist.version
        
        for segment in part_segments:
            if playlist.base_uri and not segment.uri.startswith("http"):
                segment.uri = playlist.base_uri + segment.uri
            new_playlist.add_segment(segment)
        
        # Save as playlist_part_1.m3u8, etc.
        output_filename = os.path.join(dest_dir, f"playlist_part_{i+1}.m3u8")
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(new_playlist.dumps())
            
    # 4. Generate the MASTER PLAYLIST for Jellyfin
    jellyfin_m3u8_path = os.path.join(dest_dir, "jellyfin.m3u8")
    with open(jellyfin_m3u8_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        f.write("#EXT-X-VERSION:3\n")
        f.write("#EXT-X-PLAYLIST-TYPE:VOD\n")
        f.write(f"#EXT-X-TARGETDURATION:{math.ceil(max_duration)}\n")
        
        for i, duration in enumerate(part_durations, start=1):
            f.write(f"#EXTINF:{duration:.6f},\n")
            f.write(f"/stream/{file_hash}/{i}\n")
            if i < num_parts:
                f.write("#EXT-X-DISCONTINUITY\n")
        f.write("#EXT-X-ENDLIST\n")
        
    return True

@app.get("/{encoded_path}")
def handle_strm_request(encoded_path: str, background_tasks: BackgroundTasks):
    """Endpoint 1: Receives the .strm request and returns the Master Playlist."""
    try:
        padded_encoded = encoded_path + '=' * (-len(encoded_path) % 4)
        decoded_bytes = base64.urlsafe_b64decode(padded_encoded)
        file_path = decoded_bytes.decode('utf-8')
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 string")

    is_url = file_path.startswith("http://") or file_path.startswith("https://")
    if not is_url and not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Original file not found")

    file_hash = hashlib.md5(file_path.encode()).hexdigest()[:8]
    # Keep folder naming clean
    destination_folder = os.path.join(BASE_DESTINATION_FOLDER, f"Stream_{file_hash}")
    jellyfin_playlist_path = os.path.join(destination_folder, "jellyfin.m3u8")

    # If this is the very first time we are playing this movie
    if not os.path.exists(jellyfin_playlist_path):
        success = split_m3u8_to_files(file_path, destination_folder, TOTAL_PARTS, file_hash)
        
        if success:
            print(f"--- Phase 1: Prioritizing Part 1 into '{destination_folder}' ---")
            playlist_1 = os.path.join(destination_folder, "playlist_part_1.m3u8")
            
            # Download Part 1 synchronously so Jellyfin starts instantly
            download_segment(playlist_1, "Video_Part_1", destination_folder, worker_count=30)
            
            # Spawn background task to download Part 2 and 3
            background_tasks.add_task(ensure_next_parts, destination_folder, current_part=1, max_parts=TOTAL_PARTS, lookahead=LOOKAHEAD)

    if os.path.exists(jellyfin_playlist_path):
        return FileResponse(jellyfin_playlist_path, media_type="application/vnd.apple.mpegurl")
    else:
        raise HTTPException(status_code=500, detail="Failed to initialize stream")

@app.get("/stream/{file_hash}/{part_num}.ts")
def serve_video_chunk(file_hash: str, part_num: int, background_tasks: BackgroundTasks):
    """Endpoint 2: Feeds the .ts files. Triggers auto-downloads and auto-deletion."""
    import threading # Ensure you add 'import threading' at the very top of your file!
    
    dest_folder = os.path.join(BASE_DESTINATION_FOLDER, f"Stream_{file_hash}")
            
    if not os.path.exists(dest_folder):
        raise HTTPException(404, "Stream folder not found")
        
    file_path = os.path.join(dest_folder, f"Video_Part_{part_num}.ts")
    playlist_path = os.path.join(dest_folder, f"playlist_part_{part_num}.m3u8")
    
    print(f"[>] Player requested playback of Part {part_num} / {TOTAL_PARTS}...")

    # THE SEEKING FIX: Use a Thread to instantly force the download to start!
    if not os.path.exists(file_path):
        print(f"[!] Fast-forward detected! Expediting Part {part_num}...")
        threading.Thread(
            target=download_segment, 
            args=(playlist_path, f"Video_Part_{part_num}", dest_folder, 15)
        ).start()

    # Automatically start preparing the NEXT parts instantly using threads as well
    threading.Thread(
        target=ensure_next_parts, 
        args=(dest_folder, part_num, TOTAL_PARTS, LOOKAHEAD)
    ).start()

    # Check if this is the final part. If it is, schedule the deletion using the normal background task
    if part_num == TOTAL_PARTS:
        background_tasks.add_task(delete_folder_later, dest_folder, delay_seconds=600)
    
    # Wait for the requested part to exist
    timeout = 600 
    start_time = time.time()
    while not os.path.exists(file_path):
        if time.time() - start_time > timeout:
            raise HTTPException(404, "File download timed out or failed")
        time.sleep(1) 
        
    return FileResponse(file_path, media_type="video/mp2t")
    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8800)