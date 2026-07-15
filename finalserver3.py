import base64
import m3u8
import math
import os
import re
import hashlib
import urllib.request
import asyncio
import shutil
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
import uvicorn

app = FastAPI()

# Master cache directory
BASE_DESTINATION_FOLDER = r"H:\m3u8\split_cache"
# Increased to 60. Smaller parts = MUCH faster seeking because the downloader finishes them faster.
TOTAL_PARTS = 5 
LOOKAHEAD = 4  

# Global dict to track active downloads using asyncio Events for lock-free waiting
active_downloads = {}

async def download_segment(local_playlist_path: str, output_name: str, destination_folder: str, worker_count: int):
    """Downloads an m3u8 stream asynchronously using sub-processes."""
    download_id = f"{destination_folder}_{output_name}"
    
    # If already downloading, wait for the existing process to finish natively
    if download_id in active_downloads:
        await active_downloads[download_id].wait()
        return

    # Create an event to lock this download so other requests can wait for it
    event = asyncio.Event()
    active_downloads[download_id] = event
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
        # Run subprocess asynchronously so it doesn't block the FastAPI event loop
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await process.communicate()
        
        if process.returncode == 0:
            print(f"[✓] Finished: {output_name}")
        else:
            print(f"[x] Error downloading {output_name}: {stderr.decode().strip()}")
            
    except FileNotFoundError:
        print(f"[x] Error: 'N_m3u8DL-RE' executable not found in PATH.")
    finally:
        # Signal to any waiting requests that the file is ready (or failed)
        event.set()
        active_downloads.pop(download_id, None)

async def ensure_next_parts(dest_folder: str, current_part: int, max_parts: int, lookahead: int):
    """Background task: Downloads the NEXT parts sequentially."""
    for i in range(current_part + 1, current_part + lookahead + 1):
        if i <= max_parts:
            video_file = os.path.join(dest_folder, f"Video_Part_{i}.ts")
            playlist_file = os.path.join(dest_folder, f"playlist_part_{i}.m3u8")
            
            if not os.path.exists(video_file) and os.path.exists(playlist_file):
                await download_segment(playlist_file, f"Video_Part_{i}", dest_folder, worker_count=32)

async def delete_folder_later(folder_path: str, delay_seconds: int = 600):
    """Waits a specified time, then cleans up."""
    print(f"[!] Last part requested. Deleting {folder_path} in {delay_seconds//60} mins...")
    await asyncio.sleep(delay_seconds)
    try:
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            print(f"[🗑️] Auto-deleted finished video folder: {folder_path}")
    except Exception as e:
        print(f"[x] Failed to delete folder {folder_path}: {e}")

def split_m3u8_to_files(m3u8_path, dest_dir, num_parts, file_hash):
    """Splits the M3U8 and generates a master playlist for Jellyfin (Synchronous)."""
    os.makedirs(dest_dir, exist_ok=True)
    
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
            
    segments = playlist.segments
    total_segments = len(segments)
    chunk_size = math.ceil(total_segments / num_parts)
    
    print(f"Total segments: {total_segments}. Dividing into {num_parts} parts...")

    part_durations = []
    max_duration = 0

    for i in range(num_parts):
        start = i * chunk_size
        end = min(start + chunk_size, total_segments)
        part_segments = segments[start:end]
        
        duration = sum(seg.duration for seg in part_segments if seg.duration)
        if duration == 0: continue
        
        part_durations.append(duration)
        if duration > max_duration: max_duration = duration
        
        new_playlist = m3u8.M3U8()
        new_playlist.target_duration = playlist.target_duration
        new_playlist.version = playlist.version
        
        for segment in part_segments:
            if playlist.base_uri and not segment.uri.startswith("http"):
                segment.uri = playlist.base_uri + segment.uri
            new_playlist.add_segment(segment)
        
        output_filename = os.path.join(dest_dir, f"playlist_part_{i+1}.m3u8")
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(new_playlist.dumps())
            
    jellyfin_m3u8_path = os.path.join(dest_dir, "jellyfin.m3u8")
    with open(jellyfin_m3u8_path, "w", encoding="utf-8") as f:
        f.write("#EXTM3U\n")
        f.write("#EXT-X-VERSION:3\n")
        f.write("#EXT-X-PLAYLIST-TYPE:VOD\n")
        f.write(f"#EXT-X-TARGETDURATION:{math.ceil(max_duration)}\n")
        
        for i, duration in enumerate(part_durations, start=1):
            f.write(f"#EXTINF:{duration:.6f},\n")
            f.write(f"/stream/{file_hash}/{i}.ts\n")
            if i < num_parts:
                f.write("#EXT-X-DISCONTINUITY\n")
        f.write("#EXT-X-ENDLIST\n")
        
    return True

# --- RANGE REQUEST GENERATOR (Crucial for Seeking) ---
def stream_file_range(path: str, start: int, end: int, chunk_size: int = 1024 * 1024):
    """Yields file chunks for HTTP 206 Partial Content."""
    with open(path, "rb") as f:
        f.seek(start)
        while (pos := f.tell()) <= end:
            read_size = min(chunk_size, end + 1 - pos)
            yield f.read(read_size)

@app.get("/{encoded_path}")
async def handle_strm_request(encoded_path: str, background_tasks: BackgroundTasks):
    """Endpoint 1: Returns the Master Playlist."""
    try:
        padded_encoded = encoded_path + '=' * (-len(encoded_path) % 4)
        decoded_bytes = base64.urlsafe_b64decode(padded_encoded)
        file_path = decoded_bytes.decode('utf-8')
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 string")

    file_hash = hashlib.md5(file_path.encode()).hexdigest()[:8]
    destination_folder = os.path.join(BASE_DESTINATION_FOLDER, f"Stream_{file_hash}")
    jellyfin_playlist_path = os.path.join(destination_folder, "jellyfin.m3u8")

    if not os.path.exists(jellyfin_playlist_path):
        success = split_m3u8_to_files(file_path, destination_folder, TOTAL_PARTS, file_hash)
        if success:
            print(f"--- Phase 1: Prioritizing Part 1 ---")
            playlist_1 = os.path.join(destination_folder, "playlist_part_1.m3u8")
            
            # Start Part 1 explicitly and await it so it's ready for immediate playback
            await download_segment(playlist_1, "Video_Part_1", destination_folder, worker_count=64)
            
            # Launch background buffering
            background_tasks.add_task(ensure_next_parts, destination_folder, 1, TOTAL_PARTS, LOOKAHEAD)

    if os.path.exists(jellyfin_playlist_path):
        return FileResponse(jellyfin_playlist_path, media_type="application/vnd.apple.mpegurl")
    raise HTTPException(status_code=500, detail="Failed to initialize stream")

@app.get("/stream/{file_hash}/{part_num}.ts")
async def serve_video_chunk(file_hash: str, part_num: int, request: Request, background_tasks: BackgroundTasks):
    """Endpoint 2: Feeds the .ts files with Partial Content support for seeking."""
    dest_folder = os.path.join(BASE_DESTINATION_FOLDER, f"Stream_{file_hash}")
    file_path = os.path.join(dest_folder, f"Video_Part_{part_num}.ts")
    playlist_path = os.path.join(dest_folder, f"playlist_part_{part_num}.m3u8")
    download_id = f"{dest_folder}_Video_Part_{part_num}"
    
    if not os.path.exists(dest_folder):
        raise HTTPException(404, "Stream folder not found")

    # 1. Event-based wait. If it's already actively downloading, wait for the event to clear.
    if download_id in active_downloads:
        print(f"[~] Client waiting on active download for Part {part_num}...")
        await active_downloads[download_id].wait()

    # 2. Expedite Seeking: If it STILL doesn't exist, the user seeked forward. Download it right now.
    if not os.path.exists(file_path):
        print(f"[!] Fast-forward detected! Expediting Part {part_num}...")
        await download_segment(playlist_path, f"Video_Part_{part_num}", dest_folder, worker_count=32)

    # Sanity check after download completes
    if not os.path.exists(file_path):
        raise HTTPException(404, "File download failed or timed out.")

    # 3. Trigger buffering for the next parts in the background
    background_tasks.add_task(ensure_next_parts, dest_folder, part_num, TOTAL_PARTS, LOOKAHEAD)
    if part_num == TOTAL_PARTS:
        background_tasks.add_task(delete_folder_later, dest_folder, delay_seconds=600)

    # 4. HTTP Range Processing (Crucial for Seeking in large .ts files)
    file_size = os.path.getsize(file_path)
    start = 0
    end = file_size - 1
    status_code = 200

    range_header = request.headers.get("range")
    if range_header:
        match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if match:
            start = int(match.group(1))
            if match.group(2):
                end = int(match.group(2))
            status_code = 206

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(end - start + 1),
    }

    return StreamingResponse(
        stream_file_range(file_path, start, end), 
        status_code=status_code, 
        headers=headers, 
        media_type="video/mp2t"
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8800)