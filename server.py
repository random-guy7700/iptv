import concurrent.futures
import m3u8
import math
import os
import pathlib
import subprocess

def download_segment(m3u8_url, output_name, destination_folder, worker_count):
    """
    Downloads a single m3u8 stream to mp4 with a specific number of workers using N_m3u8DL-RE.
    """
    # Ensure the destination folder exists before downloading
    os.makedirs(destination_folder, exist_ok=True)
    
    print(f"[↓] Starting: {output_name} (Workers: {worker_count})")
    
    # Construct the N_m3u8DL-RE command
    # -M format=mp4 forces muxing to MP4 (requires ffmpeg in PATH)
    command = [
        "N_m3u8DL-RE", 
        m3u8_url,
        "--save-dir", destination_folder,
        "--save-name", output_name,
        "--thread-count", str(worker_count),
        "-M", "format=mp4" ,
        "--live-perform-as-vod"
    ]
    print(command)
    try:
        # Run the command, capturing the output to keep the terminal clean
        subprocess.run(command, check=True, text=True)
        print(f"[✓] Finished: {output_name}")
        
    except subprocess.CalledProcessError as e:
        # If the download fails, print the error output from N_m3u8DL-RE
        print(f"[x] Error downloading {output_name}:")
        print(e.stderr)
    except FileNotFoundError:
        print(f"[x] Error: 'N_m3u8DL-RE' executable not found. Ensure it is installed and added to your system PATH.")

def smart_download_playlist(urls, destination_folder):
    """
    Downloads the first URL with max workers, and the rest sequentially 
    one by one with fewer workers.
    """
    if not urls:
        return
    
    # 1. Download Part 1 synchronously with MAX workers
    print(f"--- Phase 1: Prioritizing Part 1 into '{destination_folder}' ---")
    download_segment(urls[0], "Video_Part_1", destination_folder, worker_count=30)
    
    # 2. Download Parts 2-N sequentially one by one
    if len(urls) > 1:
        print("\n--- Phase 2: Downloading remaining parts sequentially ---")
        
        for i, url in enumerate(urls[1:], start=2):
            output_name = f"Video_Part_{i}"
            
            # This will wait for each download to finish before starting the next
            download_segment(url, output_name, destination_folder, worker_count=3)
            
        print("\nAll sequential downloads complete!")
        
def split_m3u8_to_files(m3u8_path, dest_dir, num_parts):
    # Setup paths
    filename = os.path.basename(m3u8_path).replace(".m3u8", "")
    
    # Create the destination folder if it doesn't exist
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    
    # Read and parse
    with open(m3u8_path, 'r', encoding='utf-8') as f:
        playlist = m3u8.loads(f.read())
    
    segments = playlist.segments
    total_segments = len(segments)
    chunk_size = math.ceil(total_segments / num_parts)
    
    print(f"Total segments: {total_segments}. Dividing into {num_parts} parts in '{dest_dir}'...")

    # Initialize an array to store the file locations
    output_files = []

    for i in range(num_parts):
        start = i * chunk_size
        end = min(start + chunk_size, total_segments)
        part_segments = segments[start:end]
        
        # Create a new M3U8 file object for this part
        new_playlist = m3u8.M3U8()
        
        # Copy essential metadata so the new playlist is valid for players
        new_playlist.target_duration = playlist.target_duration
        new_playlist.version = playlist.version
        
        # Add the segments to the new playlist object one by one
        for segment in part_segments:
            new_playlist.add_segment(segment)
        
        # Save the new m3u8 file
        output_filename = os.path.join(dest_dir, f"{filename}_part_{i+1}.m3u8")
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(new_playlist.dumps())
            
        print(f"Created: {output_filename}")
        
        # Append the new file path to our array
        output_files.append(output_filename)
        
    # Return the array containing all the file locations
    return output_files

# ==========================================
# Run the Code
# ==========================================
if __name__ == "__main__":
    # Replace these with your actual 5 m3u8 link
    

    file_path = r"H:\m3u8\movies_m3u8\MILFY – Maitland Ward And Brandi Love – Ultimate MILFs Brandi And Maitland Share A B Day BBC.m3u8"
    destination_folder = r"H:\m3u8\split_cache" 
    video_urls=split_m3u8_to_files(file_path, destination_folder, 5)
    print(video_urls)
    smart_download_playlist(video_urls, destination_folder)