import subprocess
import time
import threading
import json
import os
import smtplib
import hashlib
from email.mime.text import MIMEText
from datetime import datetime

CONFIG_FILE = "OBSOutputConfig.json"
EMAIL_ENABLED = True

# Email settings
SMTP_SERVER = "smtp-legacy.office365.com"
SMTP_PORT = 587
EMAIL_USERNAME = "mib@lillyhubtv.com"
EMAIL_PASSWORD = "N0t1fy!@!"
EMAIL_FROM = "mib@lillyhubtv.com"
EMAIL_TO = ["drogers@lillybroadcasting.com"]

output_threads = {}
output_configs = {}
running_outputs = {}
output_procs = {}
output_alert_states = {}  # Track alert state: None, 'notified_down', 'restored'
output_first_failure_time = {}  # Track when failures started

lock = threading.Lock()

FFMPEG_PATH = "/home/lilly/ffmpeg/ffmpeg"
PROCESS_TIMEOUT = 300  # 5 minutes - restart if no activity
RETRY_DELAY = 5  # seconds between restart attempts
ALERT_THRESHOLD = 180  # 3 minutes - only alert if down this long
MAX_RETRY_DELAY = 30  # Maximum retry delay in seconds

def get_log_filename():
    return f"/home/lilly/Logs/OBSOutput-{datetime.now().strftime('%Y-%m-%d')}.log"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    try:
        with open(get_log_filename(), "a") as f:
            f.write(full_message + "\n")
    except Exception as e:
        print(f"Failed to write to log file: {e}")

def send_email(subject, body):
    if not EMAIL_ENABLED:
        return
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(EMAIL_TO)

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        log(f"Sent email: {subject}")
    except Exception as e:
        log(f"Failed to send email: {e}")

def load_config():
    try:
        with open(CONFIG_FILE, "r") as f:
            data = json.load(f)
            return data.get("outputs", [])
    except FileNotFoundError:
        log(f"Config file {CONFIG_FILE} not found")
        return []
    except json.JSONDecodeError as e:
        log(f"Error parsing config JSON: {e}")
        return []
    except Exception as e:
        log(f"Error loading config: {e}")
        return []

def hash_config(outputs):
    return hashlib.md5(json.dumps(outputs, sort_keys=True).encode()).hexdigest()

def output_key(cfg):
    return f"{cfg['udp_port']}:{cfg['serial']}:{cfg['output']}:{cfg.get('name', '')}"

def read_process_output(pipe, output_list, process_name):
    """Read process output in a non-blocking way"""
    try:
        for line in iter(pipe.readline, b''):
            if line:
                decoded = line.decode(errors='ignore').strip()
                if decoded:
                    output_list.append(decoded)
                    log(f"[{process_name}] {decoded}")
    except Exception as e:
        log(f"Error reading output from {process_name}: {e}")

def start_output(cfg):
    name = cfg.get("name", f"UDP{cfg['udp_port']}->DT{cfg['serial']}:{cfg['output']}")
    key = output_key(cfg)

    # Listen on UDP with reuse so restarts don't leave the port stuck
    base_in = (
        f"udp://@0.0.0.0:{cfg['udp_port']}"
        f"?pkt_size=1316&fifo_size=1000000&overrun_nonfatal=1&reuse=1"
    )
    out_dev = f"{cfg['serial']}:{cfg['output']}"

    # 3 candidate VF chains:
    #   A) True interlace via tinterlace (requires --enable-gpl)
    #   B) True interlace via interlace filter (if present in your build)
    #   C) Last-resort: mark TFF (not true interlace, but SDI devices will at least flag as interlaced)
    vf_candidates = [
        # A) Preferred: tinterlace (29.97i from 59.94p)
        "scale=1920:1080:flags=bicubic,setsar=1,format=yuv422p,"
        "tinterlace=mode=merge,fieldorder=tff,format=uyvy422",

        # B) Alternative: interlace (some builds have this even without GPL)
        "scale=1920:1080:flags=bicubic,setsar=1,format=yuv422p,"
        "interlace=scan=tff:lowpass=0,fieldorder=tff,format=uyvy422",

        # C) Fallback: tag TFF only (progressive frames marked as interlaced)
        "scale=1920:1080:flags=bicubic,setsar=1,fieldorder=tff,format=uyvy422",
    ]

    # Common args (1080i59.94 TFF target, embedded 24-bit PCM)
    common_tail = [
        "-r", "30000/1001",
        "-c:v", "wrapped_avframe",
        "-colorspace", "bt709", "-color_primaries", "bt709", "-color_trc", "bt709",
        "-ar", "48000", "-ac", "2",
        "-sample_fmt", "s32", "-c:a", "pcm_s24le",
        "-f", "dektec", out_dev
    ]

    successful_vf = None  # Remember which filter worked
    consecutive_failures = 0
    current_retry_delay = RETRY_DELAY

    while running_outputs.get(key, False):
        # Try the successful filter first if we found one
        filters_to_try = [successful_vf] + vf_candidates if successful_vf else vf_candidates
        filters_to_try = [f for f in filters_to_try if f]  # Remove None
        
        started_successfully = False
        
        for vf_graph in filters_to_try:
            if not running_outputs.get(key, False):
                break
                
            command = [
                FFMPEG_PATH,
                "-hide_banner", "-nostats", "-loglevel", "level+info",
                "-thread_queue_size", "1024",
                "-probesize", "2M", "-analyzeduration", "2M",
                "-i", base_in,
                "-map", "0:v:0", "-map", "0:a:0",
                "-vf", vf_graph,
            ] + common_tail

            try:
                log(f"Starting output: {name}")
                log("Command: " + " ".join(command))

                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    bufsize=1,
                    start_new_session=True
                )
                
                with lock:
                    output_procs[key] = process

                # Start threads to read stdout/stderr without blocking
                stderr_lines = []
                stdout_lines = []
                
                stderr_thread = threading.Thread(
                    target=read_process_output, 
                    args=(process.stderr, stderr_lines, f"{name}-STDERR"),
                    daemon=True
                )
                stdout_thread = threading.Thread(
                    target=read_process_output,
                    args=(process.stdout, stdout_lines, f"{name}-STDOUT"),
                    daemon=True
                )
                
                stderr_thread.start()
                stdout_thread.start()

                # Monitor the process
                start_time = time.time()
                last_check = start_time
                
                while process.poll() is None and running_outputs.get(key, False):
                    time.sleep(1)
                    current_time = time.time()
                    
                    # Check if process has been running too long without being restarted
                    # (this helps catch hung processes)
                    if current_time - start_time > PROCESS_TIMEOUT:
                        log(f"[{name}] Process running for {PROCESS_TIMEOUT}s, still active")
                        start_time = current_time  # Reset timer
                    
                    # If we've been running for more than 30 seconds, consider it successful
                    if current_time - last_check > 30 and not started_successfully:
                        started_successfully = True
                        successful_vf = vf_graph  # Remember this filter works
                        consecutive_failures = 0
                        current_retry_delay = RETRY_DELAY  # Reset retry delay
                        log(f"[{name}] Output stable, using filter: {vf_graph[:50]}...")

                        # Check if we need to send a restoration email
                        with lock:
                            alert_state = output_alert_states.get(key)
                            if alert_state == 'notified_down':
                                # Send restoration email
                                restoration_msg = f"Output '{name}' has been restored and is now running normally."
                                send_email(f"OBS Output RESTORED: {name}", restoration_msg)
                                log(f"[{name}] Sent restoration notification")
                                output_alert_states[key] = 'restored'
                            # Clear failure tracking
                            output_first_failure_time.pop(key, None)

                # Process exited - wait a bit for output threads to finish
                time.sleep(0.5)
                
                # Try to get any remaining output
                try:
                    stdout_remaining, stderr_remaining = process.communicate(timeout=2)
                    if stdout_remaining:
                        for line in stdout_remaining.decode(errors='ignore').split('\n'):
                            if line.strip():
                                stdout_lines.append(line.strip())
                    if stderr_remaining:
                        for line in stderr_remaining.decode(errors='ignore').split('\n'):
                            if line.strip():
                                stderr_lines.append(line.strip())
                except subprocess.TimeoutExpired:
                    log(f"[{name}] Timeout getting final output")
                except Exception as e:
                    log(f"[{name}] Error getting final output: {e}")

                # Check stderr for specific errors
                stderr_text = '\n'.join(stderr_lines)

                # Filter not available - try next candidate
                if ("No such filter: 'tinterlace'" in stderr_text) or \
                   ("No such filter: 'interlace'" in stderr_text) or \
                   ("Error initializing filter" in stderr_text and "filter" in stderr_text.lower()):
                    log(f"[{name}] VF '{vf_graph[:50]}...' not available; trying next interlace method...")
                    with lock:
                        output_procs.pop(key, None)
                    continue

                # Check for invalid frame dimensions - wait longer before retrying
                invalid_frames = stderr_text.count("Invalid frame dimensions 0x0")
                if invalid_frames > 10:
                    log(f"[{name}] Stream has invalid frame dimensions ({invalid_frames} errors). Stream may be unstable.")
                    consecutive_failures += 1

                # Log specific error conditions
                if "bind failed" in stderr_text.lower() or "address already in use" in stderr_text.lower():
                    log(f"[{name}] UDP bind failed on :{cfg['udp_port']}. "
                        f"Tip: pkill -f 'udp://@0.0.0.0:{cfg['udp_port']}'")
                    consecutive_failures += 1

                if "Unsupported audio codec" in stderr_text:
                    log(f"[{name}] SDI muxer rejected audio codec. Try pcm_s24be or pcm_s32le.")
                    consecutive_failures += 1

                if "Could not write header" in stderr_text or "Error opening output file" in stderr_text:
                    log(f"[{name}] SDI header/open error.")
                    consecutive_failures += 1

                if "Connection refused" in stderr_text or "Network is unreachable" in stderr_text:
                    log(f"[{name}] Network error detected")
                    consecutive_failures += 1

                # Process exited, will retry
                with lock:
                    output_procs.pop(key, None)

                # If this filter ran for a while, break to retry it
                if started_successfully:
                    break
                    
            except Exception as e:
                log(f"Output error for {name}: {e}")
                with lock:
                    proc = output_procs.pop(key, None)
                if proc and proc.poll() is None:
                    try:
                        proc.terminate()
                        proc.wait(timeout=2)
                    except:
                        try:
                            proc.kill()
                        except:
                            pass
                consecutive_failures += 1

        # Track failure time and check if we should alert
        if consecutive_failures > 0:
            current_time = time.time()

            # Initialize failure tracking if this is the first failure
            with lock:
                if key not in output_first_failure_time:
                    output_first_failure_time[key] = current_time
                    log(f"[{name}] Started tracking failures")

                first_failure = output_first_failure_time[key]
                downtime_seconds = current_time - first_failure
                alert_state = output_alert_states.get(key)

                # Send alert only once after 3 minutes of being down
                if downtime_seconds >= ALERT_THRESHOLD and alert_state != 'notified_down':
                    downtime_minutes = int(downtime_seconds / 60)
                    msg = (f"Output '{name}' has been down for {downtime_minutes} minutes.\n\n"
                           f"Consecutive failures: {consecutive_failures}\n"
                           f"Last attempt failed with errors. Check logs for details.")
                    send_email(f"OBS Output DOWN: {name}", msg)
                    log(f"[{name}] Sent downtime notification ({downtime_minutes} minutes)")
                    output_alert_states[key] = 'notified_down'

            # Exponential backoff for retries
            current_retry_delay = min(current_retry_delay * 1.5, MAX_RETRY_DELAY)
        else:
            # Reset retry delay if no failures
            current_retry_delay = RETRY_DELAY

        if running_outputs.get(key, False):
            log(f"Output {name} exited. Retrying in {int(current_retry_delay)}s...")
            time.sleep(current_retry_delay)

def stop_output(key):
    with lock:
        running_outputs[key] = False
        proc = output_procs.pop(key, None)

    if proc and proc.poll() is None:
        try:
            log(f"Terminating process for {key}")
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                log(f"Force killing process for {key}")
                proc.kill()
                try:
                    proc.wait(timeout=2)
                except:
                    pass
        except Exception as e:
            log(f"Error terminating process for {key}: {e}")

    t = output_threads.get(key)
    if t and t.is_alive():
        t.join(timeout=3)

    with lock:
        output_threads.pop(key, None)
        output_configs.pop(key, None)
        # Clear alert tracking when stopping output
        output_alert_states.pop(key, None)
        output_first_failure_time.pop(key, None)

    log(f"Stopped output: {key}")

def cleanup_orphaned_processes():
    """Kill any orphaned ffmpeg processes from previous runs"""
    try:
        log("Checking for orphaned ffmpeg processes...")

        # Get current config to know which ports/devices we manage
        outputs = load_config()
        if not outputs:
            log("No outputs configured, skipping process cleanup")
            return

        killed_count = 0

        # Kill ffmpeg processes using our specific binary and UDP ports
        for cfg in outputs:
            udp_port = cfg.get('udp_port')
            serial = cfg.get('serial')
            output_port = cfg.get('output')

            if not all([udp_port, serial, output_port]):
                continue

            # Pattern matches our specific ffmpeg with this UDP port
            udp_pattern = f"udp://@0.0.0.0:{udp_port}"

            try:
                # Find processes matching our ffmpeg binary and this UDP port
                result = subprocess.run(
                    ['pgrep', '-f', f'{FFMPEG_PATH}.*{udp_pattern}'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )

                if result.returncode == 0 and result.stdout.strip():
                    pids = result.stdout.strip().split('\n')
                    for pid in pids:
                        if pid:
                            try:
                                # Verify it's actually our process before killing
                                cmdline_result = subprocess.run(
                                    ['ps', '-p', pid, '-o', 'cmd='],
                                    capture_output=True,
                                    text=True,
                                    timeout=2
                                )

                                if cmdline_result.returncode == 0:
                                    cmdline = cmdline_result.stdout.strip()
                                    # Double-check it's our ffmpeg and has dektec output
                                    if FFMPEG_PATH in cmdline and 'dektec' in cmdline:
                                        log(f"Killing orphaned ffmpeg process {pid} (UDP port {udp_port})")
                                        subprocess.run(['kill', '-9', pid], timeout=2)
                                        killed_count += 1
                            except Exception as e:
                                log(f"Error checking/killing PID {pid}: {e}")

            except subprocess.TimeoutExpired:
                log(f"Timeout searching for processes on UDP port {udp_port}")
            except Exception as e:
                log(f"Error searching for processes on UDP port {udp_port}: {e}")

        if killed_count > 0:
            log(f"Cleaned up {killed_count} orphaned ffmpeg process(es)")
            time.sleep(2)  # Give system time to release resources
        else:
            log("No orphaned processes found")

    except Exception as e:
        log(f"Error during process cleanup: {e}")

def reload_config_loop():
    last_hash = ""
    while True:
        try:
            new_cfg = load_config()
            new_hash = hash_config(new_cfg)

            if new_hash != last_hash:
                log("Config changed. Checking outputs...")
                new_keys = {output_key(c): c for c in new_cfg}
                
                with lock:
                    current_keys = set(output_configs.keys())
                
                updated_keys = set(new_keys.keys())

                to_stop = current_keys - updated_keys
                to_start = updated_keys - current_keys
                to_restart = [k for k in updated_keys & current_keys 
                             if new_keys[k] != output_configs.get(k)]

                for k in to_stop:
                    log(f"Stopping removed output: {k}")
                    stop_output(k)
                    
                for k in to_restart:
                    log(f"Restarting modified output: {k}")
                    stop_output(k)
                    time.sleep(1)
                    
                for k in to_start | set(to_restart):
                    cfg = new_keys[k]
                    with lock:
                        output_configs[k] = cfg
                        running_outputs[k] = True
                    
                    t = threading.Thread(target=start_output, args=(cfg,), daemon=True)
                    with lock:
                        output_threads[k] = t
                    t.start()
                    log(f"Started output thread: {k}")

                last_hash = new_hash
        except Exception as e:
            log(f"Error in config reload loop: {e}")

        time.sleep(30)  # Check config every 30 seconds

def main():
    log("Starting OBS to Dektec SDI Output Service")
    log(f"Using FFmpeg: {FFMPEG_PATH}")
    log(f"Config file: {CONFIG_FILE}")
    log(f"Email alerts: {'Enabled' if EMAIL_ENABLED else 'Disabled'}")

    # Clean up any orphaned ffmpeg processes from previous runs
    cleanup_orphaned_processes()

    try:
        config_thread = threading.Thread(target=reload_config_loop, daemon=True)
        config_thread.start()
        log("Config monitor started")
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        log("Shutting down...")
        with lock:
            keys_to_stop = list(running_outputs.keys())
        for key in keys_to_stop:
            stop_output(key)
        log("Shutdown complete")
        
    except Exception as e:
        log(f"Fatal error in main loop: {e}")
        msg = f"OBS Output Service crashed: {e}"
        send_email("OBS Output Service CRASH", msg)
        raise

if __name__ == "__main__":
    main()
