#!/usr/bin/env python3
"""
MulticastToDektec.py - Robust multicast to Dektec output with comprehensive failsafes

This script receives multicast streams and outputs to Dektec devices with:
- Automatic restart on any failure
- Process isolation (only kills associated processes)
- Health monitoring and recovery
- Email alerting for extended outages
- Config hot-reload without affecting other outputs
"""

import subprocess
import time
import threading
import json
import os
import signal
import smtplib
import hashlib
from email.mime.text import MIMEText
from datetime import datetime
from collections import deque

# Optional dependency for enhanced process management
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    print("Warning: psutil not available - using basic process management")

CONFIG_FILE = "OBSOutputConfig.json"
EMAIL_ENABLED = True

# Email settings
SMTP_SERVER = "smtp-legacy.office365.com"
SMTP_PORT = 587
EMAIL_USERNAME = "mib@lillyhubtv.com"
EMAIL_PASSWORD = "N0t1fy!@!"
EMAIL_FROM = "mib@lillyhubtv.com"
EMAIL_TO = ["drogers@lillybroadcasting.com"]

# Global state tracking
output_threads = {}
output_configs = {}
running_outputs = {}
output_procs = {}  # Track actual process objects
output_pids = {}  # Track PIDs for cleanup
output_alert_states = {}  # Track alert state
output_first_failure_time = {}  # Track when failures started
output_last_success_time = {}  # Track last successful run

lock = threading.Lock()

# Configuration
FFMPEG_PATH = "/home/lilly/ffmpeg/ffmpeg"
PROCESS_HEALTH_CHECK_INTERVAL = 10  # Check process health every 10 seconds
RESTART_DELAY = 5  # Initial seconds between restart attempts
MAX_RESTART_DELAY = 60  # Maximum retry delay
ALERT_THRESHOLD = 180  # Alert after 3 minutes down
MIN_STABLE_TIME = 30  # Consider stable after 30 seconds
LOG_BUFFER_SIZE = 100  # Keep last 100 log lines per output

def get_log_filename():
    """Generate daily log filename"""
    return f"/home/lilly/Logs/MulticastToDektec-{datetime.now().strftime('%Y-%m-%d')}.log"

def log(message):
    """Thread-safe logging to file and console"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    try:
        with open(get_log_filename(), "a") as f:
            f.write(full_message + "\n")
    except Exception as e:
        print(f"Failed to write to log file: {e}")

def send_email(subject, body):
    """Send email alert"""
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
    """Load output configuration from JSON file"""
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
    """Generate hash of config for change detection"""
    return hashlib.md5(json.dumps(outputs, sort_keys=True).encode()).hexdigest()

def output_key(cfg):
    """Generate unique key for output configuration"""
    return f"{cfg['udp_port']}:{cfg['serial']}:{cfg['output']}:{cfg.get('name', '')}"

def kill_process_tree(pid):
    """Kill a process and all its children"""
    if HAS_PSUTIL:
        # Enhanced process management with psutil
        try:
            parent = psutil.Process(pid)
            children = parent.children(recursive=True)

            # Terminate children first
            for child in children:
                try:
                    log(f"Terminating child process {child.pid}")
                    child.terminate()
                except psutil.NoSuchProcess:
                    pass

            # Terminate parent
            try:
                parent.terminate()
            except psutil.NoSuchProcess:
                pass

            # Wait for graceful termination
            gone, alive = psutil.wait_procs(children + [parent], timeout=3)

            # Force kill any remaining
            for proc in alive:
                try:
                    log(f"Force killing process {proc.pid}")
                    proc.kill()
                except psutil.NoSuchProcess:
                    pass

        except psutil.NoSuchProcess:
            log(f"Process {pid} already terminated")
        except Exception as e:
            log(f"Error killing process tree {pid}: {e}")
    else:
        # Fallback: use process group kill
        try:
            # Kill entire process group
            os.killpg(pid, signal.SIGTERM)
            time.sleep(1)

            # Check if still exists and force kill
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass  # Already dead

        except ProcessLookupError:
            log(f"Process {pid} already terminated")
        except Exception as e:
            log(f"Error killing process group {pid}: {e}")

def is_process_healthy(proc, pid):
    """Check if a process is still healthy and running"""
    try:
        if proc.poll() is not None:
            return False, "Process exited"

        if HAS_PSUTIL:
            # Enhanced health check with psutil
            # Check if process still exists
            if not psutil.pid_exists(pid):
                return False, "PID no longer exists"

            # Check if it's a zombie
            try:
                p = psutil.Process(pid)
                if p.status() == psutil.STATUS_ZOMBIE:
                    return False, "Process is zombie"
            except psutil.NoSuchProcess:
                return False, "Process disappeared"
        else:
            # Basic health check - just verify PID exists
            try:
                os.kill(pid, 0)  # Signal 0 doesn't kill, just checks existence
            except OSError:
                return False, "PID no longer exists"

        return True, "Healthy"
    except Exception as e:
        return False, f"Health check error: {e}"

def build_ffmpeg_command(cfg):
    """Build FFmpeg command for multicast to Dektec output"""
    name = cfg.get("name", f"UDP{cfg['udp_port']}->DT{cfg['serial']}:{cfg['output']}")

    # Multicast input with socket reuse for clean restarts
    multicast_input = (
        f"udp://@0.0.0.0:{cfg['udp_port']}"
        f"?pkt_size=1316&buffer_size=10000000&fifo_size=1000000"
        f"&overrun_nonfatal=1&reuse=1"
    )

    # Dektec output device
    dektec_output = f"{cfg['serial']}:{cfg['output']}"

    # Get format and pixel format from config
    output_format = cfg.get("format", "1080i59.94")
    pixel_format = cfg.get("pixel_format", "yuv422p10le")

    # Build video filter chain based on format
    if "1080i" in output_format:
        # For interlaced output, try multiple filter options
        vf_options = [
            # Option 1: tinterlace (requires GPL build)
            "scale=1920:1080:flags=bicubic,setsar=1,format=yuv422p10le,"
            "tinterlace=mode=interlacex2:flags=vlpf,fieldorder=tff,format=uyvy422",

            # Option 2: interlace filter
            "scale=1920:1080:flags=bicubic,setsar=1,format=yuv422p10le,"
            "interlace=scan=tff:lowpass=1,fieldorder=tff,format=uyvy422",

            # Option 3: Just set field order (fallback)
            "scale=1920:1080:flags=bicubic,setsar=1,fieldorder=tff,format=uyvy422"
        ]
    elif "1080p" in output_format:
        vf_options = [
            "scale=1920:1080:flags=bicubic,setsar=1,format=uyvy422"
        ]
    elif "720p" in output_format:
        vf_options = [
            "scale=1280:720:flags=bicubic,setsar=1,format=uyvy422"
        ]
    else:
        # Default to 1080i
        vf_options = [
            "scale=1920:1080:flags=bicubic,setsar=1,fieldorder=tff,format=uyvy422"
        ]

    # Determine frame rate
    if "59.94" in output_format or "60" in output_format:
        frame_rate = "60000/1001"
    elif "29.97" in output_format or "30" in output_format:
        frame_rate = "30000/1001"
    elif "25" in output_format:
        frame_rate = "25"
    elif "24" in output_format:
        frame_rate = "24000/1001"
    else:
        frame_rate = "30000/1001"  # Default

    commands = []
    for vf_graph in vf_options:
        cmd = [
            FFMPEG_PATH,
            "-hide_banner", "-nostats", "-loglevel", "level+info",

            # Input settings
            "-thread_queue_size", "2048",
            "-probesize", "5M",
            "-analyzeduration", "5M",
            "-i", multicast_input,

            # Stream mapping
            "-map", "0:v:0",
            "-map", "0:a:0?",  # Optional audio

            # Video filter
            "-vf", vf_graph,

            # Video encoding
            "-r", frame_rate,
            "-c:v", "wrapped_avframe",

            # Color settings
            "-colorspace", "bt709",
            "-color_primaries", "bt709",
            "-color_trc", "bt709",
            "-color_range", "tv",

            # Audio settings
            "-ar", "48000",
            "-ac", "2",
            "-sample_fmt", "s32",
            "-c:a", "pcm_s24le",

            # Output
            "-f", "dektec",
            dektec_output
        ]
        commands.append((cmd, vf_graph))

    return commands, name

def monitor_process_output(pipe, log_buffer, process_name, stop_event):
    """Monitor and log process output"""
    try:
        for line in iter(pipe.readline, b''):
            if stop_event.is_set():
                break
            if line:
                decoded = line.decode(errors='ignore').strip()
                if decoded:
                    log_buffer.append(decoded)
                    log(f"[{process_name}] {decoded}")
    except Exception as e:
        if not stop_event.is_set():
            log(f"Error reading output from {process_name}: {e}")

def start_output(cfg):
    """Start and monitor a single output with automatic restart"""
    key = output_key(cfg)
    commands, name = build_ffmpeg_command(cfg)

    consecutive_failures = 0
    current_retry_delay = RESTART_DELAY
    successful_command = None

    log(f"Starting output monitor for: {name}")

    while running_outputs.get(key, False):
        # Try successful command first, then others
        commands_to_try = [successful_command] if successful_command else []
        commands_to_try.extend([(cmd, vf) for cmd, vf in commands if (cmd, vf) != successful_command])

        process_started = False

        for cmd, vf_graph in commands_to_try:
            if not running_outputs.get(key, False):
                break

            try:
                log(f"[{name}] Starting FFmpeg process...")
                log(f"[{name}] Command: {' '.join(cmd)}")

                # Start process with new process group for clean termination
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    bufsize=1,
                    preexec_fn=os.setsid  # Create new process group
                )

                pid = process.pid
                log(f"[{name}] Started with PID {pid}")

                # Track process
                with lock:
                    output_procs[key] = process
                    output_pids[key] = pid

                # Set up output monitoring
                stderr_buffer = deque(maxlen=LOG_BUFFER_SIZE)
                stdout_buffer = deque(maxlen=LOG_BUFFER_SIZE)
                stop_event = threading.Event()

                stderr_thread = threading.Thread(
                    target=monitor_process_output,
                    args=(process.stderr, stderr_buffer, f"{name}-ERR", stop_event),
                    daemon=True
                )
                stdout_thread = threading.Thread(
                    target=monitor_process_output,
                    args=(process.stdout, stdout_buffer, f"{name}-OUT", stop_event),
                    daemon=True
                )

                stderr_thread.start()
                stdout_thread.start()

                # Monitor process health
                start_time = time.time()
                last_health_check = start_time
                is_stable = False

                while running_outputs.get(key, False):
                    current_time = time.time()

                    # Health check
                    if current_time - last_health_check >= PROCESS_HEALTH_CHECK_INTERVAL:
                        healthy, status = is_process_healthy(process, pid)

                        if not healthy:
                            log(f"[{name}] Process unhealthy: {status}")
                            break

                        last_health_check = current_time

                    # Check if process is still running
                    if process.poll() is not None:
                        log(f"[{name}] Process exited with code {process.returncode}")
                        break

                    # Check if stable
                    runtime = current_time - start_time
                    if not is_stable and runtime >= MIN_STABLE_TIME:
                        is_stable = True
                        process_started = True
                        successful_command = (cmd, vf_graph)
                        consecutive_failures = 0
                        current_retry_delay = RESTART_DELAY

                        log(f"[{name}] Process stable after {int(runtime)}s")

                        with lock:
                            output_last_success_time[key] = current_time

                            # Send restoration email if previously down
                            alert_state = output_alert_states.get(key)
                            if alert_state == 'notified_down':
                                msg = f"Output '{name}' has been restored and is running normally."
                                send_email(f"Multicast->Dektec RESTORED: {name}", msg)
                                log(f"[{name}] Sent restoration notification")
                                output_alert_states[key] = 'restored'

                            # Clear failure tracking
                            output_first_failure_time.pop(key, None)

                    time.sleep(1)

                # Process ended - stop monitoring threads
                stop_event.set()

                # Get any remaining output
                try:
                    stdout_data, stderr_data = process.communicate(timeout=2)
                    if stdout_data:
                        for line in stdout_data.decode(errors='ignore').split('\n'):
                            if line.strip():
                                stdout_buffer.append(line.strip())
                    if stderr_data:
                        for line in stderr_data.decode(errors='ignore').split('\n'):
                            if line.strip():
                                stderr_buffer.append(line.strip())
                except subprocess.TimeoutExpired:
                    log(f"[{name}] Timeout getting final output")

                # Analyze errors
                stderr_text = '\n'.join(stderr_buffer)

                # Check for filter errors - try next filter
                if any(err in stderr_text for err in [
                    "No such filter",
                    "Error initializing filter",
                    "Invalid filter"
                ]):
                    log(f"[{name}] Filter '{vf_graph[:50]}...' not available, trying next option")
                    with lock:
                        output_procs.pop(key, None)
                        output_pids.pop(key, None)
                    continue

                # Log specific errors
                if "Could not open Dektec device" in stderr_text:
                    log(f"[{name}] ERROR: Cannot open Dektec device {cfg['serial']}:{cfg['output']}")
                    consecutive_failures += 1

                if "Connection refused" in stderr_text or "Network is unreachable" in stderr_text:
                    log(f"[{name}] ERROR: Network error on UDP port {cfg['udp_port']}")
                    consecutive_failures += 1

                if "Invalid frame dimensions" in stderr_text:
                    invalid_count = stderr_text.count("Invalid frame dimensions")
                    if invalid_count > 10:
                        log(f"[{name}] WARNING: Multiple invalid frames detected ({invalid_count})")

                # Clean up process references
                with lock:
                    output_procs.pop(key, None)
                    output_pids.pop(key, None)

                # If process was stable, break to restart it
                if is_stable:
                    break

            except Exception as e:
                log(f"[{name}] Exception starting process: {e}")
                consecutive_failures += 1

                # Clean up on exception
                with lock:
                    proc = output_procs.pop(key, None)
                    pid = output_pids.pop(key, None)

                if pid:
                    try:
                        kill_process_tree(pid)
                    except:
                        pass

        # Handle failures and alerts
        if consecutive_failures > 0 or not process_started:
            consecutive_failures += 1
            current_time = time.time()

            with lock:
                # Track first failure time
                if key not in output_first_failure_time:
                    output_first_failure_time[key] = current_time
                    log(f"[{name}] Started tracking failures")

                first_failure = output_first_failure_time[key]
                downtime_seconds = current_time - first_failure
                alert_state = output_alert_states.get(key)

                # Send alert after threshold
                if downtime_seconds >= ALERT_THRESHOLD and alert_state != 'notified_down':
                    downtime_minutes = int(downtime_seconds / 60)
                    msg = (
                        f"Output '{name}' has been down for {downtime_minutes} minutes.\n\n"
                        f"Configuration:\n"
                        f"  UDP Port: {cfg['udp_port']}\n"
                        f"  Dektec: {cfg['serial']}:{cfg['output']}\n"
                        f"  Format: {cfg.get('format', 'N/A')}\n\n"
                        f"Consecutive failures: {consecutive_failures}\n"
                        f"Check logs for details."
                    )
                    send_email(f"Multicast->Dektec DOWN: {name}", msg)
                    log(f"[{name}] Sent downtime notification ({downtime_minutes} min)")
                    output_alert_states[key] = 'notified_down'

            # Exponential backoff
            current_retry_delay = min(current_retry_delay * 1.5, MAX_RESTART_DELAY)

        # Wait before retry
        if running_outputs.get(key, False):
            log(f"[{name}] Restarting in {int(current_retry_delay)}s... (failures: {consecutive_failures})")
            time.sleep(current_retry_delay)

    log(f"[{name}] Output monitor stopped")

def stop_output(key):
    """Stop a specific output and clean up all associated processes"""
    log(f"Stopping output: {key}")

    with lock:
        running_outputs[key] = False
        proc = output_procs.pop(key, None)
        pid = output_pids.pop(key, None)

    # Kill process tree if it exists
    if pid:
        log(f"Terminating process tree for PID {pid}")
        kill_process_tree(pid)
    elif proc and proc.poll() is None:
        try:
            pid = proc.pid
            kill_process_tree(pid)
        except:
            pass

    # Wait for thread to finish
    thread = output_threads.get(key)
    if thread and thread.is_alive():
        thread.join(timeout=5)

    # Clean up state
    with lock:
        output_threads.pop(key, None)
        output_configs.pop(key, None)
        output_alert_states.pop(key, None)
        output_first_failure_time.pop(key, None)
        output_last_success_time.pop(key, None)

    log(f"Stopped output: {key}")

def cleanup_orphaned_processes():
    """Kill any orphaned FFmpeg processes from previous runs"""
    try:
        log("Checking for orphaned FFmpeg processes...")

        outputs = load_config()
        if not outputs:
            log("No outputs configured, skipping cleanup")
            return

        killed_count = 0

        if HAS_PSUTIL:
            # Enhanced cleanup with psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] == 'ffmpeg' or 'ffmpeg' in (proc.info['cmdline'] or []):
                        cmdline = ' '.join(proc.info['cmdline'] or [])

                        # Check if it's our FFmpeg and uses Dektec
                        if FFMPEG_PATH in cmdline and 'dektec' in cmdline:
                            # Check if it matches any of our configured ports
                            for cfg in outputs:
                                udp_port = cfg.get('udp_port')
                                if udp_port and f":{udp_port}" in cmdline:
                                    log(f"Killing orphaned FFmpeg process {proc.info['pid']} (UDP port {udp_port})")
                                    kill_process_tree(proc.info['pid'])
                                    killed_count += 1
                                    break
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        else:
            # Fallback: use pgrep/pkill
            for cfg in outputs:
                udp_port = cfg.get('udp_port')
                if not udp_port:
                    continue

                try:
                    # Find processes matching our pattern
                    pattern = f"{FFMPEG_PATH}.*:{udp_port}.*dektec"
                    result = subprocess.run(
                        ['pgrep', '-f', pattern],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )

                    if result.returncode == 0 and result.stdout.strip():
                        pids = result.stdout.strip().split('\n')
                        for pid in pids:
                            if pid:
                                try:
                                    log(f"Killing orphaned FFmpeg process {pid} (UDP port {udp_port})")
                                    os.kill(int(pid), signal.SIGKILL)
                                    killed_count += 1
                                except (ProcessLookupError, ValueError):
                                    pass
                except subprocess.TimeoutExpired:
                    log(f"Timeout searching for processes on UDP port {udp_port}")
                except Exception as e:
                    log(f"Error searching for processes: {e}")

        if killed_count > 0:
            log(f"Cleaned up {killed_count} orphaned process(es)")
            time.sleep(2)
        else:
            log("No orphaned processes found")

    except Exception as e:
        log(f"Error during process cleanup: {e}")

def reload_config_loop():
    """Monitor config file and reload when changed"""
    last_hash = ""

    while True:
        try:
            new_cfg = load_config()
            new_hash = hash_config(new_cfg)

            if new_hash != last_hash:
                log("Config changed, checking outputs...")
                new_keys = {output_key(c): c for c in new_cfg}

                with lock:
                    current_keys = set(output_configs.keys())

                updated_keys = set(new_keys.keys())

                # Determine what changed
                to_stop = current_keys - updated_keys
                to_start = updated_keys - current_keys
                to_restart = [
                    k for k in updated_keys & current_keys
                    if new_keys[k] != output_configs.get(k)
                ]

                # Stop removed outputs
                for k in to_stop:
                    log(f"Stopping removed output: {k}")
                    stop_output(k)

                # Restart modified outputs
                for k in to_restart:
                    log(f"Restarting modified output: {k}")
                    stop_output(k)
                    time.sleep(2)  # Brief pause between stop and start

                # Start new/restarted outputs
                for k in to_start | set(to_restart):
                    cfg = new_keys[k]

                    with lock:
                        output_configs[k] = cfg
                        running_outputs[k] = True

                    # Start monitoring thread
                    thread = threading.Thread(
                        target=start_output,
                        args=(cfg,),
                        daemon=True,
                        name=f"Output-{k}"
                    )

                    with lock:
                        output_threads[k] = thread

                    thread.start()
                    log(f"Started output thread: {k}")

                last_hash = new_hash

        except Exception as e:
            log(f"Error in config reload loop: {e}")

        time.sleep(30)  # Check config every 30 seconds

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    log(f"Received signal {signum}, shutting down...")

    with lock:
        keys_to_stop = list(running_outputs.keys())

    for key in keys_to_stop:
        stop_output(key)

    log("Shutdown complete")
    exit(0)

def main():
    """Main entry point"""
    log("=" * 80)
    log("Starting MulticastToDektec Service")
    log(f"FFmpeg: {FFMPEG_PATH}")
    log(f"Config: {CONFIG_FILE}")
    log(f"Email alerts: {'Enabled' if EMAIL_ENABLED else 'Disabled'}")
    log("=" * 80)

    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Clean up orphaned processes
    cleanup_orphaned_processes()

    try:
        # Start config monitor thread
        config_thread = threading.Thread(
            target=reload_config_loop,
            daemon=True,
            name="ConfigMonitor"
        )
        config_thread.start()
        log("Config monitor started")

        # Main loop - just keep alive
        while True:
            time.sleep(1)

            # Periodic health check of all threads
            with lock:
                for key, thread in list(output_threads.items()):
                    if not thread.is_alive() and running_outputs.get(key, False):
                        log(f"WARNING: Thread for {key} died unexpectedly, restarting...")
                        cfg = output_configs.get(key)
                        if cfg:
                            new_thread = threading.Thread(
                                target=start_output,
                                args=(cfg,),
                                daemon=True,
                                name=f"Output-{key}"
                            )
                            output_threads[key] = new_thread
                            new_thread.start()

    except KeyboardInterrupt:
        log("Keyboard interrupt received")
        signal_handler(signal.SIGINT, None)

    except Exception as e:
        log(f"FATAL ERROR in main loop: {e}")
        import traceback
        log(traceback.format_exc())

        msg = f"MulticastToDektec service crashed:\n\n{e}\n\n{traceback.format_exc()}"
        send_email("MulticastToDektec SERVICE CRASH", msg)
        raise

if __name__ == "__main__":
    main()
