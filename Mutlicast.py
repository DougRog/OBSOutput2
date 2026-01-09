import subprocess
import time
import threading
import json
import os
import smtplib
import hashlib
from email.mime.text import MIMEText
from datetime import datetime

CONFIG_FILE = "Streams.json"
EMAIL_ENABLED = True

# Email settings
SMTP_SERVER = "smtp-legacy.office365.com"
SMTP_PORT = 587
EMAIL_USERNAME = "mib@lillyhubtv.com"
EMAIL_PASSWORD = "N0t1fy!@!"
EMAIL_FROM = "mib@lillyhubtv.com"
EMAIL_TO = ["drogers@lillybroadcasting.com"]

stream_threads = {}
stream_configs = {}
running_streams = {}
last_down_time = {}
email_sent_down = {}
email_sent_recovered = {}

lock = threading.Lock()

FFMPEG_PATH = "/home/lilly/ffmpeg/ffmpeg"

def get_log_filename():
    return f"/home/lilly/Logs/Multicast-{datetime.now().strftime('%Y-%m-%d')}.log"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    with open(get_log_filename(), "a") as f:
        f.write(full_message + "\n")

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
            return data.get("streams", [])
    except Exception as e:
        log(f"Error loading config: {e}")
        return []

def hash_config(streams):
    return hashlib.md5(json.dumps(streams, sort_keys=True).encode()).hexdigest()

def stream_key(cfg):
    return f"{cfg['serial']}:{cfg['input']}:{cfg['multicast_ip']}:{cfg['port']}:{cfg.get('name', '')}"

def start_stream(cfg):
    name = cfg.get("name", f"{cfg['serial']}:{cfg['input']}")
    key = stream_key(cfg)
    source = f"{cfg['serial']}:{cfg['input']}"
    mp4_mode = cfg.get("mp4", "disabled") == "enabled"

    while running_streams.get(key, False):
        try:
            command = [
                FFMPEG_PATH,
                "-f", "dektec",
                "-i", source
            ]

            if mp4_mode:
                command += [
                "-c:v", "libx264",
                "-c:s", "mov_text",
                "-map", "0",
                "-f", "mp4"
            ]
            else:
                command += [
                    "-f", "mpegts"
                ]

            command.append(f"udp://{cfg['multicast_ip']}:{cfg['port']}?pkt_size=1316&ttl=1&localaddr=10.1.224.15")

            log(f"Starting stream: {name} -> {cfg['multicast_ip']}:{cfg['port']} (MP4: {mp4_mode})")
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            stdout, stderr = process.communicate()

            if stdout:
                log(f"[{name} STDOUT]: {stdout.decode(errors='ignore')}")
            if stderr:
                err = stderr.decode(errors='ignore')
                log(f"[{name} STDERR]: {err}")
                if "error" in err.lower():
                    log(f"{name} encountered a signal issue.")

            log(f"Stream {name} exited. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            log(f"Stream error for {name}: {e}")
            time.sleep(5)

def stop_stream(key):
    running_streams[key] = False
    t = stream_threads.get(key)
    if t and t.is_alive():
        t.join(timeout=2)
    stream_threads.pop(key, None)
    stream_configs.pop(key, None)
    log(f"Stopped stream: {key}")

def reload_config_loop():
    last_hash = ""
    while True:
        new_cfg = load_config()
        new_hash = hash_config(new_cfg)

        if new_hash != last_hash:
            log("Config changed. Checking streams...")
            new_keys = {stream_key(c): c for c in new_cfg}
            current_keys = set(stream_configs.keys())
            updated_keys = set(new_keys.keys())

            to_stop = current_keys - updated_keys
            to_start = updated_keys - current_keys
            to_restart = [k for k in updated_keys & current_keys if new_keys[k] != stream_configs[k]]

            for k in to_stop:
                stop_stream(k)
            for k in to_restart:
                stop_stream(k)
                time.sleep(1)
            for k in to_start | set(to_restart):
                cfg = new_keys[k]
                stream_configs[k] = cfg
                running_streams[k] = True
                t = threading.Thread(target=start_stream, args=(cfg,), daemon=True)
                stream_threads[k] = t
                t.start()

        last_hash = new_hash
        time.sleep(60)

def main():
    try:
        threading.Thread(target=reload_config_loop, daemon=True).start()
        while True:
            time.sleep(1)
    except Exception as e:
        log(f"Fatal error in main loop: {e}")

main()
