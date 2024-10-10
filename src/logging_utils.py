from datetime import datetime

def log_progress(message, log_file, error=False):
    ''' This function logs the message with a timestamp and error flag to the log file. '''
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    log_type = "ERROR" if error else "INFO"
    with open(log_file, "a") as f:
        f.write(f"{timestamp} [{log_type}] : {message}\n")
