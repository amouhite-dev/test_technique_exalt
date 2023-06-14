from datetime import datetime
import os
import sys

import pytz
# add directory source in PATH
fileDirectory = "/".join(os.path.realpath(__file__).split("/")[:-2])
print(f"directory source {fileDirectory}")
sys.path.append(fileDirectory)

import logging

class CustomLogger:
    log_file=""
    def __init__(self, log_file):
        self.log_file = log_file
        # Configure logging level and format
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        if not os.path.isfile(log_file):
            directory = os.path.dirname(log_file)
            os.makedirs(directory, exist_ok=True)
            open(log_file, "a")
        # Create a FileHandler to write log messages to a file
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        # Add the file handler to the logger
        self.logger.addHandler(file_handler)

    def info(self, message):
        self.logger.info(message)
        self._write_to_file("INFO", message)

    def warning(self, message):
        self.logger.warning(message)
        self._write_to_file("WARNING", message)

    def error(self, message):
        self.logger.error(message)
        self._write_to_file("ERROR", message)

    def _write_to_file(self, level, message):
        log_entry = f"{level}: {message}\n"
        with open(self.log_file, 'a') as file:
            file.write(log_entry)

current_date_to_log = datetime.now(pytz.timezone(pytz.country_timezones['FR'][0]))
log_file = f"logs/etl_flightsapi_exaltit_{current_date_to_log.strftime('%Y%m%d%H%M%S')}.log"

logging = CustomLogger(log_file)


