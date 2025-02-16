import logging
from pathlib import Path
import sys
from datetime import datetime
import os
from colorama import Fore, Style, init

# Initialize colorama for Windows support
init(autoreset=True)

# Create custom logger formatter with colors
class ColoredFormatter(logging.Formatter):
    """Custom formatter with colored output"""
    
    COLOR_CODES = {
        'DEBUG': Fore.WHITE,
        'INFO': Fore.WHITE,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT,
        
        # Custom colors for specific messages
        'URL': Fore.CYAN,
        'SUCCESS': Fore.GREEN,
        'KEYWORD': Fore.GREEN + Style.BRIGHT
    }

    def format(self, record):
        # Add colors based on message content
        if 'http' in str(record.msg) or 'www.' in str(record.msg):
            color = self.COLOR_CODES['URL']
        elif 'successfully' in str(record.msg).lower() or 'success' in str(record.msg).lower():
            color = self.COLOR_CODES['SUCCESS']
        elif 'keyword' in str(record.msg).lower():
            color = self.COLOR_CODES['KEYWORD']
        else:
            # Default colors based on log level
            color = self.COLOR_CODES.get(record.levelname, Fore.WHITE)

        # Add color to the message
        record.msg = f"{color}{record.msg}{Style.RESET_ALL}"
        
        # Use parent class formatting
        return super().format(record)

# Base directories
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / 'good_output'
LOG_DIR = OUTPUT_DIR / 'logs'

# Create directories
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Basic configuration
CONFIG = {
    'VERSION': '1.0.0',
    'USER': os.getenv('COMPUTERNAME', 'default_user'),
    'DEBUG': False,
    'MAX_RETRIES': 3,
    'TIMEOUT': 30,
    'OUTPUT_DIR': str(OUTPUT_DIR),
    'DB_PATH': str(OUTPUT_DIR / 'seo_data.db')  # Add this line
}

# Set up console logging with colors
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
colored_formatter = ColoredFormatter(
    '%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler.setFormatter(colored_formatter)

# Set up file logging (without colors)
file_handler = logging.FileHandler(LOG_DIR / 'debug.log', encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_format = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(file_format)

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

def get_logger(name):
    return logging.getLogger(name)