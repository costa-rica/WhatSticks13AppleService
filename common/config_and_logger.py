import os
from ws_config import ConfigWorkstation, ConfigDev, ConfigProd
import logging
from logging.handlers import RotatingFileHandler

match os.environ.get('WS_CONFIG_TYPE'):
    case 'dev':
        config = ConfigDev()
        print('- WhatSticks13AppleService/config: Development')
    case 'prod':
        config = ConfigProd()
        print('- WhatSticks13AppleService/config: Production')
    case _:
        config = ConfigWorkstation()
        print('- WhatSticks13AppleService/config: Workstation')

# Setting up Logger
app_name = "WS11AppleService"
formatter = logging.Formatter(f'%(asctime)s - {app_name} - [%(filename)s:%(lineno)d] - %(message)s')

# Initialize a logger
logger_apple = logging.getLogger(__name__)
logger_apple.setLevel(logging.DEBUG)

# Main log file handler
file_handler = RotatingFileHandler(
    os.path.join(config.APPLE_SERVICE_11_ROOT, 'apple_service.log'),
    mode='a',
    maxBytes=5*1024*1024,
    backupCount=2
)
file_handler.setFormatter(formatter)

# Stream handler (for console output)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

# Error log file handler
error_file_handler = RotatingFileHandler(
    os.path.join(config.APPLE_SERVICE_11_ROOT, 'apple_service_errors.log'),
    mode='a',
    maxBytes=5*1024*1024,
    backupCount=2
)
error_file_handler.setFormatter(formatter)
error_file_handler.setLevel(logging.ERROR)

# Adding handlers to the logger
logger_apple.addHandler(file_handler)
logger_apple.addHandler(stream_handler)
logger_apple.addHandler(error_file_handler)

