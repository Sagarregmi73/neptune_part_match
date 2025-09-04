# lib/core/logging.py
import logging
import sys

# Create logger
logger = logging.getLogger("part_matching_app")
logger.setLevel(logging.DEBUG)  # Change to INFO in production

# Console handler
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)

# Formatter
formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s"
)
ch.setFormatter(formatter)

# Add handler to logger
logger.addHandler(ch)

# Usage example:
# from lib.core.logging import logger
# logger.info("This is an info message")
# logger.error("This is an error message")
