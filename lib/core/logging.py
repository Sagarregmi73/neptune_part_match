import logging
import sys

# Create a logger
logger = logging.getLogger("part_matching_app")
logger.setLevel(logging.DEBUG)  # You can change to INFO in production

# Create console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)

# Create formatter
formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Add formatter to console handler
console_handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(console_handler)

# Optional: disable propagation to root logger
logger.propagate = False
