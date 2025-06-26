import logging
from humiologging.handlers import HumioJSONHandler

# Configure your Humio ingest token and URL
HUMIO_URL = "YOUR_HUMIO_URL"
HUMIO_INGEST_TOKEN = "YOUR_HUMIO_INGEST_TOKEN"

# Create a logger instance
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a HumioJSONHandler
humio_handler = HumioJSONHandler(
    url=HUMIO_URL,
    ingest_token=HUMIO_INGEST_TOKEN,
    # Optional: configure other parameters like tags, timeout, etc.
)

# Add the handler to the logger
logger.addHandler(humio_handler)

# Log messages
logger.info("This is an informational message from Python.")
logger.warning("A warning occurred in the application.")
try:
    1 / 0
except ZeroDivisionError:
    logger.error("An error occurred: division by zero.", exc_info=True)