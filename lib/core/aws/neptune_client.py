# lib/core/aws/neptune_client.py
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from lib.core.logging import logger
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="env/local.env")

NEPTUNE_ENDPOINT = os.getenv("NEPTUNE_ENDPOINT")
NEPTUNE_PORT = int(os.getenv("NEPTUNE_PORT", 8182))

def get_neptune_connection():
    url = f"wss://{NEPTUNE_ENDPOINT}:{NEPTUNE_PORT}/gremlin"
    try:
        graph = Graph()
        connection = DriverRemoteConnection(url, "g")
        g = graph.traversal().withRemote(connection)
        logger.info("Connected to Neptune successfully")
        return g, connection
    except Exception as e:
        logger.error(f"Failed to connect to Neptune: {e}")
        raise
