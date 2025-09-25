from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import os

def get_neptune_connection():
    """
    Returns async Gremlin traversal source (g) and closable connection for Neptune.
    """
    endpoint = os.getenv("NEPTUNE_ENDPOINT")
    port = os.getenv("NEPTUNE_PORT", 8182)
    url = f"wss://{endpoint}:{port}/gremlin"

    graph = Graph()
    remote_conn = DriverRemoteConnection(url, 'g')
    g = graph.traversal().withRemote(remote_conn)

    return g, remote_conn
