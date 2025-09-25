from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import os

def get_neptune_connection():
    """
    Returns a Gremlin traversal source (g) and closable connection object for Neptune.
    
    Usage:
        g, conn = get_neptune_connection()
        g.V().hasLabel("PartNumber").toList()
        conn.close()
    """
    endpoint = os.getenv("NEPTUNE_ENDPOINT")
    port = os.getenv("NEPTUNE_PORT", 8182)
    url = f"wss://{endpoint}:{port}/gremlin"

    graph = Graph()
    remote_conn = DriverRemoteConnection(url, 'g')  # create closable connection
    g = graph.traversal().withRemote(remote_conn)   # GraphTraversalSource

    # Return traversal source and closable connection separately
    return g, remote_conn
