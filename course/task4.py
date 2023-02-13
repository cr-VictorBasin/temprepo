import requests
from elasticsearch import Elasticsearch

# Connect to the Elasticsearch cluster
#es = Elasticsearch(["http://localhost:9200"])
es = Elasticsearch(
    "https://localhost:9200",
    verify_certs=False,
    basic_auth=("cruser", "U87P^wK@ev")
)

es.health()

# Get the name of the node you want to remove
node_name = "elastic-cluster-vico-test1-instance-3.eng.cybereason.net"

# Drain the node before removing it from the cluster
es.cluster.reroute(commands=[{"cancel": {"index": "*", "shard": "*", "node": node_name}}])

# Remove the node from the cluster
es.nodes.hot_threads(node_id=node_name)

# Wait for the node to be removed
es.cluster.health(wait_for_status="green")



cluster_instance_groups_dict(task_id, project_id, cluster_name, stack=None):


