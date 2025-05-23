SNAP_VAR_CURRENT_PATH = "/var/snap/cassandra/current"
SNAP_CURRENT_PATH = "/snap/cassandra/current"

SNAP_CONF_PATH = f"{SNAP_VAR_CURRENT_PATH}/etc"

CAS_CONF_PATH = f"{SNAP_CONF_PATH}/cassandra"

CAS_CONF_FILE = f"{CAS_CONF_PATH}/cassandra.yaml"
CAS_ENV_CONF_FILE = f"{CAS_CONF_PATH}/cassandra-env.sh"

MGMT_API_DIR = f"{SNAP_CURRENT_PATH}/opt/mgmt-api"

PEER_RELATION = "cassandra-peers"
