PENDING_SIZE: int = 1024 * 1024
BUFFER_SIZE: int = 32768
RECONNECT_TIME_WAIT: float = 2  # in seconds
MAX_RECONNECT_ATTEMPTS: int = 60
PING_INTERVAL: float = 120  # in seconds
MAX_OUTSTANDING_PINGS: int = 2
MAX_PAYLOAD_SIZE: int = 1048576
MAX_FLUSHER_QUEUE_SIZE: int = 1024
CONNECT_TIMEOUT: float = 2  # in seconds
DRAIN_TIMEOUT: float = 30  # in seconds

# Default Pending Limits of Subscriptions
SUB_PENDING_MSGS_LIMIT: int = 65536
SUB_PENDING_BYTES_LIMIT: int = 65536 * 1024
