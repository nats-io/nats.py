from typing import Dict, List, TypedDict


class ServerInfos(TypedDict, total=False):
    server_id: str
    server_name: str
    version: str
    go: str
    git_commit: str
    host: str
    port: int
    max_payload: int
    proto: int
    client_id: int
    client_ip: str
    auth_required: bool
    tls_required: bool
    tls_verify: bool
    connect_urls: List[str]
    ldm: bool
    jetstream: bool
    headers: Dict[str, str]
    nonce: str


class ClientStats(TypedDict):
    in_msgs: int
    out_msgs: int
    in_bytes: int
    out_bytes: int
    reconnects: int
    errors_received: int
