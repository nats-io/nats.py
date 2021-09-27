from typing import Dict, List, Optional, TypedDict


class ServerInfos(TypedDict):
    server_id: Optional[str]
    server_name: Optional[str]
    version: Optional[str]
    go: Optional[str]
    git_commit: Optional[str]
    host: Optional[str]
    port: Optional[int]
    max_payload: Optional[int]
    proto: Optional[int]
    client_id: Optional[int]
    client_ip: Optional[str]
    auth_required: Optional[bool]
    tls_required: Optional[bool]
    tls_verify: Optional[bool]
    connect_urls: Optional[List[str]]
    ldm: Optional[bool]
    jetstream: Optional[bool]
    headers: Optional[Dict[str, str]]
    nonce: Optional[str]


class ClientStats(TypedDict):
    in_msgs: int
    out_msgs: int
    in_bytes: int
    out_bytes: int
    reconnects: int
    errors_received: int
