"""NATS protocol type definitions.

This module defines TypedDict classes for NATS protocol message types
used in communication between client and server. These types follow
the NATS protocol specification.
"""

from __future__ import annotations

from typing import NotRequired, Required, TypedDict


class ConnectInfo(TypedDict):
    """CONNECT message info.

    Attributes documented at: https://docs.nats.io/reference/reference-protocols/nats-protocol#connect
    """

    verbose: Required[bool]
    """Turns on +OK protocol acknowledgments"""
    pedantic: Required[bool]
    """Turns on additional protocol checks"""
    tls_required: Required[bool]
    """Indicates whether the client requires an SSL connection"""
    lang: Required[str]
    """The implementation language of the client"""
    version: Required[str]
    """The version of the client"""
    auth_token: NotRequired[str]
    """Authentication token (required if auth_required is true)"""
    user: NotRequired[str]
    """Connection username (required if auth_required is true)"""
    pass_: NotRequired[str]
    """Connection password (required if auth_required is true)"""
    name: NotRequired[str]
    """Optional client name"""
    protocol: NotRequired[int]
    """Optional int indicating protocol version"""
    echo: NotRequired[bool]
    """If set to true, the server will not send originating messages"""
    sig: NotRequired[str]
    """Client's JWT signature (required if nonce received)"""
    jwt: NotRequired[str]
    """Client's JWT"""
    no_responders: NotRequired[bool]
    """Enable no responders tracking"""
    headers: NotRequired[bool]
    """Support for headers"""
    nkey: NotRequired[str]
    """User's public NKey"""


class ServerInfo(TypedDict):
    """INFO message from server.

    Attributes documented at: https://docs.nats.io/reference/reference-protocols/nats-protocol#info
    Lame duck mode: https://docs.nats.io/running-a-nats-service/nats_admin/lame_duck_mode
    """

    server_id: Required[str]
    """Server's unique identifier"""
    server_name: Required[str]
    """Server's name"""
    version: Required[str]
    """Version of the NATS server"""
    proto: Required[int]
    """Protocol version"""
    go: Required[str]
    """Version of golang runtime"""
    host: Required[str]
    """IP address of the NATS server host"""
    port: Required[int]
    """Port number the NATS server is configured to listen on"""
    max_payload: Required[int]
    """Maximum allowed payload size"""
    headers: Required[bool]
    """If set, server supports headers"""
    client_id: NotRequired[int]
    """Client ID assigned by the server"""
    auth_required: NotRequired[bool]
    """If this is set, client must authenticate"""
    tls_required: NotRequired[bool]
    """If this is set, client must use TLS"""
    tls_verify: NotRequired[bool]
    """If this is set, client must use TLS with valid cert"""
    tls_available: NotRequired[bool]
    """If this is true, client can provide valid cert during TLS handshake"""
    connect_urls: NotRequired[list[str]]
    """List of server URLs available for client to connect"""
    ws_connect_urls: NotRequired[list[str]]
    """List of websocket server URLs"""
    ldm: NotRequired[bool]
    """If true, server has entered lame duck mode (graceful shutdown in progress)"""
    git_commit: NotRequired[str]
    """Git hash at which the NATS server was built"""
    jetstream: NotRequired[bool]
    """If set, server supports JetStream"""
    ip: NotRequired[str]
    """IP of the server"""
    client_ip: NotRequired[str]
    """IP of the client"""
    nonce: NotRequired[str]
    """Server-side nonce challenge for NKey auth"""
    cluster: NotRequired[str]
    """Name of the cluster this server is part of"""
    domain: NotRequired[str]
    """Domain name this server is part of"""
