from enum import Enum


class Prefix(Enum):
    Unknown = -1

    # Seed is the version byte used for encoded NATS Seeds
    Seed = 18 << 3  # Base32-encodes to 'S...'

    # PrefixBytePrivate is the version byte used for encoded NATS Private keys
    Private = 15 << 3  # Base32-encodes to 'P...'

    # PrefixByteOperator is the version byte used for encoded NATS Operators
    Operator = 14 << 3  # Base32-encodes to 'O...'

    # PrefixByteServer is the version byte used for encoded NATS Servers
    Server = 13 << 3  # Base32-encodes to 'N...'

    # PrefixByteCluster is the version byte used for encoded NATS Clusters
    Cluster = 2 << 3  # Base32-encodes to 'C...'

    # PrefixByteAccount is the version byte used for encoded NATS Accounts
    Account = 0  # Base32-encodes to 'A...'

    # PrefixByteUser is the version byte used for encoded NATS Users
    User = 20 << 3  # Base32-encodes to 'U...'

    Curve = 23 << 3  # Base32-encodes to 'X...'


accLookupReqTokens = 6
accLookupReqSubj = "$SYS.REQ.ACCOUNT.{subject}.CLAIMS.LOOKUP"
accPackReqSubj = "$SYS.REQ.CLAIMS.PACK"
accListReqSubj = "$SYS.REQ.CLAIMS.LIST"
accClaimsReqSubj = "$SYS.REQ.CLAIMS.UPDATE"
accDeleteReqSubj = "$SYS.REQ.CLAIMS.DELETE"

connectEventSubj = "$SYS.ACCOUNT.{subject}.CONNECT"
disconnectEventSubj = "$SYS.ACCOUNT.{subject}.DISCONNECT"
accDirectReqSubj = "$SYS.REQ.ACCOUNT.{account_name}.{subject}"
accPingReqSubj = "$SYS.REQ.ACCOUNT.PING.{subject}"  # atm. only used for STATZ and CONNZ import from system account
# kept for backward compatibility when using http resolver
# this overlaps with the names for events but you'd have to have the operator private key in order to succeed.
accUpdateEventSubjOld = "$SYS.ACCOUNT.{subject}.CLAIMS.UPDATE"
accUpdateEventSubjNew = "$SYS.REQ.ACCOUNT.{subject}.CLAIMS.UPDATE"
connsRespSubj = "$SYS._INBOX_.{subject}"
accConnsEventSubjNew = "$SYS.ACCOUNT.{subject}.SERVER.CONNS"
accConnsEventSubjOld = "$SYS.SERVER.ACCOUNT.{subject}.CONNS"  # kept for backward compatibility
lameDuckEventSubj = "$SYS.SERVER.{subject}.LAMEDUCK"
shutdownEventSubj = "$SYS.SERVER.{subject}.SHUTDOWN"
clientKickReqSubj = "$SYS.REQ.SERVER.{subject}.KICK"
clientLDMReqSubj = "$SYS.REQ.SERVER.{subject}.LDM"
authErrorEventSubj = "$SYS.SERVER.{subject}.CLIENT.AUTH.ERR"
authErrorAccountEventSubj = "$SYS.ACCOUNT.CLIENT.AUTH.ERR"
serverStatsSubj = "$SYS.SERVER.{subject}.STATSZ"
serverDirectReqSubj = "$SYS.REQ.SERVER.{server_id}.{subject}"
serverPingReqSubj = "$SYS.REQ.SERVER.PING.{subject}"
serverStatsPingReqSubj = "$SYS.REQ.SERVER.PING"  # use $SYS.REQ.SERVER.PING.STATSZ instead
serverReloadReqSubj = "$SYS.REQ.SERVER.{subject}.RELOAD"  # with server ID
leafNodeConnectEventSubj = "$SYS.ACCOUNT.{subject}.LEAFNODE.CONNECT"  # for internal use only
remoteLatencyEventSubj = "$SYS.LATENCY.M2.{subject}"
inboxRespSubj = "$SYS._INBOX.{subject}.{subject}"

# Used to return information to a user on bound account and user permissions.
userDirectInfoSubj = "$SYS.REQ.USER.INFO"
userDirectReqSubj = "$SYS.REQ.USER.{subject}.INFO"

# FIXME(dlc) - Should account scope, even with wc for now, but later on
# we can then shard as needed.
accNumSubsReqSubj = "$SYS.REQ.ACCOUNT.NSUBS"

# These are for exported debug services. These are local to this server only.
accSubsSubj = "$SYS.DEBUG.SUBSCRIBERS"

shutdownEventTokens = 4
serverSubjectIndex = 2
accUpdateTokensNew = 6
accUpdateTokensOld = 5
accUpdateAccIdxOld = 2

accReqTokens = 5
accReqAccIndex = 3

ocspPeerRejectEventSubj = "$SYS.SERVER.%s.OCSP.PEER.CONN.REJECT"
ocspPeerChainlinkInvalidEventSubj = "$SYS.SERVER.%s.OCSP.PEER.LINK.INVALID"

CurveKeyLen = 32
CurveDecodeLen = 35
CurveNonceLen = 24
