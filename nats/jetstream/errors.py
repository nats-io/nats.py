class Error(Exception):
    pass

class JetStreamNotEnabledError(Error):
    pass

class JetStreamNotEnabledForAccountError(Error):
    pass
