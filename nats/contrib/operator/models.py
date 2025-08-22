from typing import List, Optional

from nats.contrib.claims.generic import GenericFields
from nats.contrib.flatten_model import FlatteningModel
from nats.contrib.types import Types


class Operator(FlatteningModel):
    # Slice of other operator NKeys that can be used to sign on behalf of the main
    # operator identity.
    signing_keys: Optional[List[str]]  # `json:"signing_keys,omitempty"`
    # AccountServerURL is a partial URL like "https://host.domain.org:<port>/jwt/v1"
    # tools will use the prefix and build queries by appending /accounts/<account_id>
    # or /operator to the path provided. Note this assumes that the account server
    # can handle requests in a nats-account-server compatible way. See
    # https://github.com/nats-io/nats-account-server.
    account_server_url: Optional[str]  # `json:"account_server_url,omitempty"`
    # A list of NATS urls (tls://host:port) where tools can connect to the server
    # using proper credentials.
    operator_service_urls: Optional[
        List[str]]  # `json:"operator_service_urls,omitempty"`
    # Identity of the system account
    system_account: Optional[str]  # `json:"system_account,omitempty"`
    # Min Server version
    assert_server_version: Optional[
        str]  # `json:"assert_server_version,omitempty"`
    # Signing of subordinate objects will require signing keys
    strict_signing_key_usage: Optional[
        bool]  # `json:"strict_signing_key_usage,omitempty"`

    generic_fields: GenericFields

    def __init__(
        self,
        signing_keys: Optional[List[str]] = None,
        account_server_url: Optional[str] = None,
        operator_service_urls: Optional[List[str]] = None,
        system_account: Optional[str] = None,
        assert_server_version: Optional[str] = None,
        strict_signing_key_usage: Optional[bool] = None,
        generic_fields: Optional[GenericFields] = None
    ):

        self.signing_keys: Optional[List[str]] = signing_keys
        self.account_server_url: Optional[str] = account_server_url
        self.operator_service_urls: Optional[List[str]] = operator_service_urls
        self.system_account: Optional[str] = system_account
        self.assert_server_version: Optional[str] = assert_server_version
        self.strict_signing_key_usage: Optional[bool
                                                ] = strict_signing_key_usage
        self.generic_fields: GenericFields = generic_fields if generic_fields else GenericFields(
            type=Types.Operator, version=2
        )
