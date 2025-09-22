"""Tests for the Status class."""

from nats.client.message import Status


def test_status_creation():
    """Test creating Status objects."""
    # Test with code only
    status = Status(code="200")
    assert status.code == "200"
    assert status.description is None

    # Test with code and description
    status = Status(code="503", description="No Responders")
    assert status.code == "503"
    assert status.description == "No Responders"


def test_status_is_error():
    """Test Status.is_error property."""
    # 200 is not an error
    status_ok = Status(code="200")
    assert status_ok.is_error is False

    # 503 is an error
    status_error = Status(code="503", description="No Responders")
    assert status_error.is_error is True

    # 400 is an error
    status_bad_request = Status(code="400", description="Bad Request")
    assert status_bad_request.is_error is True

    # 500 is an error
    status_server_error = Status(
        code="500", description="Internal Server Error"
    )
    assert status_server_error.is_error is True


def test_status_string_representation():
    """Test Status string conversion."""
    # With description
    status = Status(code="503", description="No Responders")
    assert str(status) == "503: No Responders"

    # Without description
    status = Status(code="200")
    assert str(status) == "200"

    # Empty description should be treated as None
    status = Status(code="400", description="")
    assert str(status) == "400"


def test_status_equality():
    """Test Status equality comparison."""
    status1 = Status(code="503", description="No Responders")
    status2 = Status(code="503", description="No Responders")
    status3 = Status(code="503", description="Service Unavailable")
    status4 = Status(code="400", description="No Responders")

    # Same code and description should be equal
    assert status1 == status2

    # Different description should not be equal
    assert status1 != status3

    # Different code should not be equal
    assert status1 != status4

    # Should not be equal to non-Status objects
    assert status1 != "503: No Responders"
    assert status1 != 503


def test_status_common_codes():
    """Test common status codes."""
    # Success
    success = Status(code="200", description="OK")
    assert success.is_error is False
    assert str(success) == "200: OK"

    # Bad Request
    bad_request = Status(code="400", description="Bad Request")
    assert bad_request.is_error is True
    assert str(bad_request) == "400: Bad Request"

    # No Responders
    no_responders = Status(code="503", description="No Responders")
    assert no_responders.is_error is True
    assert str(no_responders) == "503: No Responders"

    # Server Error
    server_error = Status(code="500", description="Internal Server Error")
    assert server_error.is_error is True
    assert str(server_error) == "500: Internal Server Error"
