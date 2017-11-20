import sys
import unittest

from tests.parser_test import *
from tests.client_test import *

if sys.version_info >= (3, 5):
    from tests.client_async_await_test import *

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(ProtocolParserTest))
    test_suite.addTest(unittest.makeSuite(ClientUtilsTest))
    test_suite.addTest(unittest.makeSuite(ClientTest))
    test_suite.addTest(unittest.makeSuite(ClientReconnectTest))
    test_suite.addTest(unittest.makeSuite(ClientAuthTokenTest))
    test_suite.addTest(unittest.makeSuite(ClientTLSTest))
    test_suite.addTest(unittest.makeSuite(ClientTLSReconnectTest))
    test_suite.addTest(unittest.makeSuite(ConnectFailuresTest))

    # Skip tests using async/await syntax unless on Python 3.5
    if sys.version_info >= (3, 5):
        test_suite.addTest(unittest.makeSuite(ClientAsyncAwaitTest))
    runner = unittest.TextTestRunner(stream=sys.stdout)
    result = runner.run(test_suite)
    if not result.wasSuccessful():
        sys.exit(1)
