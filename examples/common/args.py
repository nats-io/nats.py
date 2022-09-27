import argparse


def get_args(description, epilog=""):
    parser = argparse.ArgumentParser(description=description, epilog=epilog)
    parser.add_argument("-s", "--servers", nargs='*',
                        default=["nats://localhost:4222"],
                        help="List of servers to connect. A demo server "
                        "is available at nats://demo.nats.io:4222. "
                        "However, it is very likely that the demo server "
                        "will see traffic from clients other than yours."
                        "To avoid this, start your own locally and pass "
                        "its address. "
                        "This option is default set to nats://localhost:4222 "
                        "which mean that, by default, you should run your "
                        "own server locally"
    )
    return parser.parse_known_args()
