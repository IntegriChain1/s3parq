import pytest
import os
from moto.server import ThreadedMotoServer

@pytest.fixture
async def mock_boto():
    server = ThreadedMotoServer(port=0)

    server.start()
    port = server._server.socket.getsockname()[1]
    os.environ["AWS_ENDPOINT_URL"] = f"http://127.0.0.1:{port}"

    yield

    del os.environ["AWS_ENDPOINT_URL"]
    server.stop()

def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run tests marked slow"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run tests marked 'slow'")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
