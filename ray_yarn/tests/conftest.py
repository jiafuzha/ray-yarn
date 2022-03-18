import pytest
import skein


@pytest.fixture(scope="session")
def skein_client():
    with skein.Client() as client:
        yield client
