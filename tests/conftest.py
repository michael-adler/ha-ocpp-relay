import pytest

try:
    import pytest_socket  # noqa: F401
except ImportError:
    HAVE_PYTEST_SOCKET = False
else:
    HAVE_PYTEST_SOCKET = True

# This suite's CLI/relay tests talk to a real local Mosquitto broker and
# real local relay/snoop servers over TCP. pytest-homeassistant-custom-component
# pulls in pytest-socket and blocks real socket creation for every test in the
# session by default, not just tests using the `hass` fixture, so opt back in
# globally. Guarded on availability since pytest-socket isn't installed
# outside the `ha-test` extra.
if HAVE_PYTEST_SOCKET:

    @pytest.fixture(autouse=True)
    def _allow_real_sockets(socket_enabled):
        # `socket_enabled` only undoes the socket()-construction block.
        # pytest-homeassistant-custom-component also unconditionally restricts
        # socket.socket.connect() to "127.0.0.1" before every test, which still
        # rejects "::1" (IPv6 localhost) and whatever else "localhost" resolves
        # to first on the runner. Reopen it to every host for this test too.
        pytest_socket.socket_allow_hosts(["0.0.0.0/0", "::/0"], allow_unix_socket=True)

else:

    @pytest.fixture(autouse=True)
    def _allow_real_sockets():
        pass
