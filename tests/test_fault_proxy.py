import socket
import threading

from faults.proxy import FaultProxy


def test_fault_proxy_forwards_and_records_finite_cut() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = int(server.getsockname()[1])
    received: list[bytes] = []

    def serve() -> None:
        connection, _ = server.accept()
        with connection:
            data = connection.recv(32)
            received.append(data)
            connection.sendall(b"ack:" + data)
            connection.recv(64)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    proxy = FaultProxy("127.0.0.1", port, seed=7)
    proxy.start()
    client = socket.create_connection(proxy.address, timeout=5)
    try:
        assert proxy.wait_connected()
        client.sendall(b"hello")
        assert client.recv(32) == b"ack:hello"
        proxy.inject("client_to_target", b"ignored-after-observation")
        proxy.cut()
        assert any(event.name == "forwarded" for event in proxy.events())
        assert any(event.name == "injected" for event in proxy.events())
        assert any(event.name == "cut_observed" for event in proxy.events())
    finally:
        client.close()
        proxy.close()
        server.close()
        thread.join(timeout=5)
    assert received == [b"hello"]
