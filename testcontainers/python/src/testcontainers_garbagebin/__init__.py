"""Testcontainers module for the Garbage Bin S3 server."""

from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

PORT = 9000


class GarbageBinContainer(DockerContainer):
    def __init__(
        self,
        image: str = "garbagebin:latest",
        backend: str = "memory",
        init_buckets: list[str] | None = None,
        access_key: str = "test",
        secret_key: str = "test",
        **kwargs,
    ):
        super().__init__(image, **kwargs)
        self._access_key = access_key
        self._secret_key = secret_key
        self.with_exposed_ports(PORT)
        self.with_env("S3_BACKEND", backend)
        self.with_env("S3_ACCESS_KEY", access_key)
        self.with_env("S3_SECRET_KEY", secret_key)
        if init_buckets:
            self.with_env("S3_INIT_BUCKETS", ",".join(init_buckets))
        self.waiting_for(HttpWaitStrategy(PORT, "/health").for_status_code(200))

    def endpoint(self) -> str:
        return f"http://{self.get_container_host_ip()}:{self.get_exposed_port(PORT)}"

    def access_key(self) -> str:
        return self._access_key

    def secret_key(self) -> str:
        return self._secret_key
