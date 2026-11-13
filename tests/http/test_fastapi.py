#!/usr/bin/env python
# -- Content-Encoding: UTF-8 --
"""
Tests the FastAPI routers served by the FastAPI HTTP service

:author: Thomas Calmant
"""

import http.client as httplib
import json
import shutil
import socket
import tempfile
import threading
import time
import unittest
from typing import Any, cast
from unittest import mock

try:
    import fastapi
    import websockets.sync.client
    from websockets.exceptions import InvalidStatus
except ImportError:
    raise unittest.SkipTest("fastapi library not available") from None

import pelix.http.fastapi as fastapi_http
from pelix import http, services
from pelix.framework import Bundle, BundleContext, Framework, FrameworkFactory, create_framework
from pelix.http.fastapi import HTTP_FASTAPI_PREFIX, HTTP_FASTAPI_ROUTER, FastApiRouter
from pelix.internals.registry import ServiceRegistration
from pelix.ipopo.constants import (
    IPOPO_CONFIG_FACTORY_NAME,
    IPOPO_CONFIGADMIN_FACTORY_PID,
    IPOPO_INSTANCE_NAME,
    IPopoService,
)
from pelix.ipopo.decorators import ComponentFactory, Property, Provides
from pelix.ipopo.instance import StoredInstance
from pelix.security import (
    FACTORY_HTPASSWD,
    PROP_HTPASSWD_FILE,
    AccessDenied,
    AuthenticationRequired,
    get_current_subject,
)
from tests.http.utils import DEFAULT_HOST, install_ipopo

# ------------------------------------------------------------------------------

__version_info__ = (3, 3, 0)
__version__ = ".".join(str(x) for x in __version_info__)

SERVER_NAME = "fastapi-test-server"

# ------------------------------------------------------------------------------


def make_router(name: str = "items") -> fastapi.APIRouter:
    """
    Prepares a router with a route telling its name and the caller
    """
    router = fastapi.APIRouter()

    @router.get("/hello/{who}")
    def hello(who: str, request: fastapi.Request) -> dict[str, Any]:
        client = request.client
        return {
            "router": name,
            "who": who,
            "subject": get_current_subject().name,
            "client": client.host if client else None,
            "scheme": request.url.scheme,
        }

    @router.post("/echo")
    async def echo(request: fastapi.Request) -> dict[str, int]:
        return {"size": len(await request.body())}

    @router.get("/denied")
    def denied() -> None:
        raise AccessDenied("no")

    @router.get("/login")
    def login() -> None:
        raise AuthenticationRequired("who are you?")

    @router.websocket("/ws")
    async def ws(websocket: fastapi.WebSocket) -> None:
        await websocket.accept()
        message = await websocket.receive_text()
        await websocket.send_text(f"{name}: {message}")
        await websocket.close()

    return router


@ComponentFactory("test-fastapi-router-factory")
@Provides(FastApiRouter)
@Property("_prefix", HTTP_FASTAPI_PREFIX, "/component")
class RouterComponent:
    """
    A router provided by a component
    """

    def __init__(self) -> None:
        self._prefix = "/component"
        self._router = make_router("component")

    def get_router(self) -> fastapi.APIRouter:
        return self._router


class EchoServlet:
    """
    A plain Pelix servlet
    """

    def do_GET(
        self, request: http.AbstractHTTPServletRequest, response: http.AbstractHTTPServletResponse
    ) -> None:
        response.send_content(200, f"servlet:{request.get_sub_path()}", "text/plain")


class _FastApiTestCase(unittest.TestCase):
    """
    Starts a framework with the FastAPI HTTP service
    """

    framework: Framework
    context: BundleContext
    ipopo: IPopoService
    port: int

    def setUp(self) -> None:
        self.framework = create_framework(["pelix.http.fastapi"])
        self.framework.start()
        self.addCleanup(FrameworkFactory.delete_framework)
        self.context = self.framework.get_bundle_context()
        self.ipopo = install_ipopo(self.framework)

    def start_server(self, **properties: Any) -> http.HTTPService:
        """
        Instantiates the HTTP service on a random port
        """
        service = cast(
            http.HTTPService,
            self.ipopo.instantiate(
                http.FACTORY_HTTP_FASTAPI,
                SERVER_NAME,
                {http.HTTP_SERVICE_ADDRESS: DEFAULT_HOST, http.HTTP_SERVICE_PORT: 0, **properties},
            ),
        )
        self.port = service.get_access()[1]
        return service

    def register_router(
        self, router: fastapi.APIRouter, prefix: str | None = "/items", **properties: Any
    ) -> ServiceRegistration[Any]:
        """
        Registers a router service
        """
        if prefix is not None:
            properties[HTTP_FASTAPI_PREFIX] = prefix
        return self.context.register_service(HTTP_FASTAPI_ROUTER, router, properties)

    def request(
        self,
        uri: str,
        method: str = "GET",
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 10,
    ) -> tuple[int, httplib.HTTPMessage, bytes]:
        """
        Sends a request: returns the status, headers and body of the answer
        """
        conn = httplib.HTTPConnection(DEFAULT_HOST, self.port, timeout=timeout)
        try:
            conn.request(method, uri, body, headers or {})
            response = conn.getresponse()
            return response.status, response.headers, response.read()
        finally:
            conn.close()

    def get_json(self, uri: str, **kwargs: Any) -> tuple[int, Any]:
        """
        Sends a request and decodes the JSON answer, if any
        """
        status, _, body = self.request(uri, **kwargs)
        try:
            return status, json.loads(body)
        except ValueError:
            return status, None

    def openapi_paths(self) -> list[str]:
        """
        Returns the paths given by the OpenAPI schema
        """
        status, schema = self.get_json("/openapi.json")
        self.assertEqual(status, 200)
        return sorted(schema["paths"])


class FastApiRoutersTest(_FastApiTestCase):
    """
    Routers registered and unregistered while the service runs
    """

    def test_register_unregister(self) -> None:
        """
        A router is served while its service is registered, and the OpenAPI
        schema follows
        """
        self.start_server()
        self.assertEqual(self.get_json("/items/hello/joe")[0], 404)
        self.assertEqual(self.openapi_paths(), [])

        registration = self.register_router(make_router())
        status, data = self.get_json("/items/hello/joe")
        self.assertEqual(status, 200)
        self.assertEqual((data["router"], data["who"], data["subject"]), ("items", "joe", "anonymous"))
        self.assertIn("/items/hello/{who}", self.openapi_paths())

        registration.unregister()
        status, _, body = self.request("/items/hello/joe")
        self.assertEqual(status, 404)
        # The page of the Pelix service
        self.assertIn(b"Page not found", body)
        self.assertEqual(self.openapi_paths(), [])

    def test_bound_before_start(self) -> None:
        """
        A router registered before the service starts is served
        """
        self.register_router(make_router())
        self.start_server()
        self.assertEqual(self.get_json("/items/hello/joe")[0], 200)

    def test_component(self) -> None:
        """
        A router provided by a component follows its life cycle, and its prefix
        property
        """
        self.start_server()
        self.ipopo.register_factory(self.context, RouterComponent)
        self.ipopo.instantiate("test-fastapi-router-factory", "router")
        self.assertEqual(self.get_json("/component/hello/joe")[1]["router"], "component")

        # Moving the routes
        instance = self.ipopo.get_instance("router")
        instance._prefix = "/moved"
        self.assertEqual(self.get_json("/component/hello/joe")[0], 404)
        self.assertEqual(self.get_json("/moved/hello/joe")[0], 200)
        self.assertIn("/moved/hello/{who}", self.openapi_paths())

        self.ipopo.kill("router")
        self.assertEqual(self.get_json("/moved/hello/joe")[0], 404)
        self.assertEqual(self.openapi_paths(), [])

    def test_ranking(self) -> None:
        """
        On a path served by two routers, the best ranked one answers
        """
        self.start_server()
        low = self.register_router(make_router("low"), **{"service.ranking": -10})
        self.register_router(make_router("high"), **{"service.ranking": 10})
        self.assertEqual(self.get_json("/items/hello/joe")[1]["router"], "high")

        low.set_properties({"service.ranking": 100})
        self.assertEqual(self.get_json("/items/hello/joe")[1]["router"], "low")

    def test_faulty_router(self) -> None:
        """
        A router provider which raises doesn't prevent the others from being served
        """

        class Faulty:
            def get_router(self) -> fastapi.APIRouter:
                raise ValueError("Oops")

        self.start_server()
        with self.assertLogs(SERVER_NAME, "ERROR"):
            self.context.register_service(FastApiRouter, Faulty(), {})
        self.register_router(make_router())
        self.assertEqual(self.get_json("/items/hello/joe")[0], 200)

    def test_servlets_first(self) -> None:
        """
        A servlet handles the paths under its own: the routes get the others
        """
        self.start_server()
        self.register_router(make_router(), prefix="/api")
        registration = self.context.register_service(
            http.HTTP_SERVLET, EchoServlet(), {http.HTTP_SERVLET_PATH: "/api/hello"}
        )
        status, _, body = self.request("/api/hello/joe")
        self.assertEqual((status, body), (200, b"servlet:/joe"))
        self.assertEqual(self.get_json("/api/echo", method="POST", body=b"abc")[1], {"size": 3})

        registration.unregister()
        self.assertEqual(self.get_json("/api/hello/joe")[1]["router"], "items")

    def test_websocket(self) -> None:
        """
        A WebSocket route of a router, through Starlette
        """
        self.start_server()
        self.register_router(make_router())
        with websockets.sync.client.connect(f"ws://{DEFAULT_HOST}:{self.port}/items/ws") as ws:
            ws.send("hello")
            self.assertEqual(ws.recv(5), "items: hello")

        # Unknown route
        with self.assertRaises(InvalidStatus) as ctx:
            websockets.sync.client.connect(f"ws://{DEFAULT_HOST}:{self.port}/items/unknown").close()
        self.assertEqual(ctx.exception.response.status_code, 404)

    def test_application_properties(self) -> None:
        """
        Title, documentation and root path of the application
        """
        self.start_server(
            **{
                fastapi_http.HTTP_FASTAPI_TITLE: "Test API",
                fastapi_http.HTTP_FASTAPI_DOCS_URL: "",
                fastapi_http.HTTP_FASTAPI_ROOT_PATH: "/proxied",
            }
        )
        self.register_router(make_router())
        status, schema = self.get_json("/openapi.json")
        self.assertEqual(status, 200)
        self.assertEqual(schema["info"]["title"], "Test API")
        self.assertEqual(schema["servers"], [{"url": "/proxied"}])
        self.assertEqual(self.request("/docs")[0], 404)
        self.assertEqual(self.get_json("/items/hello/joe")[0], 200)

    def test_address_in_use(self) -> None:
        """
        The socket is bound when the component is validated: an address in use
        makes it fail at once
        """
        with socket.socket() as sock:
            sock.bind((DEFAULT_HOST, 0))
            sock.listen()
            port = sock.getsockname()[1]
            with self.assertLogs(level="ERROR"):
                self.ipopo.instantiate(
                    http.FACTORY_HTTP_FASTAPI,
                    SERVER_NAME,
                    {http.HTTP_SERVICE_ADDRESS: DEFAULT_HOST, http.HTTP_SERVICE_PORT: port},
                )
        details = self.ipopo.get_instance_details(SERVER_NAME)
        self.assertEqual(details["state"], StoredInstance.ERRONEOUS)
        self.assertIn("OSError", str(details["error"]))

    def test_no_openapi(self) -> None:
        """
        No OpenAPI schema, no documentation page
        """
        self.start_server(**{fastapi_http.HTTP_FASTAPI_OPENAPI_URL: ""})
        self.assertEqual(self.request("/openapi.json")[0], 404)
        self.assertEqual(self.request("/docs")[0], 404)


class FastApiChecksTest(_FastApiTestCase):
    """
    The checks of the HTTP service apply to the routes
    """

    def test_host_and_origin(self) -> None:
        """
        The Host and Origin checks protect the routes, the WebSockets included
        """
        self.start_server()
        self.register_router(make_router())
        self.assertEqual(self.request("/items/hello/joe", headers={"Host": "evil.example.com"})[0], 403)
        self.assertEqual(
            self.request("/items/echo", "POST", b"x", {"Origin": "https://evil.example.com"})[0],
            403,
        )
        with self.assertRaises(InvalidStatus) as ctx:
            websockets.sync.client.connect(
                f"ws://{DEFAULT_HOST}:{self.port}/items/ws", origin=cast(Any, "https://evil.example.com")
            ).close()
        self.assertEqual(ctx.exception.response.status_code, 403)

    def test_cors_policy(self) -> None:
        """
        The CORS handler service answers the preflight requests of the routes
        and adds its headers to their responses
        """
        self.framework.install_bundle("pelix.http.cors").start()
        self.ipopo.instantiate(
            http.FACTORY_HTTP_CORS, "cors", {http.HTTP_CORS_ORIGINS: ["https://client.example.com"]}
        )
        self.start_server()
        self.register_router(make_router())
        origin = {"Origin": "https://client.example.com"}

        status, headers, _ = self.request(
            "/items/echo", "OPTIONS", headers={**origin, "Access-Control-Request-Method": "POST"}
        )
        self.assertEqual(status, 204)
        self.assertEqual(headers["Access-Control-Allow-Origin"], "https://client.example.com")

        status, headers, _ = self.request("/items/echo", "POST", b"abc", origin)
        self.assertEqual(status, 200)
        self.assertEqual(headers["Access-Control-Allow-Origin"], "https://client.example.com")

    def test_body_limit(self) -> None:
        """
        The maximum body size of the service applies to the routes, whether the
        size is announced or not
        """
        self.start_server(**{http.HTTP_MAX_BODY_SIZE: 10})
        self.register_router(make_router())
        self.assertEqual(self.get_json("/items/echo", method="POST", body=b"x" * 10)[1], {"size": 10})
        self.assertEqual(self.request("/items/echo", "POST", b"x" * 11)[0], 413)

        # Chunked body
        conn = httplib.HTTPConnection(DEFAULT_HOST, self.port, timeout=10)
        try:
            conn.request("POST", "/items/echo", iter([b"x" * 8, b"x" * 8]), encode_chunked=True)
            self.assertEqual(conn.getresponse().status, 413)
        finally:
            conn.close()

    def test_security_errors(self) -> None:
        """
        A route raising a refusal of pelix.security
        """
        self.start_server()
        self.register_router(make_router())
        self.assertEqual(self.get_json("/items/denied"), (403, {"detail": "Forbidden"}))
        self.assertEqual(self.get_json("/items/login"), (401, {"detail": "Not authenticated"}))

    def test_auth_required(self) -> None:
        """
        A service requiring an authentication protects its routes, and gives
        them the subject of the request
        """
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        users = f"{folder}/.htpasswd"
        with open(users, "w", encoding="utf-8") as file:
            # alice:abc
            file.write("alice:{SHA}qZk+NkcGgWq6PiVxeFDCbJzQ2J0=\n")
        for name in ("pelix.http.auth", "pelix.security.core", "pelix.security.htpasswd"):
            self.framework.install_bundle(name).start()
        self.ipopo.instantiate(FACTORY_HTPASSWD, "htpasswd", {PROP_HTPASSWD_FILE: users})
        self.ipopo.instantiate(http.FACTORY_HTTP_AUTH_BASIC, "basic-auth", {})
        self.start_server(**{http.HTTP_AUTH_REQUIRED: True})
        self.register_router(make_router())

        status, headers, _ = self.request("/items/hello/joe")
        self.assertEqual(status, 401)
        self.assertIn("Basic", headers["WWW-Authenticate"])

        # alice:abc
        status, data = self.get_json("/items/hello/joe", headers={"Authorization": "Basic YWxpY2U6YWJj"})
        self.assertEqual(status, 200)
        self.assertEqual(data["subject"], "alice")

    def test_trusted_proxy(self) -> None:
        """
        The routes see the client told by a trusted proxy
        """
        self.start_server(**{http.HTTP_PROXY_TRUSTED: "127.0.0.1"})
        self.register_router(make_router())
        headers = {"X-Forwarded-For": "203.0.113.7", "X-Forwarded-Proto": "https"}
        data = self.get_json("/items/hello/joe", headers=headers)[1]
        self.assertEqual((data["client"], data["scheme"]), ("203.0.113.7", "https"))

        # Without the headers: the peer
        data = self.get_json("/items/hello/joe")[1]
        self.assertEqual((data["client"], data["scheme"]), ("127.0.0.1", "http"))


class FastApiConcurrencyTest(_FastApiTestCase):
    """
    Routers changing while requests are handled, and stops of the service
    """

    def test_bundle_stop_during_request(self) -> None:
        """
        Stopping the bundle of a router doesn't wait for its requests, which
        end normally, and its routes are gone at once
        """
        self.start_server()
        bundle: Bundle = self.context.install_bundle("tests.http.fastapi_bundle")
        bundle.start()
        module = bundle.get_module()
        module.entered.clear()
        module.release.clear()
        self.addCleanup(module.release.set)

        result: dict[str, Any] = {}
        thread = threading.Thread(target=lambda: result.update(answer=self.get_json("/bundle/slow")))
        thread.start()
        self.assertTrue(module.entered.wait(5))

        start = time.monotonic()
        bundle.stop()
        self.assertLess(time.monotonic() - start, 2, "The stop waited for the request")
        self.assertEqual(self.get_json("/bundle/slow", timeout=2)[0], 404)
        self.assertNotIn("/bundle/slow", self.openapi_paths())

        module.release.set()
        thread.join(5)
        self.assertEqual(result["answer"], (200, {"state": "done"}))

    def test_routers_change_under_load(self) -> None:
        """
        Requests sent while routers come and go are either answered by a route
        or not found, never an error
        """
        self.start_server()
        stop = threading.Event()
        statuses: set[int] = set()
        errors: list[BaseException] = []

        def client() -> None:
            while not stop.is_set():
                try:
                    statuses.add(self.request("/items/hello/joe")[0])
                except BaseException as ex:  # noqa: BLE001
                    errors.append(ex)

        clients = [threading.Thread(target=client) for _ in range(4)]
        for thread in clients:
            thread.start()
        try:
            for _ in range(30):
                registration = self.register_router(make_router())
                time.sleep(0.01)
                registration.unregister()
                time.sleep(0.01)
        finally:
            stop.set()
            for thread in clients:
                thread.join(10)

        self.assertEqual(errors, [])
        self.assertEqual(statuses, {200, 404})

    def assert_bounded_stop(
        self, router: fastapi.APIRouter, endpoint_reached: threading.Event, left_behind: bool
    ) -> None:
        """
        Kills the service while a request of the given router runs: the stop
        must end within the delays, giving up on the thread of the server if needed
        """
        with (
            # By name: the framework imports the module again after a previous test
            mock.patch("pelix.http.fastapi._SHUTDOWN_TIMEOUT", 0.5),
            mock.patch("pelix.http.fastapi._WS_CLOSE_TIMEOUT", 0.5),
        ):
            self.start_server()
            self.register_router(router, prefix=None)
            thread = threading.Thread(target=lambda: self.request("/block", timeout=30), daemon=True)
            thread.start()
            self.assertTrue(endpoint_reached.wait(5))

            start = time.monotonic()
            with self.assertLogs(SERVER_NAME, "INFO") as logs:
                self.ipopo.kill(SERVER_NAME)
            self.assertLess(time.monotonic() - start, 2.5)
        self.assertEqual(any("did not stop in time" in line for line in logs.output), left_behind)

    def test_bounded_stop_blocked_request(self) -> None:
        """
        A synchronous route which doesn't return: uvicorn cancels its task
        after its graceful delay, leaving the thread of the route
        """
        reached, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def block() -> None:
            reached.set()
            release.wait(30)

        router = fastapi.APIRouter()
        router.add_api_route("/block", block, methods=["GET"])
        self.assert_bounded_stop(router, reached, False)

    def test_bounded_stop_blocked_loop(self) -> None:
        """
        A coroutine blocking the event loop: uvicorn can't even notice the stop
        """
        reached = threading.Event()

        async def block() -> None:
            reached.set()
            time.sleep(3)  # noqa: ASYNC251

        router = fastapi.APIRouter()
        router.add_api_route("/block", block, methods=["GET"])
        self.assert_bounded_stop(router, reached, True)

    def test_stop_with_open_websocket(self) -> None:
        """
        uvicorn closes the WebSockets of the routes when the service stops
        """
        router = fastapi.APIRouter()
        opened = threading.Event()

        @router.websocket("/ws")
        async def ws_endpoint(websocket: fastapi.WebSocket) -> None:
            await websocket.accept()
            opened.set()
            while True:
                await websocket.receive_text()

        self.start_server()
        self.register_router(router, prefix=None)
        with websockets.sync.client.connect(f"ws://{DEFAULT_HOST}:{self.port}/ws") as ws:
            self.assertTrue(opened.wait(5))
            start = time.monotonic()
            self.ipopo.kill(SERVER_NAME)
            self.assertLess(time.monotonic() - start, 3)
            with self.assertRaises(websockets.exceptions.ConnectionClosed) as ctx:
                ws.recv(5)
            # Service restart
            self.assertEqual(ctx.exception.rcvd.code if ctx.exception.rcvd else None, 1012)


class FastApiConfigAdminTest(unittest.TestCase):
    """
    The properties of the application can be updated while the service runs
    """

    def test_update_title(self) -> None:
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        framework = create_framework(
            (
                "pelix.ipopo.core",
                "pelix.ipopo.waiting",
                "pelix.services.configadmin",
                "pelix.ipopo.configadmin",
                "pelix.http.fastapi",
            ),
            {"configuration.folder": folder},
        )
        self.addCleanup(FrameworkFactory.delete_framework)
        framework.start()
        context = framework.get_bundle_context()
        config_admin = context.get_service(
            cast(Any, context.get_service_reference(services.IConfigurationAdmin))
        )

        with socket.socket() as sock:
            sock.bind((DEFAULT_HOST, 0))
            port = sock.getsockname()[1]

        properties = {
            IPOPO_CONFIG_FACTORY_NAME: http.FACTORY_HTTP_FASTAPI,
            IPOPO_INSTANCE_NAME: SERVER_NAME,
            http.HTTP_SERVICE_ADDRESS: DEFAULT_HOST,
            http.HTTP_SERVICE_PORT: port,
            fastapi_http.HTTP_FASTAPI_TITLE: "First",
        }
        config = config_admin.create_factory_configuration(IPOPO_CONFIGADMIN_FACTORY_PID)
        config.update(properties)

        def title() -> str | None:
            conn = httplib.HTTPConnection(DEFAULT_HOST, port, timeout=5)
            try:
                conn.request("GET", "/openapi.json")
                return json.loads(conn.getresponse().read())["info"]["title"]
            except (OSError, ValueError):
                return None
            finally:
                conn.close()

        for expected in ("First", "Second"):
            config.update({**properties, fastapi_http.HTTP_FASTAPI_TITLE: expected})
            deadline = time.monotonic() + 5
            while title() != expected and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertEqual(title(), expected)


# ------------------------------------------------------------------------------


if __name__ == "__main__":
    unittest.main()
