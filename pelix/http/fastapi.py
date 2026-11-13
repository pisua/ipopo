#!/usr/bin/env python
# -- Content-Encoding: UTF-8 --
"""
Pelix HTTP service based on FastAPI, served by uvicorn.

It serves the servlets of the other HTTP services (sync, async and WebSocket)
and the FastAPI routers provided as services: a router is mounted when its
service is registered and removed when it is unregistered, the OpenAPI schema
following.

:author: Thomas Calmant
:copyright: Copyright 2026, Thomas Calmant
:license: Apache License 2.0
:version: 3.3.0

..

    Copyright 2026 Thomas Calmant

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""

import asyncio
import concurrent.futures
import io
import logging
import os
import socket
import sys
import threading
import traceback
import urllib.parse
from collections.abc import Coroutine
from typing import IO, TYPE_CHECKING, Any, Protocol, cast

import fastapi
import starlette.concurrency
import starlette.datastructures
import starlette.exceptions
import starlette.requests
import starlette.responses
import starlette.types
import starlette.websockets
import uvicorn

import pelix.ipv6utils
from pelix import http, utilities
from pelix.constants import Specification
from pelix.http._base import (
    SERVLET_PARAMETERS,
    AbstractHttpService,
    RequestRouting,
    SessionContext,
    compute_sub_path,
    format_sse_event,
)
from pelix.internals.registry import ServiceReference
from pelix.ipopo.decorators import (
    BindField,
    ComponentFactory,
    Invalidate,
    Property,
    Provides,
    Requires,
    UnbindField,
    UpdateField,
    Validate,
)
from pelix.security import AccessDenied, AuthenticationFailed, AuthenticationRequired, Subject, run_as

if TYPE_CHECKING:
    from pelix.framework import BundleContext

# ------------------------------------------------------------------------------

# Module version
__version_info__ = (3, 3, 0)
__version__ = ".".join(str(x) for x in __version_info__)

# Documentation strings format
__docformat__ = "restructuredtext en"

__all__ = [
    "HTTP_FASTAPI_DOCS_URL",
    "HTTP_FASTAPI_OPENAPI_URL",
    "HTTP_FASTAPI_PREFIX",
    "HTTP_FASTAPI_ROOT_PATH",
    "HTTP_FASTAPI_ROUTER",
    "HTTP_FASTAPI_TITLE",
    "FastApiHttpServiceImpl",
    "FastApiRouter",
]

# ------------------------------------------------------------------------------

HTTP_FASTAPI_ROUTER = "pelix.http.fastapi.router"
""" Specification of the services giving a FastAPI router (see :class:`FastApiRouter`) """

HTTP_FASTAPI_PREFIX = "pelix.http.fastapi.prefix"
"""
Router service property: path prefix of the routes of the router (string, none
by default). It starts with a slash and doesn't end with one.
"""

HTTP_FASTAPI_TITLE = "pelix.http.fastapi.title"
""" HTTP service property: title of the API in its OpenAPI schema ("Pelix" by default) """

HTTP_FASTAPI_OPENAPI_URL = "pelix.http.fastapi.openapi_url"
"""
HTTP service property: path of the OpenAPI schema (``/openapi.json`` by
default). Empty to disable it, and the documentation page with it.
"""

HTTP_FASTAPI_DOCS_URL = "pelix.http.fastapi.docs_url"
""" HTTP service property: path of the Swagger UI page (``/docs`` by default), empty to disable it """

HTTP_FASTAPI_ROOT_PATH = "pelix.http.fastapi.root_path"
"""
HTTP service property: path prefix stripped by a reverse proxy before the
requests reach the service (empty by default). It is given to FastAPI, so that
the documentation and the OpenAPI schema give the URLs of the clients.
"""

_SHUTDOWN_TIMEOUT = 5
""" Seconds uvicorn waits for the requests being handled when the service stops, before cancelling them """

_WS_CLOSE_TIMEOUT = 2.0
""" Seconds the stop of the service waits for the close of the open WebSocket servlets """

_WS_DEFAULT_MAX_SIZE = 4 * 1024 * 1024
""" Default maximum size of the messages of a WebSocket servlet, as the async HTTP service """

_WS_MAX_SIZE = 16 * 1024 * 1024
""" Maximum size of the WebSocket messages uvicorn accepts (its default), for all the WebSockets """

_WS_GOING_AWAY = 1001
_WS_POLICY_VIOLATION = 1008
_WS_MESSAGE_TOO_BIG = 1009

# ------------------------------------------------------------------------------


@Specification(HTTP_FASTAPI_ROUTER)
class FastApiRouter(Protocol):
    """
    Service giving a FastAPI router: the FastAPI HTTP services serve its routes
    while the service is registered, under the prefix given by its
    ``pelix.http.fastapi.prefix`` property.

    A ``fastapi.APIRouter`` can also be registered as is with this specification.
    """

    def get_router(self) -> fastapi.APIRouter:
        """
        Returns the router to serve. It is called each time the routers
        served by the HTTP service change.
        """
        ...


class _Headers(starlette.datastructures.Headers):
    """
    Headers of an ASGI request, with the ``get_all()`` the trusted proxies check needs
    """

    def get_all(self, name: str, default: Any = None) -> Any:
        return self.getlist(name) or default


def _content_length(headers: _Headers) -> int | None:
    """
    Returns the size of the body announced by the client, None if it is unknown
    """
    try:
        size = int(headers.get("content-length", ""))
    except ValueError:
        return None
    return size if size >= 0 else None


class _BodyStream:
    """
    Gives the body of an ASGI request as an asyncio stream, read from the
    client while the buffer of the stream isn't full. The stream calls
    ``pause_reading()`` and ``resume_reading()``, as on a transport.

    Once the body is read, it waits for the client to disconnect: uvicorn
    silently drops what is sent after that, so a streamed response must check.
    """

    def __init__(self, receive: starlette.types.Receive) -> None:
        self._receive = receive
        self._reader = asyncio.StreamReader()
        self._reader.set_transport(cast(asyncio.ReadTransport, self))
        self._resumed = asyncio.Event()
        self._resumed.set()
        self._task: asyncio.Task[None] | None = None
        self.disconnected = asyncio.Event()

    def pause_reading(self) -> None:
        self._resumed.clear()

    def resume_reading(self) -> None:
        self._resumed.set()

    def get_reader(self) -> asyncio.StreamReader:
        """
        Returns the stream of the body, starting to read it
        """
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self.__feed())
        return self._reader

    async def __feed(self) -> None:
        try:
            while True:
                await self._resumed.wait()
                message = await self._receive()
                if message["type"] != "http.request":
                    # The client is gone
                    self.disconnected.set()
                    return

                if body := message.get("body"):
                    self._reader.feed_data(body)
                if not message.get("more_body"):
                    break
        finally:
            self._reader.feed_eof()

        while (await self._receive())["type"] != "http.disconnect":
            pass
        self.disconnected.set()

    def close(self) -> None:
        """
        Stops reading the body
        """
        if self._task is not None:
            self._task.cancel()


class _HeaderList:
    """
    Headers of a response, in order, repeated ones included
    """

    def __init__(self) -> None:
        self.items: list[tuple[str, str]] = []

    def set(self, name: str, value: Any) -> None:
        self.remove(name)
        if value is not None:
            self.items.append((name, str(value)))

    def add(self, name: str, value: Any) -> None:
        self.items.append((name, str(value)))

    def remove(self, name: str) -> None:
        name = name.lower()
        self.items = [item for item in self.items if item[0].lower() != name]

    def has(self, name: str) -> bool:
        name = name.lower()
        return any(item[0].lower() == name for item in self.items)

    def encode(self) -> list[tuple[bytes, bytes]]:
        return [(name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in self.items]


class _ScopeRequest:
    """
    What the sync and async servlet requests take from the ASGI scope
    """

    remote: tuple[tuple[str, int], bool] | None
    sessions: Any

    def __init__(
        self,
        scope: starlette.types.Scope,
        headers: _Headers,
        full_path: str,
        prefix: str,
        remote: tuple[tuple[str, int], bool] | None,
        sessions: SessionContext | None,
    ) -> None:
        self.remote = remote
        self.sessions = sessions
        self._scope = scope
        self._headers = headers
        self._full_path = full_path
        self._prefix = prefix
        self._sub_path = compute_sub_path(full_path, prefix)

    def get_command(self) -> str:
        return str(self._scope.get("method", "GET")).upper()

    def get_query(self) -> dict[str, list[str]]:
        query = cast(bytes, self._scope.get("query_string", b"")).decode("latin-1")
        return urllib.parse.parse_qs(query, keep_blank_values=True)

    def get_peer_address(self) -> tuple[str, int]:
        client = self._scope.get("client")
        if not client:
            raise OSError("No peer address available for the request")
        return client[0], client[1]

    def get_path(self) -> str:
        return self._full_path

    def get_prefix_path(self) -> str:
        return self._prefix

    def get_sub_path(self) -> str:
        return self._sub_path


class _SyncHTTPServletRequest(_ScopeRequest, http.AbstractHTTPServletRequest):
    """
    Request given to a synchronous servlet: its body is read before it is called
    """

    def __init__(self, *args: Any, content: bytes) -> None:
        super().__init__(*args)
        self._content = content

    def get_header(self, name: str, default: Any = None) -> Any:
        return self._headers.get(name, default)

    def get_headers(self) -> dict[str, Any]:
        return cast(dict[str, Any], self._headers)

    def get_rfile(self) -> IO[bytes]:
        return io.BytesIO(self._content)


class _AsyncHTTPServletRequest(_ScopeRequest, http.AbstractAsyncHTTPServletRequest):
    """
    Request given to an asynchronous servlet or a WebSocket handler
    """

    def __init__(self, *args: Any, body: _BodyStream | None = None, max_body_size: int | None = None) -> None:
        super().__init__(*args)
        self._body = body
        self.max_body_size = max_body_size

    async def get_header(self, name: str, default: Any = None) -> Any:
        return self._headers.get(name, default)

    async def get_headers(self) -> dict[str, Any]:
        return cast(dict[str, Any], self._headers)

    def get_rfile(self) -> asyncio.StreamReader:
        if self._body is None:
            # WebSocket handshake: no body
            reader = asyncio.StreamReader()
            reader.feed_eof()
            return reader
        return self._body.get_reader()


class _BodyBuffer(utilities.StreamBase[bytes]):
    """
    Write-only stream buffering the body of a synchronous servlet response
    """

    _MODE = "wb"
    _WRITABLE = True

    def __init__(self) -> None:
        super().__init__()
        self._buffer = io.BytesIO()

    def get(self) -> bytes:
        return self._buffer.getvalue()

    def write(self, data: bytes) -> int:
        self._check_open()
        return self._buffer.write(data)


class _SyncHTTPServletResponse(http.AbstractHTTPServletResponse):
    """
    Response of a synchronous servlet: sent once the servlet returned
    """

    def __init__(self) -> None:
        self._code = 200
        self._headers = _HeaderList()
        self._headers_set = False
        self._buffer = _BodyBuffer()

    def _check_headers(self) -> None:
        if self._headers_set:
            raise OSError("Headers have already been set, cannot change them")

    def set_response(self, code: int, message: str | None = None) -> None:
        # ASGI has no reason phrase
        self._check_headers()
        self._code = code

    def set_header(self, name: str, value: Any) -> None:
        self._check_headers()
        self._headers.set(name, value)

    def add_header(self, name: str, value: Any) -> None:
        self._check_headers()
        self._headers.add(name, value)

    def is_header_set(self, name: str) -> bool:
        return self._headers.has(name)

    def end_headers(self) -> None:
        self._headers_set = True

    def get_wfile(self) -> IO[bytes]:
        if not self._headers_set:
            self.end_headers()
        return self._buffer

    def write(self, data: bytes) -> None:
        self.get_wfile().write(data)

    async def send(self, send: starlette.types.Send) -> None:
        """
        Sends the response
        """
        body = self._buffer.get()
        if not self._headers.has("content-length"):
            self._headers.set("content-length", len(body))
        await send({"type": "http.response.start", "status": self._code, "headers": self._headers.encode()})
        await send({"type": "http.response.body", "body": body})


class _AsgiWriter(http.AbstractAsyncWriter):
    """
    Writer of the body of an asynchronous servlet response
    """

    def __init__(self, response: "_AsyncHTTPServletResponse") -> None:
        self._response = response

    async def write(self, raw: bytes) -> int:
        await self._response.write(raw)
        return len(raw)

    async def flush(self) -> None:
        # Each write is handed to the server
        pass


class _AsyncHTTPServletResponse(http.AbstractAsyncHTTPServletResponse):
    """
    Response of an asynchronous servlet: streamed once its headers are sent
    """

    def __init__(self, headers: _Headers, send: starlette.types.Send, body: _BodyStream) -> None:
        self._request_headers = headers
        self._send = send
        self._body = body
        self._code = 200
        self._headers = _HeaderList()
        self.started = False
        self._sse_set = False

    def _check_headers(self) -> None:
        if self.started:
            raise OSError("Headers have already been set, cannot change them")

    def set_response(self, code: int, message: str | None = None) -> None:
        # ASGI has no reason phrase
        self._check_headers()
        self._code = code

    def set_header(self, name: str, value: Any) -> None:
        self._check_headers()
        self._headers.set(name, value)

    def add_header(self, name: str, value: Any) -> None:
        self._check_headers()
        self._headers.add(name, value)

    def is_header_set(self, name: str) -> bool:
        return self._headers.has(name)

    def setup_sse(self, strict: bool = True) -> None:
        if self._sse_set:
            return

        if strict and not any(
            "text/event-stream" in accepted for accepted in self._request_headers.getlist("accept")
        ):
            raise ValueError("Cannot set up SSE for a non-SSE request")

        self._check_headers()
        self._headers.set("Content-Type", "text/event-stream")
        self._headers.set("Cache-Control", "no-cache")
        self._sse_set = True

    async def end_headers(self) -> None:
        if not self.started:
            self.started = True
            # Watch for the client to leave
            self._body.get_reader()
            await self._send(
                {"type": "http.response.start", "status": self._code, "headers": self._headers.encode()}
            )

    def get_wfile(self) -> http.AbstractAsyncWriter:
        return _AsgiWriter(self)

    async def write(self, data: bytes) -> None:
        await self.end_headers()
        if self._body.disconnected.is_set():
            raise ConnectionResetError("The client is disconnected")
        await self._send({"type": "http.response.body", "body": data, "more_body": True})

    async def send_sse(
        self, event: str | None = None, data: str | None = None, id: str | None = None
    ) -> None:
        if not self._sse_set:
            raise OSError("SSE not set up, call setup_sse() first")
        await self.write(format_sse_event(event, data, id))

    async def finish(self) -> None:
        """
        Ends the response, sending its headers if the servlet didn't
        """
        await self.end_headers()
        await self._send({"type": "http.response.body", "body": b""})


class _WSSession(http.WebSocketSession):
    """
    WebSocket session given to a WebSocket handler
    """

    def __init__(
        self,
        websocket: starlette.websockets.WebSocket,
        request: _AsyncHTTPServletRequest,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._websocket = websocket
        self._request = request
        # Kept for the submit_* methods, called from other threads
        self._loop = loop

    def is_open(self) -> bool:
        return self._websocket.application_state == starlette.websockets.WebSocketState.CONNECTED

    def get_client_address(self) -> tuple[str, int]:
        return self._request.get_client_address()

    async def __send(self, message: starlette.types.Message) -> None:
        if not self.is_open():
            raise OSError("WebSocket session is closed")
        try:
            await self._websocket.send(message)
        except (RuntimeError, starlette.websockets.WebSocketDisconnect) as ex:
            raise OSError("WebSocket session is closed") from ex

    async def send_binary(self, message: bytes) -> None:
        await self.__send({"type": "websocket.send", "bytes": message})

    async def send_text(self, message: str) -> None:
        await self.__send({"type": "websocket.send", "text": message})

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        if self.is_open():
            try:
                await self._websocket.close(code, reason)
            except (OSError, RuntimeError, starlette.websockets.WebSocketDisconnect):
                # The client is already gone
                pass

    def _submit(self, coroutine: Coroutine[Any, Any, None]) -> concurrent.futures.Future[None]:
        try:
            return asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        except RuntimeError as ex:
            # The loop is closed: close the coroutine to avoid a "never awaited" warning
            coroutine.close()
            raise OSError("The HTTP server is stopped") from ex

    def submit_text(self, message: str) -> concurrent.futures.Future[None]:
        return self._submit(self.send_text(message))

    def submit_binary(self, message: bytes) -> concurrent.futures.Future[None]:
        return self._submit(self.send_binary(message))

    def submit_close(self, code: int = 1000, reason: str | None = None) -> concurrent.futures.Future[None]:
        return self._submit(self.close(code, reason))


class _Server(uvicorn.Server):
    """
    uvicorn server telling when it serves, and on which loop
    """

    def __init__(self, config: uvicorn.Config) -> None:
        super().__init__(config)
        self.ready: utilities.EventData[asyncio.AbstractEventLoop] = utilities.EventData()

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        await super().startup(sockets)
        self.ready.set(asyncio.get_running_loop())


async def _send_response(
    scope: starlette.types.Scope,
    receive: starlette.types.Receive,
    send: starlette.types.Send,
    response: starlette.responses.Response,
) -> None:
    """
    Sends a response, or refuses the handshake of a WebSocket with it when the
    server supports it (else with a 403)
    """
    if scope["type"] == "http":
        await response(scope, receive, send)
        return

    websocket = starlette.websockets.WebSocket(scope, receive, send)
    if "websocket.http.response" in scope.get("extensions", {}):
        await websocket.send_denial_response(response)
    else:
        await websocket.close(_WS_POLICY_VIOLATION)


def _html(code: int, page: str, headers: dict[str, str] | None = None) -> starlette.responses.Response:
    return starlette.responses.HTMLResponse(page, code, headers)


_BAD_REQUEST = "<html><body><h1>Bad Request</h1></body></html>"

# ------------------------------------------------------------------------------


@ComponentFactory(http.FACTORY_HTTP_FASTAPI)
@Provides(http.HTTP_SERVICE)
@Requires("_servlets_services", http.Servlet, True, True)
@Requires("_servlets_async_services", http.AsyncServlet, True, True)
@Requires("_websocket_handler_services", http.WebSocketHandler, True, True)
@Requires("_routers_services", FastApiRouter, True, True)
@Requires("_error_handler", http.ErrorHandler, optional=True)
@Requires("_cors_handler", http.CorsHandler, optional=True)
@Requires("_http_authenticators", http.HttpAuthenticator, aggregate=True, optional=True)
@Property("_title", HTTP_FASTAPI_TITLE, "Pelix")
@Property("_openapi_url", HTTP_FASTAPI_OPENAPI_URL, "/openapi.json")
@Property("_docs_url", HTTP_FASTAPI_DOCS_URL, "/docs")
@Property("_root_path", HTTP_FASTAPI_ROOT_PATH, "")
class FastApiHttpServiceImpl(AbstractHttpService):
    """
    HTTP service component based on FastAPI and uvicorn
    """

    def __init__(self) -> None:
        super().__init__()

        # Fields injected by iPOPO
        self._servlets_services: list[http.Servlet] = []
        self._servlets_async_services: list[http.AsyncServlet] = []
        self._websocket_handler_services: list[http.WebSocketHandler] = []
        self._routers_services: list[FastApiRouter] = []

        # FastAPI application properties
        self._title = "Pelix"
        self._openapi_url: str | None = "/openapi.json"
        self._docs_url: str | None = "/docs"
        self._root_path: str | None = ""

        # Router service -> its reference
        self._routers: dict[ServiceReference[Any], Any] = {}

        # The application serving the routers, replaced when they change
        self._app: fastapi.FastAPI | None = None

        # Server control
        self._bound_address: tuple[str, int] | None = None
        self._server: _Server | None = None
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        # Open WebSockets of the servlets, closed with "going away" at stop
        self._ws_sessions: set[_WSSession] = set()

    def __str__(self) -> str:
        return f"FastApiHttpService({self._address}, {self._port})"

    # --------------------------------------------------------------------------
    # Life cycle
    # --------------------------------------------------------------------------

    @Validate
    def validate(self, context: "BundleContext") -> None:
        """
        Component validated: binds the socket and starts uvicorn in its thread
        """
        self._normalize_configuration()
        self._setup_logger()

        # Bound here, so that an address in use makes the component fail now
        sock = self.__bind()
        self._bound_address = cast(tuple[str, int], sock.getsockname()[:2])
        self._port = self._bound_address[1]

        try:
            server = self.__make_server()
            with self._binding_lock:
                self._app = self.__make_app()
        except BaseException:
            sock.close()
            raise

        thread = threading.Thread(
            target=self.__run, args=(server, sock), name=f"HttpService-FastAPI-{self._port}", daemon=True
        )
        thread.start()
        try:
            if not server.ready.wait(10):
                raise OSError("HTTP server did not start in time")
        except BaseException:
            server.should_exit = True
            thread.join(_SHUTDOWN_TIMEOUT)
            self._app = None
            raise

        self._server = server
        self._thread = thread
        self._loop = server.ready.data

        # Register the servlets bound before the server was ready
        self._register_bound_servlets()

        self.log(
            logging.INFO,
            "HTTP%s server (FastAPI) started: [%s]:%d",
            "S" if self._uses_ssl else "",
            self._address,
            self._port,
        )

    @Invalidate
    def invalidate(self, context: "BundleContext") -> None:
        """
        Component invalidated: stops uvicorn, within a bounded delay
        """
        # Refuse new registrations and notify the bound servlets
        self._unregister_all_servlets()

        self.log(logging.INFO, "Stopping HTTP server (FastAPI): [%s]:%d ...", self._address, self._port)

        server, thread, loop = self._server, self._thread, self._loop
        if loop is not None and self._ws_sessions:
            # As the async HTTP service does: uvicorn would close them with 1012
            future = asyncio.run_coroutine_threadsafe(self.__close_websockets(), loop)
            try:
                future.result(_WS_CLOSE_TIMEOUT)
            except Exception as ex:  # noqa: BLE001
                future.cancel()
                self.log(logging.WARNING, "Open WebSockets not closed in time: %s", ex or type(ex).__name__)

        if server is not None:
            server.should_exit = True
        if thread is not None:
            # uvicorn cancels the requests still running after its own timeout
            thread.join(_SHUTDOWN_TIMEOUT + _WS_CLOSE_TIMEOUT)
            if thread.is_alive():
                if server is not None:
                    server.force_exit = True
                # Its event loop is blocked, e.g. by a coroutine calling blocking code
                self.log(logging.WARNING, "The HTTP server thread did not stop in time: leaving it behind")

        self._server = None
        self._thread = None
        self._loop = None
        self._app = None
        self._ws_sessions.clear()

    def __make_server(self) -> _Server:
        """
        Prepares uvicorn
        """
        password = self._key_password.reveal() if self._key_password else None
        config = uvicorn.Config(
            self.__asgi,
            interface="asgi3",
            loop="asyncio",
            lifespan="off",
            # The logging of the application is left untouched
            log_config=None,
            access_log=False,
            # The trusted proxies are those of pelix.http.proxy.trusted
            proxy_headers=False,
            server_header=False,
            ws_max_size=_WS_MAX_SIZE,
            timeout_graceful_shutdown=_SHUTDOWN_TIMEOUT,
            ssl_certfile=self._cert_file,
            ssl_keyfile=self._key_file,
            ssl_keyfile_password=password,
        )
        return _Server(config)

    def __bind(self) -> socket.socket:
        """
        Prepares the server socket
        """
        family, _, _, _, address = socket.getaddrinfo(
            self._address, self._port, 0, socket.SOCK_STREAM, socket.IPPROTO_TCP
        )[0]
        sock = socket.socket(family, socket.SOCK_STREAM)
        try:
            if os.name == "posix" and sys.platform != "cygwin":
                # As asyncio: a restart must not wait for the TIME_WAIT of the previous sockets
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if family == socket.AF_INET6:
                try:
                    pelix.ipv6utils.set_double_stack(sock)
                except (AttributeError, OSError) as ex:
                    self.log(logging.WARNING, "Error setting up the IPv6 double stack: %s", ex)
            sock.bind(address)
        except BaseException:
            sock.close()
            raise
        return sock

    def __run(self, server: _Server, sock: socket.socket) -> None:
        """
        Runs uvicorn, in the thread of the server
        """
        try:
            server.run(sockets=[sock])
        except (Exception, SystemExit) as ex:  # noqa: BLE001
            self.log_exception("Error running the HTTP server: %s", ex)
            if not server.ready.is_set():
                server.ready.raise_exception(ex if isinstance(ex, Exception) else OSError(str(ex)))
        finally:
            sock.close()

    async def __close_websockets(self) -> None:
        sessions = list(self._ws_sessions)
        await asyncio.gather(
            *(session.close(_WS_GOING_AWAY, "Server stopping") for session in sessions),
            return_exceptions=True,
        )

    def get_access(self) -> tuple[str, int]:
        """
        Retrieves the (address, port) tuple to access the server
        """
        assert self._bound_address is not None, "Server must be started before accessing its address"
        return self._bound_address

    # --------------------------------------------------------------------------
    # FastAPI application
    # --------------------------------------------------------------------------

    def _normalize_live_configuration(self) -> None:
        """
        Also applies an update of the properties of the FastAPI application
        """
        super()._normalize_live_configuration()
        with self._binding_lock:
            if self._validated:
                self._app = self.__make_app()

    def __make_app(self) -> fastapi.FastAPI:
        """
        Prepares a FastAPI application with the current routers.

        A new application is made each time they change: FastAPI has no way to
        remove a route, and the requests being handled keep the previous one.
        """
        app = fastapi.FastAPI(
            title=str(self._title or "Pelix"),
            openapi_url=self._openapi_url or None,
            docs_url=self._docs_url or None,
            redoc_url=None,
            debug=bool(self._debug_errors),
        )
        # Requests no route matches
        app.router.default = self.__not_found
        app.add_exception_handler(AuthenticationRequired, self.__route_auth_error)
        app.add_exception_handler(AuthenticationFailed, self.__route_auth_error)
        app.add_exception_handler(AccessDenied, self.__route_auth_error)

        for reference in sorted(self._routers):
            service = self._routers[reference]
            prefix = reference.get_property(HTTP_FASTAPI_PREFIX) or ""
            try:
                router = service if isinstance(service, fastapi.APIRouter) else service.get_router()
                app.include_router(router, prefix=prefix)
            except Exception as ex:  # noqa: BLE001
                # The other routers must still be served
                self.log_exception("Error mounting the router of %s: %s", reference, ex)
        return app

    async def __not_found(
        self, scope: starlette.types.Scope, receive: starlette.types.Receive, send: starlette.types.Send
    ) -> None:
        await _send_response(
            scope, receive, send, _html(404, self.make_not_found_page(scope.get("path", "")))
        )

    async def __route_auth_error(
        self, request: starlette.requests.Request, exc: Exception
    ) -> starlette.responses.Response:
        """
        A route raised a ``pelix.security`` refusal
        """
        if isinstance(exc, AccessDenied):
            return starlette.responses.JSONResponse({"detail": "Forbidden"}, 403)

        headers = {}
        challenge = self.get_auth_challenge(
            RequestRouting(request.url.path, None, {}, "", http.ServletType.SYNC)
        )
        if challenge:
            headers["WWW-Authenticate"] = challenge
        return starlette.responses.JSONResponse({"detail": "Not authenticated"}, 401, headers)

    def __update_routers(self) -> None:
        if self._validated:
            self._app = self.__make_app()

    @BindField("_routers_services")
    def _bind_router(self, _: str, service: Any, service_reference: ServiceReference[Any]) -> None:
        """
        A router service is bound: it is served from now on
        """
        if self._is_imported(service_reference):
            return

        with self._binding_lock:
            self._routers[service_reference] = service
            self.__update_routers()

    @UpdateField("_routers_services")
    def _update_router(
        self,
        _: str,
        service: Any,
        service_reference: ServiceReference[Any],
        old_properties: dict[str, Any],
    ) -> None:
        """
        The properties of a router service changed: its prefix or its ranking
        """
        if service_reference not in self._routers:
            return

        with self._binding_lock:
            self.__update_routers()

    @UnbindField("_routers_services")
    def _unbind_router(self, _: str, service: Any, service_reference: ServiceReference[Any]) -> None:
        """
        A router service is gone: its routes are removed
        """
        with self._binding_lock:
            if self._routers.pop(service_reference, None) is not None:
                self.__update_routers()

    # --------------------------------------------------------------------------
    # Requests
    # --------------------------------------------------------------------------

    async def __asgi(
        self, scope: starlette.types.Scope, receive: starlette.types.Receive, send: starlette.types.Send
    ) -> None:
        """
        Entry point of the requests: the checks of the HTTP service, then the
        servlet of the longest prefix of the path, else the FastAPI routes
        """
        if scope["type"] not in ("http", "websocket"):
            # The life span is the one of the component
            return

        app = self._app
        if app is None:
            await _send_response(
                scope, receive, send, starlette.responses.PlainTextResponse("Service unavailable", 503)
            )
            return

        headers = _Headers(scope=scope)
        method = str(scope.get("method", "GET"))
        raw_path = cast(bytes, scope.get("raw_path") or scope["path"].encode("utf-8")).decode("latin-1")
        client = scope.get("client")
        peer = (str(client[0]), int(client[1])) if client else ("", 0)
        checks = self.check_request(method, raw_path, headers, peer)
        if checks.refused:
            # A rebound host name or a page of another site
            await _send_response(scope, receive, send, _html(403, self.make_auth_error_page(403)))
            return

        routing, cors_decision, remote, sessions = (
            checks.routing,
            checks.cors_decision,
            checks.remote,
            checks.sessions,
        )
        if cors_decision is not None and cors_decision.preflight:
            # Preflight requests are answered by the server, not by the servlets
            if cors_decision.headers is None:
                await _send_response(
                    scope, receive, send, _html(403, "<html><body><h1>Forbidden</h1></body></html>")
                )
            else:
                await _send_response(
                    scope,
                    receive,
                    send,
                    starlette.responses.Response(status_code=204, headers=cors_decision.headers),
                )
            return

        send = self.__wrap_send(send, cors_decision.headers if cors_decision else None, sessions)
        if routing.error is not None:
            await _send_response(scope, receive, send, _html(400, _BAD_REQUEST))
            return

        # Authenticate before looking at the servlet: an unauthenticated client
        # mustn't learn which paths exist
        if self.needs_authentication(routing) or (sessions is not None and sessions.cookie_value):
            # Checking a password is slow by design: keep it out of the event loop
            auth_decision = await starlette.concurrency.run_in_threadpool(
                self.resolve_authentication, routing, headers, remote[0][0], sessions, peer[0] or None
            )
        else:
            auth_decision = self.resolve_authentication(
                routing, headers, remote[0][0], sessions, peer[0] or None
            )

        if auth_decision.refused:
            await _send_response(scope, receive, send, self.__auth_error_response(401, routing))
            return

        if routing.servlet is not None:
            await self.__handle_servlet(
                scope, receive, send, headers, routing, remote, sessions, auth_decision.subject
            )
        else:
            await self.__handle_route(
                app, scope, receive, send, headers, routing, remote, auth_decision.subject
            )

    @staticmethod
    def __wrap_send(
        send: starlette.types.Send, cors_headers: dict[str, str] | None, sessions: SessionContext | None
    ) -> starlette.types.Send:
        """
        Adds the CORS headers computed for a request, and the session cookie it
        changed, to its response, whoever builds it
        """
        if not cors_headers and sessions is None:
            return send

        async def wrapped(message: starlette.types.Message) -> None:
            if message["type"] in (
                "http.response.start",
                "websocket.accept",
                "websocket.http.response.start",
            ):
                headers = list(message.get("headers") or [])
                names = {name.lower() for name, _ in headers}
                for name, value in (cors_headers or {}).items():
                    # Keep the headers the servlet set itself
                    if name.lower().encode("latin-1") not in names:
                        headers.append((name.lower().encode("latin-1"), value.encode("latin-1")))
                if sessions is not None and not sessions.sent:
                    # Once, as the other services do
                    sessions.sent = True
                    if sessions.cookie:
                        headers.append((b"set-cookie", sessions.cookie.encode("latin-1")))
                message = {**message, "headers": headers}
            await send(message)

        return wrapped

    def __auth_error_response(self, code: int, routing: RequestRouting) -> starlette.responses.Response:
        """
        Prepares a 401 (with the authentication challenges) or 403 response
        """
        headers: dict[str, str] = {}
        if code == 401:
            challenge = self.get_auth_challenge(routing)
            if challenge:
                headers["WWW-Authenticate"] = challenge
        return _html(code, self.make_auth_error_page(code), headers)

    async def __handle_route(
        self,
        app: fastapi.FastAPI,
        scope: starlette.types.Scope,
        receive: starlette.types.Receive,
        send: starlette.types.Send,
        headers: _Headers,
        routing: RequestRouting,
        remote: tuple[tuple[str, int], bool],
        subject: Subject,
    ) -> None:
        """
        Hands a request no servlet handles to the FastAPI application
        """
        # The client as told by the trusted proxies
        (host, port), secure = remote
        scope = dict(scope)
        scope["client"] = (host, port)
        if self._root_path:
            # As FastAPI did with its deprecated root_path: the proxy strips it, servlets don't see it
            scope["root_path"] = self._root_path
        if scope["type"] == "http":
            scope["scheme"] = "https" if secure else "http"
            max_size = self.resolve_max_body_size(routing.parameters)
            if max_size is not None:
                size = _content_length(headers)
                if size is not None and size > max_size:
                    await _send_response(
                        scope,
                        receive,
                        send,
                        starlette.responses.PlainTextResponse("Request body is too large", 413),
                    )
                    return
                receive = self.__bound_body(receive, max_size)
        else:
            scope["scheme"] = "wss" if secure else "ws"

        try:
            with run_as(subject):
                await app(scope, receive, send)
        except Exception as ex:  # noqa: BLE001
            # Already answered with a 500 error by FastAPI: log it here rather than in uvicorn
            self.log_exception(
                "Error handling %s request to %s: %s", scope.get("method", "WS"), routing.path, ex
            )

    @staticmethod
    def __bound_body(receive: starlette.types.Receive, max_size: int) -> starlette.types.Receive:
        """
        Refuses a body bigger than the limit while FastAPI reads it (chunked request)
        """
        received = 0

        async def bounded() -> starlette.types.Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > max_size:
                    # Raised as is by FastAPI, unlike the other errors of the body
                    raise starlette.exceptions.HTTPException(413, "Request body is too large")
            return message

        return bounded

    async def __handle_servlet(
        self,
        scope: starlette.types.Scope,
        receive: starlette.types.Receive,
        send: starlette.types.Send,
        headers: _Headers,
        routing: RequestRouting,
        remote: tuple[tuple[str, int], bool],
        sessions: SessionContext | None,
        subject: Subject,
    ) -> None:
        """
        Calls a servlet, as the async HTTP service does
        """
        servlet, servlet_type, path, prefix = (
            routing.servlet,
            routing.servlet_type,
            routing.path,
            routing.prefix,
        )
        method = str(scope.get("method", "GET")).upper()
        max_body_size = self.resolve_max_body_size(routing.parameters)
        request_args = (scope, headers, path, prefix, remote, sessions)
        body = _BodyStream(receive)
        async_response: _AsyncHTTPServletResponse | None = None
        response: starlette.responses.Response | None = None

        try:
            # The context is copied to the threads of the synchronous servlets
            with run_as(subject):
                if scope["type"] == "websocket":
                    if servlet_type is http.ServletType.WEBSOCKET and isinstance(
                        servlet, http.WebSocketHandler
                    ):
                        # A servlet can override the size limit of the messages, as with the async service
                        ws_max_size = (
                            max_body_size
                            if http.HTTP_MAX_BODY_SIZE in routing.parameters
                            else _WS_DEFAULT_MAX_SIZE
                        )
                        await self.__handle_websocket(
                            scope,
                            receive,
                            send,
                            servlet,
                            _AsyncHTTPServletRequest(*request_args),
                            ws_max_size,
                        )
                        return

                elif servlet_type is http.ServletType.ASYNC and hasattr(servlet, f"do_async_{method}"):
                    servlet_request = _AsyncHTTPServletRequest(
                        *request_args, body=body, max_body_size=max_body_size
                    )
                    async_response = _AsyncHTTPServletResponse(headers, send, body)
                    await getattr(servlet, f"do_async_{method}")(servlet_request, async_response)
                    await async_response.finish()
                    return

                elif servlet_type is http.ServletType.SYNC and hasattr(servlet, f"do_{method}"):
                    content = await http.read_body(body.get_reader(), _content_length(headers), max_body_size)
                    sync_response = _SyncHTTPServletResponse()
                    await starlette.concurrency.run_in_threadpool(
                        getattr(servlet, f"do_{method}"),
                        _SyncHTTPServletRequest(*request_args, content=content),
                        sync_response,
                    )
                    await sync_response.send(send)
                    return

                elif servlet_type is http.ServletType.WEBSOCKET:
                    # Not an upgrade request
                    response = _html(400, _BAD_REQUEST)

        except (AuthenticationRequired, AuthenticationFailed):
            # The servlet requires an identity: the client can retry with credentials
            response = self.__auth_error_response(401, routing)
        except AccessDenied:
            response = self.__auth_error_response(403, routing)
        except http.MultipartError:
            response = _html(400, _BAD_REQUEST)
        except http.BodyTooLargeError as ex:
            self.log(
                logging.WARNING,
                "Refused a request body of %d bytes on %s (maximum is %d)",
                ex.size,
                path,
                ex.max_size,
            )
            response = starlette.responses.PlainTextResponse("Request body is too large", 413)
        except Exception:  # noqa: BLE001
            # The details are logged by make_exception_page()
            self.log(logging.ERROR, "Error handling %s request to %s", method, path)
            response = _html(500, self.make_exception_page(path, traceback.format_exc()))
        finally:
            body.close()

        if async_response is not None and async_response.started:
            # Too late for an error page
            await async_response.finish()
            return

        if response is None:
            response = _html(404, self.make_not_found_page(path))
        await _send_response(scope, receive, send, response)

    async def __handle_websocket(
        self,
        scope: starlette.types.Scope,
        receive: starlette.types.Receive,
        send: starlette.types.Send,
        handler: http.WebSocketHandler,
        request: _AsyncHTTPServletRequest,
        max_size: int | None,
    ) -> None:
        """
        Runs a WebSocket servlet
        """
        if not await handler.ws_accept(request):
            await _send_response(
                scope,
                receive,
                send,
                starlette.responses.PlainTextResponse("WebSocket connection refused", 400),
            )
            return

        websocket = starlette.websockets.WebSocket(scope, receive, send)
        await websocket.accept()
        session = _WSSession(websocket, request, asyncio.get_running_loop())
        self._ws_sessions.add(session)
        try:
            await handler.ws_open(session, request)
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    await handler.ws_close(
                        session, message.get("code", 1000), message.get("reason") or "Session closed"
                    )
                    break

                text, data = message.get("text"), message.get("bytes")
                size = len(text.encode("utf-8")) if text is not None else len(data or b"")
                if max_size is not None and size > max_size:
                    await session.close(_WS_MESSAGE_TOO_BIG, "Message too big")
                    await handler.ws_close(session, _WS_MESSAGE_TOO_BIG, "Message too big")
                    break

                if text is not None:
                    await handler.ws_message(session, text)
                elif data is not None:
                    await handler.ws_binary(session, data)
        except Exception as ex:  # noqa: BLE001
            self.log_exception("Error handling WebSocket connection")
            await handler.ws_error(session, str(ex))
        finally:
            self._ws_sessions.discard(session)
            await session.close()

    # --------------------------------------------------------------------------
    # Servlets
    # --------------------------------------------------------------------------

    def _set_extra_parameters(self, parameters: dict[str, Any], servlet_type: http.ServletType) -> None:
        """
        Tells the servlet if it is registered in asynchronous mode
        """
        parameters[http.PARAM_ASYNC] = servlet_type != http.ServletType.SYNC

    @BindField("_servlets_services")
    @BindField("_servlets_async_services")
    @BindField("_websocket_handler_services")
    def _bind_servlet(self, _: str, service: Any, service_reference: ServiceReference[Any]) -> None:
        """
        Called by iPOPO when a servlet is bound
        """
        self._on_bind(service, service_reference)

    @UpdateField("_servlets_services")
    @UpdateField("_servlets_async_services")
    @UpdateField("_websocket_handler_services")
    def _update_servlet(
        self, _: str, service: Any, service_reference: ServiceReference[Any], old_properties: dict[str, Any]
    ) -> None:
        """
        Called by iPOPO when the properties of a servlet have been updated
        """
        self._on_update(
            service,
            service_reference,
            old_properties,
            (
                http.HTTP_SERVLET_PATH,
                http.HTTP_SERVLET_ASYNC_PATH,
                http.HTTP_WEBSOCKET_PATH,
                *SERVLET_PARAMETERS,
            ),
        )

    @UnbindField("_servlets_services")
    @UnbindField("_servlets_async_services")
    @UnbindField("_websocket_handler_services")
    def _unbind_servlet(self, _: str, service: Any, service_reference: ServiceReference[Any]) -> None:
        """
        Called by iPOPO when a servlet is gone
        """
        self._on_unbind(service, service_reference)
