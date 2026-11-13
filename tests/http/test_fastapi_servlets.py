#!/usr/bin/env python
# -- Content-Encoding: UTF-8 --
"""
Runs the servlet tests of the async HTTP service against the FastAPI HTTP
service: both must handle the servlets the same way.

:author: Thomas Calmant
"""

import logging
import unittest
from typing import cast

try:
    import fastapi  # noqa: F401
except ImportError:
    raise unittest.SkipTest("fastapi library not available") from None

import tests.http.test_auth as auth_tests
import tests.http.test_base as base_tests
import tests.http.test_basic_async as basic_async_tests
import tests.http.test_basic_async_ssl as ssl_tests
import tests.http.test_basic_async_websockets as websocket_tests
import tests.http.test_basic_sse as sse_tests
import tests.http.test_body_limits as body_tests
import tests.http.test_contextvars as context_tests
import tests.http.test_cors as cors_tests
import tests.http.test_routing_async as routing_tests
from pelix import http
from pelix.utilities import EventData
from tests.http.utils import async_test

try:
    from aiohttp import ClientSession, WSMsgType
except ImportError:
    raise unittest.SkipTest("aiohttp library not available") from None

# ------------------------------------------------------------------------------

__version_info__ = (3, 3, 0)
__version__ = ".".join(str(x) for x in __version_info__)

# ------------------------------------------------------------------------------


def make_test_class(module: object, test_class: str) -> type:
    """
    Makes a copy of a test class of the async HTTP service, running against the
    FastAPI HTTP service. Built with type() so that the original class isn't
    collected twice.

    :param module: The module of the test class
    :param test_class: The name of the test class
    :return: A test class
    """
    base_class = cast(type, getattr(module, test_class))
    return type(
        f"FastApi{base_class.__name__}",
        (base_class,),
        {
            "http_bundle": "pelix.http.fastapi",
            "http_factory": http.FACTORY_HTTP_FASTAPI,
            "instance_name": "test-http-fastapi",
            # Don't inherit the skip on a missing aiohttp
            "__unittest_skip__": False,
        },
    )


FastApiEndToEndPathTest = make_test_class(base_tests, "AsyncEndToEndPathTest")
FastApiHTTPServiceMethodsTest = make_test_class(basic_async_tests, "AsyncHTTPServiceMethodsTest")
FastApiHTTPServiceServletsTest = make_test_class(basic_async_tests, "AsyncHTTPServiceServletsTest")
FastApiFullAsyncMethodsTest = make_test_class(basic_async_tests, "FullAsyncHTTPServiceMethodsTest")
FastApiFullAsyncServletsTest = make_test_class(basic_async_tests, "FullAsyncHTTPServiceServletsTest")
FastApiHTTPSTest = make_test_class(ssl_tests, "AsyncHTTPSTest")
FastApiBodyLimitsTest = make_test_class(body_tests, "AsyncBodyLimitsTest")
FastApiPerServletLimitsTest = make_test_class(body_tests, "AsyncPerServletLimitsTest")
FastApiSyncServletCorsTest = make_test_class(cors_tests, "AsyncSyncServletCorsTest")
FastApiCorsTest = make_test_class(cors_tests, "AsyncCorsTest")
FastApiSyncServletAuthTest = make_test_class(auth_tests, "AsyncSyncServletAuthTest")
FastApiAuthTest = make_test_class(auth_tests, "AsyncAuthTest")
FastApiRoutingTests = make_test_class(routing_tests, "AsyncHttpRoutingTests")


class FastApiWebSocketTest(websocket_tests.WebSocketTestCase):
    """
    WebSocket handlers on the FastAPI HTTP service
    """

    http_bundle = "pelix.http.fastapi"
    http_factory = http.FACTORY_HTTP_FASTAPI

    @async_test
    async def test_client_closes_session(self) -> None:
        """
        When the handler closed the session, uvicorn gives the code it sent,
        not the one of the answer of the client
        """
        closed = EventData[tuple[int, str]]()

        class WSHandler(http.AbstractWebSocketHandler):
            async def ws_message(self, session: http.WebSocketSession, message: str) -> None:
                await session.send_text(f"Echo: {message}")
                await session.close(4000)

            async def ws_close(self, session: http.WebSocketSession, code: int, reason: str) -> None:
                closed.set((code, reason))

        self.register_handler(WSHandler())
        async with ClientSession() as session, session.ws_connect(self.ws_url) as ws:
            await ws.send_str("hello")
            msg = await ws.receive(timeout=1)
            self.assertEqual(msg.data, "Echo: hello")
            msg = await ws.receive(timeout=1)
            self.assertEqual((msg.type, msg.data), (WSMsgType.CLOSE, 4000))

        self.assertTrue(closed.wait(1))
        assert closed.data is not None
        self.assertEqual(closed.data[0], 4000)


FastApiSSETest = make_test_class(sse_tests, "SSETestCase")
FastApiContextTest = make_test_class(context_tests, "AsyncBackendContextTest")

# ------------------------------------------------------------------------------


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
