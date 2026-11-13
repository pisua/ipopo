#!/usr/bin/env python
# -- Content-Encoding: UTF-8 --
"""
Bundle providing a FastAPI router component, for the tests of the FastAPI HTTP service

:author: Thomas Calmant
"""

import threading

import fastapi

from pelix.http.fastapi import HTTP_FASTAPI_PREFIX, FastApiRouter
from pelix.ipopo.decorators import ComponentFactory, Instantiate, Property, Provides

# ------------------------------------------------------------------------------

__version_info__ = (3, 3, 0)
__version__ = ".".join(str(x) for x in __version_info__)

# ------------------------------------------------------------------------------

entered = threading.Event()
""" Set when the slow route is entered """

release = threading.Event()
""" Lets the slow route answer """


@ComponentFactory("fastapi-bundle-router-factory")
@Provides(FastApiRouter)
@Property("_prefix", HTTP_FASTAPI_PREFIX, "/bundle")
@Instantiate("fastapi-bundle-router")
class BundleRouter:
    """
    Router with a route answering once released
    """

    def __init__(self) -> None:
        self._prefix = "/bundle"
        self._router = fastapi.APIRouter()
        self._router.add_api_route("/slow", self.slow, methods=["GET"])

    def get_router(self) -> fastapi.APIRouter:
        return self._router

    def slow(self) -> dict[str, str]:
        entered.set()
        release.wait(10)
        return {"state": "done"}
