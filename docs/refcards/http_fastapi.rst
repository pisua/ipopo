.. _refcard_http_fastapi:
.. module:: pelix.http.fastapi

FastAPI HTTP Service
====================

The FastAPI HTTP service is an implementation of the :ref:`HTTP service <refcard_http>`
based on `FastAPI <https://fastapi.tiangolo.com/>`_, served by
`uvicorn <https://uvicorn.dev/>`_.

It handles the same servlets as the :ref:`asynchronous HTTP service <refcard_http_async>`
(synchronous, asynchronous and WebSocket ones) and, in addition, serves the
FastAPI routers that components provide as services: a router is mounted when
its service is registered and removed when it is unregistered, and the OpenAPI
schema of the application follows. Each component brings its own routes, and
they come and go with it, as servlets do.

.. important:: The FastAPI HTTP service requires the ``fastapi``, ``uvicorn``
   and ``websockets`` packages, installed by the ``fastapi`` extra:
   ``pip install ipopo[fastapi]``.

.. versionadded:: 3.3.0


Configuration properties
------------------------

The FastAPI HTTP service supports the properties of the basic HTTP service,
described in :ref:`its reference card <refcard_http>` (address, port, TLS,
``pelix.http.allowed_hosts``, ``pelix.http.cors.*``, ``pelix.http.auth.*``,
``pelix.http.proxy.trusted``, sessions, ...), plus the following ones:

=============================== ================= ========================================
Property                        Default           Description
=============================== ================= ========================================
pelix.http.fastapi.title        Pelix             Title of the API in its OpenAPI schema
pelix.http.fastapi.openapi_url  ``/openapi.json`` Path of the OpenAPI schema. Empty to
                                                  disable it, and the documentation page
                                                  with it
pelix.http.fastapi.docs_url     ``/docs``         Path of the Swagger UI page, empty to
                                                  disable it. There is no ReDoc page
pelix.http.fastapi.root_path    (empty)           Path prefix stripped by a reverse proxy
                                                  in front of the service: the routes,
                                                  the documentation and the OpenAPI
                                                  schema then give the URLs of the
                                                  clients
=============================== ================= ========================================

Those four properties can be updated while the service runs (e.g. by the
ConfigurationAdmin bridge): the application is rebuilt at once.

``pelix.http.socket_timeout`` is accepted but not applied, as with the
asynchronous service: the timeouts of the connections are left to uvicorn.


Instantiation
-------------

The factory provided by the ``pelix.http.fastapi`` bundle is named
``pelix.http.service.fastapi.factory`` (``pelix.http.FACTORY_HTTP_FASTAPI``):

.. code-block:: python

    from pelix.framework import create_framework
    from pelix.http import FACTORY_HTTP_FASTAPI
    from pelix.ipopo.constants import use_ipopo

    framework = create_framework(["pelix.ipopo.core", "pelix.http.fastapi"])
    framework.start()

    with use_ipopo(framework.get_bundle_context()) as ipopo:
        ipopo.instantiate(
            FACTORY_HTTP_FASTAPI,
            "http-server",
            {"pelix.http.address": "localhost", "pelix.http.port": 9000},
        )

uvicorn is started when the component is validated, and stopped when it is
invalidated. The socket is bound before the component is valid: an address
already in use makes the component fail at once.


Routers
-------

A router is a service providing the ``pelix.http.fastapi.router``
specification (:class:`FastApiRouter`): an object whose ``get_router()``
method returns a ``fastapi.APIRouter``. A plain ``APIRouter`` can also be
registered as is with this specification. The ``pelix.http.fastapi.prefix``
service property gives the path prefix of its routes.

.. code-block:: python

    import fastapi

    from pelix.http.fastapi import HTTP_FASTAPI_PREFIX, FastApiRouter
    from pelix.ipopo.decorators import ComponentFactory, Instantiate, Property, Provides, Requires

    @ComponentFactory("items-api-factory")
    @Provides(FastApiRouter)
    @Property("_prefix", HTTP_FASTAPI_PREFIX, "/items")
    @Requires("_store", "sample.items.store")
    @Instantiate("items-api")
    class ItemsApi:
        def __init__(self) -> None:
            self._store = None
            self._router = fastapi.APIRouter(tags=["items"])
            self._router.add_api_route("/{item_id}", self.get_item, methods=["GET"])

        def get_router(self) -> fastapi.APIRouter:
            return self._router

        def get_item(self, item_id: int) -> dict:
            return self._store.get(item_id)

The routes of ``ItemsApi`` are served while the component is valid: when its
store goes away, the component is invalidated, its service unregistered, and
``/items/...`` answers 404 again. Changing ``_prefix`` moves the routes.

* The routers are mounted in the order of their service ranking, then of their
  service ID: on a path two routers serve, the best ranked one answers.
* Each time the set of routers changes (registration, unregistration, update
  of the properties of a router service), the service builds a new FastAPI
  application with the routers of the moment and swaps it in. ``get_router()``
  is called again for each router. The requests being handled end with the
  application they started with; the next ones get the new one.
* The OpenAPI schema and the documentation page are those of the current
  application: they always describe the routes being served.
* A router whose ``get_router()`` raises is logged and left out; the other
  ones are served.
* The life span events of the routers are not run: the life cycle of a
  router is the one of its component (``@Validate`` / ``@Invalidate``).


Handling of a request
---------------------

Every request, whether it goes to a servlet or to a route, first goes through
the checks of the HTTP service: ``Host`` header (``pelix.http.allowed_hosts``),
``Origin`` header (``pelix.http.cors.enforce``), CORS preflight requests,
authentication (``pelix.http.auth.*`` and the sessions). Then:

#. the servlet registered on the longest prefix of the path handles it, as with
   the other HTTP services;
#. otherwise, the FastAPI application handles it;
#. if no route matches either, the 404 page of the service is sent (see
   ``pelix.http.error.pages``).

Servlets take precedence: a servlet registered on ``/`` handles every request,
leaving nothing to the routes.

For the routes:

* The properties of the HTTP service apply: CORS policy of the bound
  ``CorsHandler`` service, ``pelix.http.auth.required``,
  ``pelix.http.max_body_size`` (a bigger body is answered with a 413 error
  code, whether its size is announced or not). The per-servlet overrides of
  those properties don't exist for routers.
* The route is called as the subject of the request (see
  ``pelix.security.get_current_subject()``), synchronous routes included: they
  run in a thread pool, in a copy of the context of the request. A route
  raising ``AuthenticationRequired`` or ``AuthenticationFailed`` is answered
  with a 401 error code and the challenges of the HTTP authenticators, a route
  raising ``AccessDenied`` with a 403 one.
* ``request.client`` and ``request.url.scheme`` are those told by the trusted
  proxies (``pelix.http.proxy.trusted``): uvicorn's own handling of the
  ``X-Forwarded-*`` headers is disabled.
* An error raised by a route is answered with a 500 error code by FastAPI and
  logged by the HTTP service. With ``pelix.http.debug``, FastAPI sends the
  stack trace.


WebSockets
----------

WebSocket handlers (``pelix.http.websocket.handler`` services, see the
:ref:`asynchronous HTTP service <refcard_http_async>`) work as with the
asynchronous service, on top of Starlette's WebSocket, and so do the
WebSocket routes of the routers (``@router.websocket(...)``).

* The upgrade requests go through the checks of the service: a refused
  upgrade is answered with an HTTP error code.
* The messages of a handler are limited to 4 MiB, or to its
  ``pelix.http.max_body_size`` property, as with the asynchronous service;
  uvicorn refuses any message bigger than 16 MiB, for handlers and routes.
* When the service stops, the open sessions of the handlers are closed with
  the code 1001 and ``ws_close()`` is called; uvicorn closes those of the routes
  with the code 1012.
* When a handler closes a session itself, ``ws_close()`` is given the code it
  sent, not the one of the answer of the client.


Threads and event loops
-----------------------

uvicorn runs its own event loop, in a daemon thread started by the component.
The asynchronous servlets, the WebSocket handlers and the coroutine routes run
in that loop; the synchronous servlets and routes run in the thread pool of
anyio (the one of FastAPI).

This loop is not the one of other components: an object bound to an event loop
(an ``asyncio.Lock``, a queue, a client session...) created elsewhere, e.g. in
the coroutine callbacks of a component, must not be used from a route, and the
other way round. From another thread, use ``WebSocketSession.submit_text()``
and the like, or ``asyncio.run_coroutine_threadsafe()``. A coroutine calling
blocking code blocks all the requests of the service.


Stopping the service
--------------------

The stop is bounded: uvicorn stops accepting connections, waits up to
5 seconds for the requests being handled, then cancels them. If its thread
still doesn't end, e.g. because a coroutine blocks the event loop, the
component logs a warning and leaves it behind (it is a daemon thread): the stop
of the framework is not held.

A synchronous route or servlet can't be interrupted: its thread of the pool
goes on until it returns.


API
---

.. autoclass:: FastApiRouter
   :members: get_router

.. autodata:: HTTP_FASTAPI_ROUTER
.. autodata:: HTTP_FASTAPI_PREFIX
.. autodata:: HTTP_FASTAPI_TITLE
.. autodata:: HTTP_FASTAPI_OPENAPI_URL
.. autodata:: HTTP_FASTAPI_DOCS_URL
.. autodata:: HTTP_FASTAPI_ROOT_PATH
