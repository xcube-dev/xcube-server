# The MIT License (MIT)
# Copyright (c) 2018 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import asyncio
import logging
import os.path
import signal
import sys
import time
import traceback
from datetime import datetime
from typing import Callable, Optional, Tuple

import tornado.options
from tornado.ioloop import IOLoop
from tornado.log import enable_pretty_logging
from tornado.web import RequestHandler, Application

from .common import exception_to_json

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

_LOG = logging.getLogger('xcube-wmts')

ApplicationFactory = Callable[[], Application]


class Service:
    """
    A web service that provides a remote API to some application.
    """

    def __init__(self):
        self.name = None
        self.application = None
        self.server = None
        self.auto_stop_enabled = None
        self.update_timer = None
        self.update_period = None
        self.service_info_file = None
        self.service_info = None

    @classmethod
    def get_service(cls, application: Application) -> 'Service':
        """
        Retrieves the associated WebAPI service from the given Tornado web application.

        :param application: The Tornado web application
        :return: The WebAPI instance, or None
        """
        return application.service if application and hasattr(application, 'service') else None

    def start(self,
              application: Application,
              log_file_prefix: str = 'xcube-wmts.log',
              log_to_stderr: bool = False,
              update_period: float = 1.0,
              port: int = 8080,
              address: str = 'localhost') -> dict:

        """
        Start a WebAPI service.

        The *service_info_file*, if given, represents the service in the filesystem, similar to
        the ``/var/run/`` directory on Linux systems.

        If the service file exist and its information is compatible with the requested *port*, *address*, *caller*, then
        this function simply returns without taking any other actions.

        :param application: The Tornado web application
        :param log_file_prefix: Log file prefix, default is "xcube_wmts.log"
        :param log_to_stderr: Whether logging should be shown on stderr
        :param update_period: if not-None, time of idleness in seconds before service is updated
        :param port: the port number
        :param address: the address
        :return: service information dictionary
        """

        log_dir = os.path.dirname(log_file_prefix)
        if log_dir and not os.path.isdir(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        options = tornado.options.options
        options.log_file_prefix = log_file_prefix or 'xcube-wmts.log'
        options.log_to_stderr = log_to_stderr
        enable_pretty_logging()

        self.update_period = update_period
        self.update_timer = None
        self.service_info = dict(port=port,
                                 address=address,
                                 started=datetime.now().isoformat(sep=' '),
                                 pid=os.getpid())

        application.service = self
        application.time_of_last_activity = time.clock()
        self.application = application

        print(f'xcube-wmts: started service, listening on {address}:{port}')

        self.server = application.listen(port, address=address or 'localhost')
        # Ensure we have the same event loop in all threads
        asyncio.set_event_loop_policy(_GlobalEventLoopPolicy(asyncio.get_event_loop()))
        # Register handlers for common termination signals
        signal.signal(signal.SIGINT, self._sig_handler)
        signal.signal(signal.SIGTERM, self._sig_handler)
        self._install_update_check()
        IOLoop.current().start()
        return self.service_info

    def shut_down(self, kill=False):
        """
        Stops the Tornado web server.
        """
        if kill:
            sys.exit(0)
        else:
            IOLoop.current().add_callback(self._on_shut_down)

    def _on_shut_down(self):

        print('xcube-wmts: shutting down service...')

        # noinspection PyUnresolvedReferences,PyBroadException
        try:
            self.update_timer.cancel()
        except Exception:
            pass

        if self.server:
            self.server.stop()
            self.server = None

        IOLoop.current().stop()

    # noinspection PyUnusedLocal
    def _sig_handler(self, sig, frame):
        print(f'xcube-wmts: caught signal {sig}')
        IOLoop.current().add_callback_from_signal(self._on_shut_down)

    def _install_update_check(self):
        IOLoop.current().call_later(self.update_period, self._check_for_updates)

    def _check_for_updates(self):
        # noinspection PyUnresolvedReferences
        _LOG.info('')
        self._install_update_check()


# noinspection PyAbstractClass
class WebAPIRequestHandler(RequestHandler):
    """
    Base class for REST API requests.
    All JSON REST responses should have same structure, namely a dictionary as follows:

    {
       "status": "ok" | "error",
       "error": optional error-details,
       "content": optional content, if status "ok"
    }

    See methods write_status_ok() and write_status_error().
    """

    def __init__(self, application, request, **kwargs):
        super(WebAPIRequestHandler, self).__init__(application, request, **kwargs)

    @property
    def service(self) -> Service:
        return Service.get_service(self.application)

    @classmethod
    def to_int(cls, name: str, value: str) -> int:
        """
        Convert str value to int.
        :param name: Name of the value
        :param value: The string value
        :return: The int value
        :raise: WebAPIRequestError
        """
        if value is None:
            raise WebAPIRequestError('%s must be an integer, but was None' % name)
        try:
            return int(value)
        except ValueError as e:
            raise WebAPIRequestError('%s must be an integer, but was "%s"' % (name, value)) from e

    @classmethod
    def to_int_tuple(cls, name: str, value: str) -> Tuple[int, ...]:
        """
        Convert str value to int.
        :param name: Name of the value
        :param value: The string value
        :return: The int value
        :raise: WebAPIRequestError
        """
        if value is None:
            raise WebAPIRequestError('%s must be a list of integers, but was None' % name)
        try:
            return tuple(map(int, value.split(','))) if value else ()
        except ValueError as e:
            raise WebAPIRequestError('%s must be a list of integers, but was "%s"' % (name, value)) from e

    @classmethod
    def to_float(cls, name: str, value: str) -> float:
        """
        Convert str value to float.
        :param name: Name of the value
        :param value: The string value
        :return: The float value
        :raise: WebAPIRequestError
        """
        if value is None:
            raise WebAPIRequestError('%s must be a number, but was None' % name)
        try:
            return float(value)
        except ValueError as e:
            raise WebAPIRequestError('%s must be a number, but was "%s"' % (name, value)) from e

    def get_query_argument_int(self, name: str, default: int) -> Optional[int]:
        """
        Get query argument of type int.
        :param name: Query argument name
        :param default: Default value.
        :return: int value
        :raise: WebAPIRequestError
        """
        value = self.get_query_argument(name, default=None)
        return self.to_int(name, value) if value is not None else default

    def get_query_argument_int_tuple(self, name: str, default: Tuple[int, ...]) -> Optional[Tuple[int, ...]]:
        """
        Get query argument of type int list.
        :param name: Query argument name
        :param default: Default value.
        :return: int list value
        :raise: WebAPIRequestError
        """
        value = self.get_query_argument(name, default=None)
        return self.to_int_tuple(name, value) if value is not None else default

    def get_query_argument_float(self, name: str, default: float) -> Optional[float]:
        """
        Get query argument of type float.
        :param name: Query argument name
        :param default: Default value.
        :return: float value
        :raise: WebAPIRequestError
        """
        value = self.get_query_argument(name, default=None)
        return self.to_float(name, value) if value is not None else default

    def on_finish(self):
        """
        Store time of last activity so we can measure time of inactivity and then optionally auto-exit.
        """
        self.application.time_of_last_activity = time.clock()

    def write_status_ok(self, content: object = None):
        self.write(dict(status='ok', content=content))

    def write_status_error(self, message: str = None, exc_info=None):
        if message is not None:
            _LOG.error(message)
        if exc_info is not None:
            _LOG.info(''.join(traceback.format_exception(exc_info[0], exc_info[1], exc_info[2])))
        self.write(self._to_status_error(message=message, exc_info=exc_info))

    @classmethod
    def _to_status_error(cls, message: str = None, exc_info=None):
        error_data = None
        if exc_info is not None:
            message = message or str(exc_info[1])
            error_data = exception_to_json(exc_info)
        if message:
            if error_data:
                return dict(status='error', error=dict(message=message, data=error_data))
            else:
                return dict(status='error', error=dict(message=message))
        return dict(status='error')


class _GlobalEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    """
    Event loop policy that has one fixed global loop for all threads.

    We use it for the following reason: As of Tornado 5 IOLoop.current() no longer has
    a single global instance. It is a thread-local instance, but only on the main thread.
    Other threads have no IOLoop instance by default.

    _GlobalEventLoopPolicy is a fix that allows us to access the same IOLoop
    in all threads.

    Usage::

        asyncio.set_event_loop_policy(_GlobalEventLoopPolicy(asyncio.get_event_loop()))

    """

    def __init__(self, global_loop):
        super().__init__()
        self._global_loop = global_loop

    def get_event_loop(self):
        return self._global_loop


class WebAPIError(Exception):
    """
    WepAPI error base class.
    Exceptions thrown by the Cate WebAPI.
    """

    def __init__(self, message: str):
        super().__init__(message)

    @property
    def cause(self):
        return self.__cause__


class WebAPIServiceError(WebAPIError):
    """
    Exception which may be raised by the WebAPI service class.
    """


class WebAPIRequestError(WebAPIError):
    """
    Exception which may be raised and handled(!) by WebAPI service requests.
    """


def url_pattern(pattern: str):
    """
    Convert a string *pattern* where any occurrences of ``{{NAME}}`` are replaced by an equivalent
    regex expression which will assign matching character groups to NAME. Characters match until
    one of the RFC 2396 reserved characters is found or the end of the *pattern* is reached.

    The function can be used to map URLs patterns to request handlers as desired by the Tornado web server, see
    http://www.tornadoweb.org/en/stable/web.html

    RFC 2396 Uniform Resource Identifiers (URI): Generic Syntax lists
    the following reserved characters::

        reserved    = ";" | "/" | "?" | ":" | "@" | "&" | "=" | "+" | "$" | ","

    :param pattern: URL pattern
    :return: equivalent regex pattern
    :raise ValueError: if *pattern* is invalid
    """
    name_pattern = '(?P<%s>[^\;\/\?\:\@\&\=\+\$\,]+)'
    reg_expr = ''
    pos = 0
    while True:
        pos1 = pattern.find('{{', pos)
        if pos1 >= 0:
            pos2 = pattern.find('}}', pos1 + 2)
            if pos2 > pos1:
                name = pattern[pos1 + 2:pos2]
                if not name.isidentifier():
                    raise ValueError('name in {{name}} must be a valid identifier, but got "%s"' % name)
                reg_expr += pattern[pos:pos1] + (name_pattern % name)
                pos = pos2 + 2
            else:
                raise ValueError('no matching "}}" after "{{" in "%s"' % pattern)

        else:
            reg_expr += pattern[pos:]
            break
    return reg_expr
