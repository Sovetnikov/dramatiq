# This file is a part of Dramatiq.
#
# Copyright (C) 2017,2018 CLEARTYPE SRL <bogdan@cleartype.io>
#
# Dramatiq is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Dramatiq is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import sys
import time
from pydoc import locate

import math
import redis

from ..backend import DEFAULT_TIMEOUT, ResultBackend, ResultMissing, ResultTimeout
from dramatiq.logging import get_logger

class RedisBackend(ResultBackend):
    """A result backend for Redis_.  This is the recommended result
    backend as waiting for a result is resource efficient.

    Parameters:
      namespace(str): A string with which to prefix result keys.
      encoder(Encoder): The encoder to use when storing and retrieving
        result data.  Defaults to :class:`.JSONEncoder`.
      client(Redis): An optional client.  If this is passed,
        then all other parameters are ignored.
      url(str): An optional connection URL.  If both a URL and
        connection paramters are provided, the URL is used.
      **parameters(dict): Connection parameters are passed directly
        to :class:`redis.Redis`.

    .. _redis: https://redis.io
    """

    def __init__(self, *, namespace="dramatiq-results", encoder=None, client=None, url=None, **parameters):
        super().__init__(namespace=namespace, encoder=encoder)
        self.logger = get_logger(__name__, type(self))

        if url:
            parameters["connection_pool"] = redis.ConnectionPool.from_url(url)

        # TODO: Replace usages of StrictRedis (redis-py 2.x) with Redis in Dramatiq 2.0.
        self.client = client or redis.StrictRedis(**parameters)

    def get_result(self, message, *, block=False, timeout=None, propagate=True):
        """Get a result from the backend.

        Warning:
          Sub-second timeouts are not respected by this backend.

        Parameters:
          message(Message)
          block(bool): Whether or not to block until a result is set.
          timeout(int): The maximum amount of time, in ms, to wait for
            a result when block is True.  Defaults to 10 seconds.
          propagate(bool): Whether or not to propagate Exception if actor execution failed

        Raises:
          ResultMissing: When block is False and the result isn't set.
          ResultTimeout: When waiting for a result times out.

        Returns:
          object: The result.
        """
        if timeout is None:
            timeout = DEFAULT_TIMEOUT

        message_key = self.build_message_key(message)
        if block:
            timeout = int(timeout / 1000)
            if timeout == 0:
                data = self.client.rpoplpush(message_key, message_key)
            else:
                data = self.client.brpoplpush(message_key, message_key, timeout)

            if data is None:
                raise ResultTimeout(message)

        else:
            data = self.client.lindex(message_key, 0)
            if data is None:
                raise ResultMissing(message)

        data = self.encoder.decode(data)
        if 'actor_exception' in data:
            if propagate:
                self._raise_exception(data['actor_exception'])
            else:
                return self._deserialize_exception(data['actor_exception'])
        if 'actor_result' in data:
            return data['actor_result']
        return data

    def _store(self, message_key, result, ttl):
        with self.client.pipeline() as pipe:
            pipe.delete(message_key)
            pipe.lpush(message_key, self.encoder.encode(dict(actor_result=result)))
            pipe.pexpire(message_key, ttl)
            pipe.execute()

    _exception_token = 'exc'

    def _serialize_exception(self, exc):
        return {'type': type(exc).__name__,
                'args': exc.args,
                'mod': type(exc).__module__}

    def _deserialize_exception(self, serialized):
        mod = serialized.get('mod')
        t = serialized['type']
        if mod is None:
            cls = locate(serialized['type'])
        else:
            try:
                cls = getattr(sys.modules[mod], t)
            except KeyError:
                cls = locate(serialized['type'])

        args = serialized['args']
        return cls(*args if isinstance(args, list) else args)

    def _raise_exception(self, serialized):
        raise self._deserialize_exception(serialized)

    def _store_exception(self, message_key, exception, ttl):
        with self.client.pipeline() as pipe:
            pipe.delete(message_key)
            pipe.lpush(message_key, self.encoder.encode(dict(actor_exception=self._serialize_exception(exception))))
            pipe.pexpire(message_key, ttl)
            pipe.execute()

    def get_any_results(self, messages, *, block=False, timeout=None, propagate=True, with_task=False):
        if block:
            if timeout is None:
                timeout = DEFAULT_TIMEOUT
            deadline = time.monotonic() + (timeout / 1000)
        else:
            timeout = 0
            deadline = time.monotonic()

        message_keys = {self.build_message_key(message): message for message in messages}

        while message_keys:
            if block:
                # Block until timeout occurs
                until_deadline = math.floor(max(1, deadline - time.monotonic()))
                self.logger.debug('Getting any results blocking, until deadline %s' % until_deadline)
                found = self.client.brpop(message_keys.keys(), timeout=until_deadline)
            else:
                # get any existing result as fast as possible
                # zero timeout will block indefinitely or return empty result (on practice)
                self.logger.debug('Getting any results non-blocking')
                found = self.client.brpop(message_keys.keys(), timeout=1)

            if found is None:
                self.logger.debug('Empty result')
                if block:
                    raise ResultTimeout('No any results')
                else:
                    raise ResultMissing('No any results')
            found_key, data = found
            # put result back to redis for future usage, it's ok to postpone key decode
            self.client.lpush(found_key, data)

            found_key = found_key.decode()
            message = message_keys.pop(found_key)
            data = self.encoder.decode(data)

            if 'actor_exception' in data:
                if propagate:
                    self.logger.debug('Propagating actor exception')
                    self._raise_exception(data['actor_exception'])
                else:
                    self.logger.debug('Returning actor exception')
                    result = self._deserialize_exception(data['actor_exception'])
            elif 'actor_result' in data:
                result = data['actor_result']
            else:
                result = data

            if with_task:
                yield result, message
            else:
                yield result

            if not message_keys:
                self.logger.debug('Found all results')
                # Successful
                return

            if block:
                # In non-blocking way we iterate over results until no any results ready
                # in other words we wait until found_key is None with smallest possible timeout
                pass
            else:
                # In blocking way we wait until deadline reached
                if time.monotonic() >= deadline:
                    self.logger.debug('Deadline reached')
                    raise ResultTimeout('No any results')
