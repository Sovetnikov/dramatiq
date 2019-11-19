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

from ..backend import Missing, ResultBackend


class StubBackend(ResultBackend):
    """An in-memory result backend.  For use in unit tests.

    Parameters:
      namespace(str): A string with which to prefix result keys.
      encoder(Encoder): The encoder to use when storing and retrieving
        result data.  Defaults to :class:`.JSONEncoder`.
    """

    results = {}

    def _get(self, message_key):
        data, expiration = self.results.get(message_key, (None, None))
        if data is not None and time.monotonic() < expiration:
            data = self.encoder.decode(data)
            if 'actor_exception' in data:
                self._raise_exception(data['actor_exception'])
            if 'actor_result' in data:
                return data['actor_result']
            return data
        return Missing

    def _store(self, message_key, result, ttl):
        result_data = self.encoder.encode(dict(actor_result=result))
        expiration = time.monotonic() + int(ttl / 1000)
        self.results[message_key] = (result_data, expiration)

    def _serialize_exception(self, exc):
        return {'type': type(exc).__name__,
                'args': exc.args,
                'mod': type(exc).__module__}

    def _raise_exception(self, serialized):
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
        exc = cls(*args if isinstance(args, list) else args)
        raise exc

    def _store_exception(self, message_key, exception, ttl):
        result_data = self.encoder.encode(dict(actor_exception=self._serialize_exception(exception)))
        expiration = time.monotonic() + int(ttl / 1000)
        self.results[message_key] = (result_data, expiration)
