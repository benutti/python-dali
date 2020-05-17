import asyncio
import asyncio.protocols
import logging
import struct
import dali.frame
from dali.exceptions import UnsupportedFrameTypeError, CommunicationError
from dali.gear.general import EnableDeviceType
from dali.sequences import sleep as seq_sleep
from dali.sequences import progress as seq_progress

class _callback:
    """Helper class for callback registration
    """
    def __init__(self, parent):
        self._parent = parent
        self._callbacks = {}

    class _callback_handle:
        """Callback handle

        Call unregister() to remove this callback.
        """
        def __init__(self, callback):
            self._callback = callback

        def unregister(self):
            del self._callback._callbacks[self]

    def register(self, func):
        wrapper = self._callback_handle(self)
        self._callbacks[wrapper] = func
        return wrapper

    def _invoke(self, *args):
        for func in self._callbacks.values():
            self._parent.loop.call_soon(func, self._parent, *args)

class daliserver:
    """Connect to an instance of https://github.com/onitake/daliserver
    """
    def __init__(self, hostname="localhost", port=55825,
                 reconnect_interval=1, reconnect_limit=None,
                 loop=None):
        self._log = logging.getLogger()
        self._hostname = hostname
        self._port = 55825
        self._reconnect_interval = reconnect_interval
        self._reconnect_limit = reconnect_limit
        self._reconnect_count = 0
        self._reconnect_task = None
        self.loop = loop or asyncio.get_event_loop()
        self._reader = None
        self._writer = None

        # Should the send() method raise an exception if there is a
        # problem communicating with the underlying device, or should
        # it catch the exception and keep trying?  Set this attribute
        # as required.
        self.exceptions_on_send = True

        # Acquire this lock to perform a series of commands as a
        # transaction.  While you hold the lock, you must call send()
        # with keyword argument in_transaction=True
        self.transaction_lock = asyncio.Lock(loop=self.loop)

        # Register to be called back with "connected", "disconnected"
        # or "failed" as appropriate ("failed" means the reconnect
        # limit has been reached; no more connections will be
        # attempted unless you call connect() explicitly.)
        self.connection_status_callback = _callback(self)

        # Register to be called back with bus traffic; three arguments are passed:
        # command, response, config_command_error

        # config_command_error is true if the config command has a response, or
        # if the command was not sent twice within the required time limit
        self.bus_traffic = _callback(self)

        # This event will be set when we are connected to the device
        # and cleared when the connection is lost
        self.connected = asyncio.Event(loop=self.loop)

        # firmware_version and serial may be populated on some
        # devices, and will read as None on devices that don't support
        # reading them.  They are only valid after self.connected is
        # set.
        self.firmware_version = None
        self.serial = None

    def connect(self):
        """Attempt to connect to the device.

        Attempts to open the device.  If this fails, schedules a
        reconnection attempt.

        Returns True if opening the device file succeded immediately,
        False otherwise.  NB you must still await connected.wait()
        before using the device, because there may be further
        initialisation for the driver to perform.

        If your application is (for example) a command-line script
        that wants to report failure as early as possible, you could
        do so if this returns False.
        """
        self._connect_task = asyncio.ensure_future(self._connect(), loop=self.loop)
        return True

    async def _connect(self):
        if self._reader or self._writer:
            raise CommunicationError
        self._log.debug("trying to connect to %s port %d...", self._hostname,
                        self._port)
        self._reader, self._writer = await asyncio.open_connection(
            self._hostname, self._port)
        # if error?
        self.connected.set()
        self.connection_status_callback._invoke("connected")

    def disconnect(self, reconnect=False):
        if self._reader or self._writer:
            self._writer.close()
        self._reader = None
        self._writer = None

    async def send(self, command, in_transaction=False, exceptions=None):
        """Send a DALI command and receive a response

        Sends the command.  Returns a response, or None if the command
        does not expect a response.

        If you have acquired the transaction_lock to perform a
        transaction, you must set the in_transaction keyword argument
        to True.

        This call can raise dali.exceptions.CommunicationError if
        there is a problem sending the command to the device.  If you
        prefer to wait for the device to become available again, pass
        exceptions=False or set the exceptions_on_send attribute to False.
        """
        if exceptions is None:
            exceptions = self.exceptions_on_send

        if not in_transaction:
            await self.transaction_lock.acquire()
        try:
            command_sent = False
            while not command_sent:
                try:
                    if command.devicetype != 0:
                        await self._send_raw(EnableDeviceType(command.devicetype))
                    response = await self._send_raw(command)
                    command_sent = True
                except CommunicationError:
                    if exceptions:
                        raise
            return response
        finally:
            if not in_transaction:
                self.transaction_lock.release()

    async def run_sequence(self, seq, progress=None):
        """Run a command sequence as a transaction
        """
        await self.transaction_lock.acquire()
        response = None
        try:
            while True:
                try:
                    cmd = seq.send(response)
                except StopIteration as r:
                    return r.value
                response = None
                if isinstance(cmd, seq_sleep):
                    await asyncio.sleep(cmd.delay)
                elif isinstance(cmd, seq_progress):
                    if progress:
                        progress(cmd)
                else:
                    if cmd.devicetype != 0:
                        await self._send_raw(EnableDeviceType(cmd.devicetype))
                    response = await self._send_raw(cmd)
        finally:
            self.transaction_lock.release()
            seq.close()

    async def _send_raw(self, command):
        frame = command.frame
        if len(frame) != 16:
            raise UnsupportedFrameTypeError
        await self.connected.wait()
        message = struct.pack("BB", 2, 0) + frame.pack
        self._writer.write(message)
        if command.sendtwice:
            self._writer.write(message)
        result = await self._reader.readexactly(4)
        if command.sendtwice:
            result = await self._reader.readexactly(4)
        ver, status, rval, pad = struct.unpack("BBBB", result)
        response = None

        if command.response:
            if status == 0:
                response = command.response(None)
            elif status == 1:
                response = command.response(dali.frame.BackwardFrame(rval))
            elif status == 255:
                # This is "failure" - daliserver seems to be reporting
                # this for a garbled response when several ballasts
                # reply.  It should be interpreted as "Yes".
                response = command.response(dali.frame.BackwardFrameError(255))
            else:
                raise CommunicationError("status was %d" % status)

        self.bus_traffic._invoke(command, response, False)
        return response
