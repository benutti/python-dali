from enum import Enum, auto
from collections import namedtuple

from dali.exceptions import ResponseError
from dali.gear.general import ReadMemoryLocation, DTR0, DTR1, \
    WriteMemoryLocationNoReply


class MemoryType(Enum):
    ROM = auto()       # ROM
    RAM_RO = auto()    # RAM-RO
    RAM_RW = auto()    # RAM-RW
    NVM_RO = auto()    # NVM-RO
    NVM_RW = auto()    # NVM-RW
    NVM_RW_L = auto()  # NVM-RW (lockable)
    NVM_RW_P = auto()  # NVM-RW (protectable — vendor-specific mechanism)


class MemoryLocationNotImplemented(Exception):
    pass


class MemoryBank:
    class MemoryLocationOverlap(Exception):
        pass

    class LatchingNotSupported(Exception):
        pass

    MemoryBankEntry = namedtuple(
        'MemoryBankEntry', ['memory_location', 'memory_value'])

    def __init__(self, address, last_address, has_lock=False, has_latch=False):
        """Declares a memory bank at a given address
        """
        self.__address = address
        self.locations = {x: None for x in range(0xff)}
        self.values = []

        # add value for last addressable location
        class LastAddress(NumericValue):
            bank = self
            locations = (MemoryLocation(
                0x00, default=last_address,
                reset=last_address, type_=MemoryType.ROM),)
        self.LastAddress = LastAddress

        if has_lock or has_latch:
            class LockByte(MemoryValue):
                bank = self
                lock = has_lock
                latch = has_latch
                locations = (MemoryLocation(
                    0x02, reset=0xff, default=0xff, type_=MemoryType.RAM_RW),)

            self.LockByte = LockByte
        else:
            self.LockByte = None

    @property
    def address(self):
        return self.__address

    def add_memory_value(self, memory_value):
        self.values.append(memory_value)
        for location in memory_value.locations:
            if self.locations[location.address]:
                raise self.MemoryLocationOverlap(
                    f'Overlapping MemoryLocation at address {location.address}')
            self.locations[location.address] = self.MemoryBankEntry(
                location, memory_value)

    def last_address(self, addr):
        """Sequence that returns the last available address in this bank
        """
        la = yield from self.LastAddress.read(addr)
        return la

    def read_all(self, addr):
        last_address = yield from self.LastAddress.read(addr)
        # Bank 0 has a useful value at address 0x02; all other banks
        # use this for the lock/latch byte
        start_address = 0x02 if self.address == 0 else 0x03
        # don't need to set DTR1, as we just did that in last_address()
        yield DTR0(start_address)
        raw_data = [None] * start_address
        for loc in range(start_address, last_address + 1):
            r = yield ReadMemoryLocation(addr)
            if r.raw_value is not None:
                if r.raw_value.error:
                    raise ResponseError(
                        f"Framing error while reading memory bank "
                        f"{self.address} location {loc}")
                raw_data.append(r.raw_value.as_integer)
            else:
                raw_data.append(None)
        result = {}
        for memory_value in self.values:
            try:
                r = memory_value.from_list(raw_data)
            except MemoryLocationNotImplemented:
                pass
            else:
                result[memory_value] = r
        return result

    def latch(self, addr):
        """(Re-)latch all memory locations of this bank.

        Raises LatchingNotSupported exception if bank does not support
        latching.
        """
        if self.LockByte and self.LockByte.latch:
            yield DTR1(self.address)
            yield DTR0(self.LockByte.locations[0].address)
            yield WriteMemoryLocationNoReply(0xAA, addr)
        else:
            raise self.LatchingNotSupported(f'Latching not supported for {str(self)}.')

    def is_locked(self, addr):
        """Check whether this bank is locked
        """
        if self.LockByte and self.LockByte.lock:
            r = yield from self.LockByte.read(addr)
            return r[0] != 0x55
        else:
            return False

    def factory_default_contents(self):
        """Return factory default contents for known memory locations
        """
        for address in range(0xff):
            loc = self.locations[address]
            yield loc.memory_location.default if loc else None

    def __repr__(self):
        return f'MemoryBank(address={self.address}, ' \
            f'has_lock={bool(self.LockByte and self.LockByte.lock)}, ' \
            f'has_latch={bool(self.LockByte and self.LockByte.latch)})'


MemoryLocation = namedtuple(
    'MemoryLocation', ['address', 'default', 'reset', 'type_'],
    defaults=[None] * 3)


def MemoryRange(start, end, **kwargs):
    return tuple(
        MemoryLocation(address, **kwargs) for address in range(start, end + 1)
    )


class _RegisterMemoryValue(type):
    """Metaclass to register new MemoryValue classes
    """
    def __init__(cls, name, bases, attrs):
        # cls is the new MemoryValue subclass; it already exists, it's
        # being initialised
        if hasattr(cls, 'locations'):
            if not hasattr(cls, 'bank'):
                raise Exception(
                    f"MemoryValue subclass {name} missing 'bank' attribute")
            cls.name = name
            # Shorthand: locations can be a single MemoryLoction instance
            if isinstance(cls.locations, MemoryLocation):
                cls.locations = (cls.locations, )
            cls.bank.add_memory_value(cls)

    def __str__(cls):
        if hasattr(cls, 'name'):
            return cls.name
        return super().__str__()


class FlagValue(Enum):
    Invalid = "Invalid"  # Memory value not valid according to the standard
    MASK = "MASK"        # Memory value not implemented
    TMASK = "TMASK"      # Memory value temporarily unavailable


class MemoryValue(metaclass=_RegisterMemoryValue):
    """A group of memory locations that together represent a value

    This is an abstract base class. Concrete classes should declare
    the 'bank' and 'locations' attributes.

    'bank' must be a MemoryBank instance

    'locations' must be a sequence of MemoryLocation instances in the
    order required by the _to_value() method. It is most efficient if
    these memory locations are contiguous increasing in address.
    """
    # Is MASK a possible value?  MASK is part of the DiiA extended
    # memory bank specifications, parts 252 and 253.
    mask_supported = False

    # Is TMASK a possible value?  TMASK is part of the DiiA extended
    # memory bank specifications, parts 252 and 253.
    tmask_supported = False

    # Should the value be treated as signed when checking for MASK
    # and/or TMASK?
    signed = False

    @classmethod
    def raw_to_value(cls, raw):
        """Converts raw bytes to the wanted result

        This method should only be called with valid values for 'raw'.
        Checks for invalid and special values should be performed first.
        """
        return raw

    @classmethod
    def is_valid(cls, raw):
        """Check whether raw bytes are valid for this memory value"""
        return True

    @classmethod
    def check_raw(cls, raw):
        """Check for invalid or special patterns in raw bytes

        Returns None if no invalid or special patterns were found, otherwise
        returns the appropriate FlagValue
        """
        if cls.mask_supported:
            if cls.signed:
                mask = (pow(2, len(raw) * 8 - 1) - 1).to_bytes(
                    len(raw), 'big', signed=True)
            else:
                mask = (pow(2, len(raw) * 8) - 1).to_bytes(
                    len(raw), 'big')
            if raw == mask:
                return FlagValue.MASK
        if cls.tmask_supported:
            if cls.signed:
                tmask = (pow(2, len(raw) * 8 - 1) - 2).to_bytes(
                    len(raw), 'big', signed=True)
            else:
                tmask = (pow(2, len(raw) * 8) - 2).to_bytes(
                    len(raw), 'big')
            if raw == tmask:
                return FlagValue.TMASK
        if not cls.is_valid(raw):
            return FlagValue.Invalid

    @classmethod
    def read_raw(cls, addr):
        """Read the value from the bus unit without interpretation

        Raises MemoryLocationNotImplemented if the device does
        not respond, e.g. the location is not implemented.

        Returns a bytes() object with the same length as cls.locations
        """
        result = []
        dtr0 = None
        yield DTR1(cls.bank.address)
        for location in cls.locations:
            # select correct memory location
            if location.address != dtr0:
                dtr0 = location.address
                yield DTR0(location.address)
            # read back value of the memory location
            r = yield ReadMemoryLocation(addr)
            # increase DTR0 to reflect the internal state of the driver
            dtr0 = min(dtr0 + 1, 255)
            if r.raw_value is None:
                raise MemoryLocationNotImplemented(
                    f'Bus unit at address "{str(addr)}" does not implement '
                    f'memory bank {cls.bank.address} {str(location)}.')
            if r.raw_value.error:
                raise ResponseError(
                    f'Framing error in response from bus unit at address '
                    f'"{str(addr)}" while reading '
                    f'memory bank {cls.bank.address} {str(location)}.')
            result.append(r.raw_value.as_integer)
        return bytes(result)

    @classmethod
    def read(cls, addr):
        """Read the value from the bus unit

        Raises MemoryLocationNotImplemented if the device does not respond.

        Returns an interpreted value if possible, otherwise a FlagValue
        """
        raw = yield from cls.read_raw(addr)
        return cls.check_raw(raw) or cls.raw_to_value(raw)

    @classmethod
    def from_list(cls, list_):
        """Extracts the value from a list containing all values of the memory bank.
        """
        raw = []
        for location in cls.locations:
            try:
                r = list_[location.address]
            except IndexError:
                raise MemoryLocationNotImplemented(f'List is missing memory location {str(location)}.')
            if r is None:
                raise MemoryLocationNotImplemented(f'List is missing memory location {str(location)}.')
            raw.append(r)
        raw = bytes(raw)
        return cls.check_raw(raw) or cls.raw_to_value(raw)

    @classmethod
    def is_addressable(cls, addr):
        """Checks whether this value is addressable

        Queries the value of the last addressable memory location for
        this memory bank
        """
        last_location = max(loc.address for loc in cls.locations)
        try:
            last_address = yield from cls.bank.last_address(addr)
        except MemoryLocationNotImplemented:
            return False
        return last_address >= last_location

    @classmethod
    def is_locked(cls, addr):
        """Checks whether this value is locked
        """
        if cls.locations[0].type_ == MemoryType.NVM_RW_L:
            locked = yield from cls.bank.is_locked(addr)
            return locked
        else:
            return False


class NumericValue(MemoryValue):
    """Numeric value stored with MSB at the first location
    """
    unit = ''
    min_value = None
    max_value = None

    @classmethod
    def raw_to_value(cls, raw):
        return int.from_bytes(raw, 'big', signed=cls.signed)

    @classmethod
    def is_valid(cls, raw):
        trial = int.from_bytes(raw, 'big', signed=cls.signed)
        if cls.min_value is not None:
            if trial < cls.min_value:
                return False
        if cls.max_value is not None:
            if trial > cls.max_value:
                return False
        return True


class FixedScaleNumericValue(NumericValue):
    """Numeric value with fixed scaling factor
    """
    scaling_factor = 1

    @classmethod
    def raw_to_value(cls, raw):
        return cls.scaling_factor * super().raw_to_value(raw)


class StringValue(MemoryValue):
    @classmethod
    def raw_to_value(cls, raw):
        result = ''
        for value in raw:
            if value == 0:
                break  # string is Null terminated
            else:
                result += chr(value)
        return result


class BinaryValue(MemoryValue):
    @classmethod
    def raw_to_value(cls, raw):
        if raw[0] == 1:
            return True
        else:
            return False

    @classmethod
    def is_valid(cls, raw):
        return raw[0] in (0, 1)


class TemperatureValue(NumericValue):
    unit = '°C'
    offset = 60

    @classmethod
    def raw_to_value(cls, raw):
        return int.from_bytes(raw, 'big') - cls.offset


class VersionNumberValue(NumericValue):
    """A version number

    When encoded into a byte, IEC 62386 part 102 section 4.2 states
    that a version shall be in the format "x.y", where the major
    version number x is in the range 0..62 and the minor version
    number y is in the range 0..2. The major version number is placed
    in bits 7:2 and the minor version number is in bits 1:0. Part 102
    section 9.10.6 states that the value 0xff is reserved for "not
    implemented" when used for Part 102 and 103 versions in memory bank 0.

    When encoded into two bytes, the major version number is in the
    first byte and the minor version number is in the second byte.
    """
    @classmethod
    def raw_to_value(cls, raw):
        if len(raw) == 1:
            n = super().raw_to_value(raw)
            if n == 0xff:
                return "not implemented"
            return f"{n >> 2}.{n & 0x3}"
        else:
            return '.'.join(str(x) for x in raw)
