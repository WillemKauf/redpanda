import struct
import crc32c
import os
import logging

from collections import namedtuple
from enum import Enum
from io import BytesIO
from reader import Reader

logger = logging.getLogger('rp')

# https://docs.python.org/3.8/library/struct.html#format-strings
#
# redpanda header prefix:
#   - little endian encoded
#   - batch size, base offset, type crc
#
# note that the crc that is stored is the crc reported by kafka which happens to
# be computed over the big endian encoding of the same data. thus to verify the
# crc we need to rebuild part of the header in big endian before adding to crc.
HDR_FMT_RP_PREFIX_NO_CRC = "iqbI"
HDR_FMT_RP_PREFIX = "<I" + HDR_FMT_RP_PREFIX_NO_CRC

# below the crc redpanda and kafka have the same layout
#   - little endian encoded
#   - attributes ... record_count
HDR_FMT_CRC = "hiqqqhii"

HDR_FMT_RP = HDR_FMT_RP_PREFIX + HDR_FMT_CRC
HEADER_SIZE = struct.calcsize(HDR_FMT_RP)

RecordBatchHeader = namedtuple(
    'Header', ('header_crc', 'batch_size', 'base_offset', 'batch_type', 'crc',
               'attrs', 'delta', 'first_ts', 'max_ts', 'producer_id',
               'producer_epoch', 'base_seq', 'record_count'))


class CompressionType(Enum):
    none = 0
    gzip = 1
    snappy = 2
    lz4 = 3
    zstd = 4
    unknown = -1

    @classmethod
    def _missing_(e, value):
        return e.unknown


class TimestampType(Enum):
    append_time = 0
    create_time = 1


class RecordBatchAttrs:
    compression_mask = 0x7
    ts_type_mask = 0x8
    transactional_mask = 0x10
    control_mask = 0x20

    @staticmethod
    def compression_type(header: RecordBatchHeader):
        return CompressionType(header.attrs
                               & RecordBatchAttrs.compression_mask)

    @staticmethod
    def is_transactional(header: RecordBatchHeader):
        return header.attrs & RecordBatchAttrs.transactional_mask == RecordBatchAttrs.transactional_mask

    @staticmethod
    def is_control(header: RecordBatchHeader):
        return header.attrs & RecordBatchAttrs.control_mask == RecordBatchAttrs.control_mask

    @staticmethod
    def timestamp_type(header: RecordBatchHeader):
        return TimestampType(header.attrs & RecordBatchAttrs.ts_type_mask)


class CorruptBatchError(Exception):
    def __init__(self, batch):
        self.batch = batch


class Record:
    def __init__(self, length, attrs, timestamp_delta, offset_delta, key,
                 value, headers):
        self.length = length
        self.attrs = attrs
        self.timestamp_delta = timestamp_delta
        self.offset_delta = offset_delta
        self.key = key
        self.value = value
        self.headers = headers

    def kv_dict(self):
        key = None if self.key == None else self.key.hex()
        val = None if self.value == None else self.value.hex()
        return {"k": key, "v": val}


class RecordHeader:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class RecordIter:
    def __init__(self, record_count, records_data):
        self.data_stream = BytesIO(records_data)
        self.rdr = Reader(self.data_stream)
        self.record_count = record_count

    def _parse_header(self):
        k_sz = self.rdr.read_varint()
        key = self.rdr.read_bytes(k_sz)
        v_sz = self.rdr.read_varint()
        value = self.rdr.read_bytes(v_sz)
        return RecordHeader(key, value)

    def __next__(self):
        if self.record_count == 0:
            raise StopIteration()

        self.record_count -= 1
        len = self.rdr.read_varint()
        attrs = self.rdr.read_int8()
        timestamp_delta = self.rdr.read_varint()
        offset_delta = self.rdr.read_varint()
        key_length = self.rdr.read_varint()
        if key_length > 0:
            key = self.rdr.read_bytes(key_length)
        else:
            key = None
        value_length = self.rdr.read_varint()
        if value_length > 0:
            value = self.rdr.read_bytes(value_length)
        else:
            value = None
        hdr_size = self.rdr.read_varint()
        headers = []
        for i in range(0, hdr_size):
            headers.append(self._parse_header())

        return Record(len, attrs, timestamp_delta, offset_delta, key, value,
                      headers)


class RecordBatchType(Enum):
    """Keep this in sync with model/record_batch_types.h"""
    raft_data = 1
    raft_configuration = 2
    controller = 3
    kvstore = 4
    checkpoint = 5
    topic_management_cmd = 6
    ghost_batch = 7
    id_allocator = 8
    tx_prepare = 9
    tx_fence = 10
    tm_update = 11
    user_management_cmd = 12
    acl_management_cmd = 13
    group_prepare_tx = 14
    group_commit_tx = 15
    group_abort_tx = 16
    node_management_cmd = 17
    data_policy_management_cmd = 18
    archival_metadata = 19
    cluster_config_cmd = 20
    feature_update = 21
    cluster_bootstrap_cmd = 22
    version_fence = 23
    tx_tm_hosted_trasactions = 24
    prefix_truncate = 25
    plugin_update = 26
    tx_registry = 27
    cluster_recovery_cmd = 28
    compaction_placeholder = 29
    role_management_cmd = 30
    client_quota = 31
    data_migration_cmd = 32
    group_fence_tx = 33
    unknown = -1

    @classmethod
    def _missing_(e, value):
        return e.unknown


class RecordBatch:
    def __init__(self, index, header, records):
        self.index = index
        self.header = header
        self.term = None
        self.records = records
        self.batch_type = RecordBatchType(header[3])
        self.header_dict = self.parse_header_dict()

        header_crc_bytes = struct.pack(
            "<" + HDR_FMT_RP_PREFIX_NO_CRC + HDR_FMT_CRC, *self.header[1:])
        header_crc = crc32c.crc32c(header_crc_bytes)
        if self.header.header_crc != header_crc:
            raise CorruptBatchError(self)
        crc = crc32c.crc32c(self._crc_header_be_bytes())
        crc = crc32c.crc32c(records, crc)
        if self.header.crc != crc:
            raise CorruptBatchError(self)

    def parse_header_dict(self):
        header = self.header._asdict()
        attrs = header['attrs']
        header["type_name"] = self.batch_type.name
        header['expanded_attrs'] = {
            'compression': RecordBatchAttrs.compression_type(self.header).name,
            'transactional': RecordBatchAttrs.is_transactional(self.header),
            'control_batch': RecordBatchAttrs.is_control(self.header),
            'timestamp_type': RecordBatchAttrs.timestamp_type(self.header).name
        }
        return header

    def last_offset(self):
        return self.header.base_offset + self.header.record_count - 1

    def _crc_header_be_bytes(self):
        # encode header back to big-endian for crc calculation
        return struct.pack(">" + HDR_FMT_CRC, *self.header[5:])

    @staticmethod
    def from_stream(f, index):
        data = f.read(HEADER_SIZE)
        if len(data) == HEADER_SIZE:
            header = RecordBatchHeader(*struct.unpack(HDR_FMT_RP, data))
            # it appears that we may have hit a truncation point if all of the
            # fields in the header are zeros
            if all(map(lambda v: v == 0, header)):
                return
            records_size = header.batch_size - HEADER_SIZE
            data = f.read(records_size)
            if len(data) < records_size:
                # Short read, probably end of a partially written log.
                logger.info(
                    "Stopping batch parse on short read (this is normal if the log was not from a clean shutdown)"
                )
                return None
            assert len(data) == records_size
            return RecordBatch(index, header, data)

        if len(data) < HEADER_SIZE:
            # Short read, probably log being actively written or unclean shutdown
            return None

    def __len__(self):
        return self.header.record_count

    def __iter__(self):
        return RecordIter(self.header.record_count, self.records)


class RecordBatchIterator:
    def __init__(self, path):
        self.path = path
        self.file = open(path, "rb")
        self.idx = 0

    def __next__(self):
        b = RecordBatch.from_stream(self.file, self.idx)
        if not b:
            fsize = os.stat(self.path).st_size
            if fsize != self.file.tell():
                logger.warn(
                    f"Incomplete read of {self.path}: {self.file.tell()}/{fsize}"
                )
            raise StopIteration()
        self.idx += 1
        return b

    def __del__(self):
        self.file.close()
