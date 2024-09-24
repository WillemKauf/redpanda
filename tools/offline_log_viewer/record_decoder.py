from reader import Reader
from record import Record, Header, RecordBatchType, RecordBatchAttrs


def read_record_batch_type(reader: Reader) -> RecordBatchType:
    v = reader.read_int8()
    return RecordBatchType(v)


class TxRecordDecoder:
    @staticmethod
    def decode_record(record: Record, header: RecordBatchHeader) -> dict:
        match header.batch_type:
            case RecordBatchType.tx_prepare:
                # Deprecated.
                pass
            case RecordBatchType.tx_fence:
                return TxRecordDecoder.parse_tx_fence(record, header)
            case RecordBatchType.tm_update:
                return parse_tm_update()
            case RecordBatchType.group_prepare_tx:
                #Deprecated.
                pass
            case RecordBatchType.group_commit_tx:
                return parse_group_commit_tx()
            case RecordBatchType.group_abort_tx:
                return parse_group_abort_tx()
            case RecordBatchType.tx_tm_hosted_trasactions:
                #Deprecated.
                pass
            case RecordBatchType.tx_registry:
                #Deprecated.
                pass
            case RecordBatchType.group_fence_tx:
                return parse_group_fence_tx()
            case RecordBatchType.unknown:
                pass
        return {}

    @staticmethod
    def parse_tx_fence(record: Record, header: RecordBatchHeader) -> dict:
        ret = {}
        key_rdr = Reader(BytesIO(self.r.key))
        ret['type'] = read_record_batch_type(key_rdr)
        assert ret['type'] == header.batch_type
        ret['producer_id'] = key_rdr.read_int64()
        assert ret['producer_id'] == header.producer_id
        val_rdr = Reader(BytesIO(self.r.value))
        ret['version'] = val_rdr.read_int8()
        # Only support latest version of tx_fence parsing.
        ret['group'] = val_rdr.read_string()
        ret['tx_seq'] = val_rdr.read_int64()
        ret['tx_timeout'] = val_rdr.read_int64()
        ret['partition'] = val_rdr.read_int32()
        return ret

    @staticmethod
    def parse_tm_update(record: Record, header: RecordBatchHeader) -> dict:
        # tm_update batch must contain a single record
        assert record.length == 1

        ret = {}
        key_rdr = Reader(BytesIO(self.r.key))
        ret['type'] = read_record_batch_type(key_rdr)
        assert ret['type'] == header.batch_type
        assert ret['producer_id'] == key_rdr.read_int64()

        val_rdr = Reader(BytesIO(self.r.value))
        ret['version'] = val_rdr.read_int8()
        # Only support latest version of tm_update parsing.
        ret['tx_id'] = val_rdr.read_string()
        ret['producer_id'] = val_rdr.read_int64()
        ret['producer_epoch'] = val_rdr.read_int16()
        ret['last_producer_id'] = val_rdr.read_int64()
        ret['last_producer_epoch'] = val_rdr.read_int16()
        ret['last_producer_id'] = val_rdr.read_int64()
        ret['tx_seq'] = val_rdr.read_int64()
        ret['etag'] = val_rdr.read_int64()
        ret['tx_status'] = val_rdr.read_int32()
        ret['timeout_ms'] = val_rdr.read_int64()
        ret['last_update_ts'] = val_rdr.read_int64()
        def read_tx_partition(reader: Reader):
            reader.
        ret['partitions'] = val_rdr.read_vector(read_tx_partition)
        ret['groups'] = val_rdr.read_vector(read_tx_group)
        return ret


class NonTxRecordDecoder:
    @staticmethod
    def decode_record(record: Record, header: RecordBatchHeader):
        match header.batch_type:
            case RecordBatchType.raft_data:
                pass
            case RecordBatchType.raft_configuration:
                pass
            case RecordBatchType.controller:
                pass
            case RecordBatchType.kvstore:
                pass
            case RecordBatchType.checkpoint:
                pass
            case RecordBatchType.topic_management_cmd:
                pass
            case RecordBatchType.ghost_batch:
                pass
            case RecordBatchType.id_allocator:
                pass
            case RecordBatchType.user_management_cmd:
                pass
            case RecordBatchType.acl_management_cmd:
                pass
            case RecordBatchType.node_management_cmd:
                pass
            case RecordBatchType.data_policy_management_cmd:
                pass
            case RecordBatchType.archival_metadata:
                pass
            case RecordBatchType.cluster_config_cmd:
                pass
            case RecordBatchType.feature_update:
                pass
            case RecordBatchType.cluster_bootstrap_cmd:
                pass
            case RecordBatchType.version_fence:
                pass
            case RecordBatchType.prefix_truncate:
                pass
            case RecordBatchType.plugin_update:
                pass
            case RecordBatchType.cluster_recovery_cmd:
                pass
            case RecordBatchType.compaction_placeholder:
                pass
            case RecordBatchType.role_management_cmd:
                pass
            case RecordBatchType.client_quota:
                pass
            case RecordBatchType.data_migration_cmd:
                pass
            case RecordBatchType.unknown:
                pass


class RecordDecoder:
    @staticmethod
    def decode_record(record: Record, header: RecordBatchHeader):
        if RecordBatchAttrs.is_transactional(header):
            return TxRecordDecoder.decode_record(record, header)
        else:
            return NonTxRecordDecoder.decode_record(record)


class RecordBatchDecoder:
    """Use like:
       for record in RecordBatchDecoder(record_batch):
           ...
    """
    def __init__(self, record_batch: RecordBatch):
        self.record_batch = record_batch

    def __iter__(self):
        for record in self.record_batch:
            yield RecordDecoder.decode_record(record, self.record_batch.header)
