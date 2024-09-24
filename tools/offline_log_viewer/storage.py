import os
import re
from os.path import join
import glob
import re
from record import RecordBatchIterator

SEGMENT_NAME_PATTERN = re.compile(
    "(?P<base_offset>\d+)-(?P<term>\d+)-v(?P<version>\d)\.log")


class Segment:
    def __init__(self, path):
        self.path = path

    def __iter__(self):
        return RecordBatchIterator(self.path)


class Ntp:
    def __init__(self, base_dir, namespace, topic, partition, ntp_id):
        self.base_dir = base_dir
        self.nspace = namespace
        self.topic = topic
        self.partition = partition
        self.ntp_id = ntp_id
        self.path = os.path.join(self.base_dir, self.nspace, self.topic,
                                 f"{self.partition}_{self.ntp_id}")
        pattern = os.path.join(self.path, "*.log")
        self.segments = glob.iglob(pattern)

        def _base_offset(segment_path):
            m = SEGMENT_NAME_PATTERN.match(os.path.basename(segment_path))
            return int(m['base_offset'])

        self.segments = sorted(self.segments, key=_base_offset)

    def __str__(self):
        return "{0.nspace}/{0.topic}/{0.partition}_{0.ntp_id}".format(self)


def listdirs(path):
    return [x for x in os.listdir(path) if os.path.isdir(join(path, x))]


class Store:
    def __init__(self, base_dir):
        self.base_dir = os.path.abspath(base_dir)
        self.ntps = []
        self.__search()

    def __search(self):
        for nspace in listdirs(self.base_dir):
            if nspace == "cloud_storage_cache":
                continue
            for topic in listdirs(join(self.base_dir, nspace)):
                for part_ntp_id in listdirs(join(self.base_dir, nspace,
                                                 topic)):
                    assert re.match("^\\d+_\\d+$", part_ntp_id), \
                        "ntp dir at {} does not match expected format. Wrong --path or extra directories present?"\
                        .format(join(self.base_dir, nspace, topic, part_ntp_id))
                    [part, ntp_id] = part_ntp_id.split("_")
                    ntp = Ntp(self.base_dir, nspace, topic, int(part),
                              int(ntp_id))
                    self.ntps.append(ntp)
