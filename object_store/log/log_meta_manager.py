from typing import List, Dict, Tuple, Set, Optional
import json
import struct
from datetime import datetime, timezone
from object_store.log.log_meta import LogMetaHeader, LogEntry
from common.constants import CHUNK_SIZE
from common.models import Range
from common.logger import print_info, print_debug, print_warning, print_error
import traceback

class LogMetaManager:
    def __init__(self, version: str):
        self.header = LogMetaHeader(version)
        self.entries: List[LogEntry] = []
        self.modified_flag: bool = False
       
    ######update log meta info#############
    def update_log_data_size(self, data_size: int):
        self.header.data_file_size = data_size
        self.modified_flag=True

    def append_log_data_size(self, data_size: int):
        self.header.data_file_size += data_size
        self.modified_flag=True

    ######entries#############
    def add_entry(self, entry: LogEntry):
        self.entries.append(entry)
        entry_idx = len(self.entries) - 1
        self.header.add_entry_summary(entry.chunk_index,entry.timestamp,entry.length, entry_idx)
    
    def add_entires(self, entries: List[LogEntry]):
        for entry in entries:
            self.add_entry(entry)

    def construct_log_entries(self, ranges: List[Range], s3_object_paths: List[str] = None) -> List[LogEntry]:
        log_entries = []
        current_timestamp = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
        for idx, r in enumerate(ranges):
            start_offset = r.offset
            length = r.length
            end_offset = start_offset + length
            current_offset = start_offset
            remaining_length = length
            # For each range, only one chunk is supported for s3_object_path mapping
            s3_path = s3_object_paths[idx] if s3_object_paths is not None else None
            while remaining_length > 0:
                chunk_index = current_offset // CHUNK_SIZE
                chunk_start = chunk_index * CHUNK_SIZE
                chunk_end = chunk_start + CHUNK_SIZE
                chunk_inner_start = current_offset - chunk_start
                chunk_inner_end = min(chunk_end, end_offset) - chunk_start
                chunk_inner_length = chunk_inner_end - chunk_inner_start
                log_entries.append(LogEntry(
                    chunk_index=chunk_index,
                    offset=chunk_inner_start,
                    length=chunk_inner_length,
                    timestamp=current_timestamp,
                    data_offset=0,
                    s3_object_path=s3_path
                ))
                current_offset += chunk_inner_length
                remaining_length -= chunk_inner_length
        return log_entries
    
    def get_entries_for_chunk(self, chunk_id: int) -> List[LogEntry]:
        """Get all entries for a specific chunk using chunk_summary entry_indices"""
        idx = str(chunk_id)
        if idx not in self.header.chunk_summary:
            return []
        entry_indices = self.header.chunk_summary[idx].get("entry_indices", [])
        return [self.entries[i] for i in entry_indices]

    ###########serialize###########

    def serialize(self) -> bytes:
        """Serialize to efficient binary format"""
        try:
            # 1. Header (JSON)
            header_json = json.dumps(self.header.to_dict()).encode("utf-8")
            # 2. Entries (binary)
            entries_bytes = bytearray()
            for i, entry in enumerate(self.entries):
                try:
                    entry_binary = entry.to_binary()
                    entries_bytes.extend(entry_binary)
                except Exception as e:
                    print_error(f"Failed to serialize entry {i}: {e}")
                    print_error(f"Entry data: {entry.to_dict()}")
                    raise e
            # 拼接
            result = bytearray()
            result.extend(header_json)
            result.extend(entries_bytes)
            return bytes(result)
        except Exception as e:
            print_error(f"Failed to serialize LogMetaManager: {e}")
            print_error(traceback.format_exc())
            raise e

    @staticmethod
    def deserialize(raw_bytes: bytes) -> 'LogMetaManager':
        """Deserialize from binary format with backward compatibility for JSON format"""
        try:
            # 先尝试解析header（JSON）
            header_end = 0
            for i in range(1, min(len(raw_bytes), 4096)):
                try:
                    header_json = raw_bytes[:i].decode("utf-8")
                    header_dict = json.loads(header_json)
                    if "version" in header_dict:
                        header_end = i
                        break
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
            if header_end == 0:
                raise ValueError("Failed to find valid header JSON in log meta file")
            header_json = raw_bytes[:header_end].decode("utf-8")
            header_dict = json.loads(header_json)
            header = LogMetaHeader.from_dict(header_dict)
            manager = LogMetaManager(header.version)
            manager.header = header
            # 解析entries
            entries_data = raw_bytes[header_end:]
            for i in range(0, len(entries_data), LogEntry.ENTRY_SIZE):
                entry_data = entries_data[i:i+LogEntry.ENTRY_SIZE]
                if len(entry_data) == LogEntry.ENTRY_SIZE:
                    entry = LogEntry.from_binary(entry_data)
                    manager.entries.append(entry)
            return manager
        except Exception as e:
            print_error(f"Failed to deserialize LogMetaManager: {e}")
            print_error(traceback.format_exc())
            raise e

    def header_to_json(self)->str:
        return json.dumps(self.header.to_dict(), indent=4, ensure_ascii=False)
        
    def from_header(self, header_dict: dict)->'LogMetaManager':
        print_info(f"from_header: {header_dict}")
        self.header = LogMetaHeader.from_dict(header_dict)
        self.entries = []
        return self

