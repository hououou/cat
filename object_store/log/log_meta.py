from typing import Dict, List, Set, Optional
import struct
import json
from datetime import datetime, timezone
from common.logger import print_info, print_debug, print_warning, print_error

"""
Improved header format - more efficient for S3 operations
{
  "version": "a",           // log slot identifier
  "entry_count": 128,       // total entries
  "header_size": 1024,      // size of header in bytes
  "index_offset": 2048,     // offset to chunk index
  "chunk_summary": {        // quick chunk statistics
    "0": {"dirty_size": 12345, "entry_count": 5, "first_ts": "2025-06-11T14:00:00Z", "last_ts": "2025-06-12T17:00:00Z"},
    "1": {"dirty_size": 12345, "entry_count": 3, "first_ts": "2025-06-12T13:22:00Z", "last_ts": "2025-06-12T16:55:00Z"}
  }
}

Binary format for better performance:
- Header: JSON (variable length)
- Chunk Index: [chunk_id, first_entry_offset, entry_count] * N (fixed width)
- Entries: Binary format (fixed width per entry)
"""
class LogMetaHeader:
    def __init__(self, version: str, slot: str = None, meta_key: str = None, data_key: str = None):
        self.version = version
        self.slot = slot  # 新增 slot
        self.meta_key: Optional[str] = meta_key  # 新增 meta_key
        self.data_key: Optional[str] = data_key  # 新增 data_key
        self.entry_count = 0
        self.header_size = 0
        self.index_offset = 0
        self.chunk_summary: Dict[str, Dict] = {}
        self.meta_file_size = 0  # 新增 meta file size
        self.data_file_size = 0  # 新增 data file size
        self.last_modified: Optional[datetime] = None  # 新增 last_modified
        self.status = "inactive"  # 新增 status
        self.chunk_coverage: Set[int] = set()  # 新增 chunk_coverage
        self.has_s3_file: bool = False  # 新增 has_s3_file

    def to_dict(self):
        return {
            "version": self.version,
            "slot": self.slot,
            "meta_key": self.meta_key,
            "data_key": self.data_key,
            "entry_count": self.entry_count,
            "header_size": self.header_size,
            "index_offset": self.index_offset,
            "chunk_summary": self.chunk_summary,
            "meta_file_size": self.meta_file_size,
            "data_file_size": self.data_file_size,
            "last_modified": self.last_modified.isoformat() if self.last_modified else None,
            "status": self.status,
            "chunk_coverage": list(self.chunk_coverage),
            "has_s3_file": self.has_s3_file
        }

    def add_entry_summary(self, chunk_index: int, timestamp: str, size: int, entry_idx: int):
        idx = str(chunk_index)
        if idx not in self.chunk_summary:
            self.chunk_summary[idx] = {
                "first_ts": timestamp,
                "last_ts": timestamp,
                "dirty_size": size,
                "entry_count": 1,
                "enrty_indices": [entry_idx]
            }
        else:
            summary = self.chunk_summary[idx]
            summary["last_ts"] = timestamp
            summary["dirty_size"] += size
            summary["entry_count"] += 1
            summary["entry_indices"].append(entry_idx)
        # --- 新增：同步更新其它 summary 字段 ---
        self.header.entry_count += 1
        self.last_modified = datetime.now(timezone.utc)
        self.chunk_coverage.add(chunk_index)
        self.data_file_size += size
        self.has_s3_file = True

    def calculate_offsets(self, header_json_size: int):
        """Calculate header_size and index_offset based on actual sizes"""
        self.header_size = header_json_size
        # Index starts after header, each index entry is 12 bytes (3 int32)
        self.index_offset = header_json_size


"""
Improved log entry format - more compact and efficient
Binary format: 24 bytes per entry
- chunk_index: uint32 (4 bytes)
- offset: uint32 (4 bytes) 
- length: uint32 (4 bytes)
- timestamp: uint64 (8 bytes) - Unix timestamp in seconds
- data_offset: uint32 (4 bytes)
"""
class LogEntry:
    # Binary format constants
    ENTRY_SIZE = 24  # 4 + 4 + 4 + 8 + 4 = 24 bytes
    INDEX_ENTRY_SIZE = 12  # 4 + 4 + 4 = 12 bytes (chunk_id, first_offset, count)
    
    def __init__(self, chunk_index: int, offset: int, length: int, data_offset: int, timestamp: str, s3_object_path: str = None):
        self.chunk_index = chunk_index
        self.offset = offset
        self.length = length
        self.data_offset = data_offset
        self.timestamp = timestamp
        self.s3_object_path = s3_object_path

    def to_dict(self):
        return {
            "chunk_index": self.chunk_index,
            "offset": self.offset,
            "length": self.length,
            "data_offset": self.data_offset,
            "timestamp": self.timestamp,
            "s3_object_path": self.s3_object_path
        }

    def to_binary(self) -> bytes:
        """Convert to binary format for efficient storage"""
        try:
            # Convert ISO timestamp to Unix timestamp
            timestamp_str = self.timestamp
            # 修复 +00:00Z 或 +00:00+00:00 的情况
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1]  # 去掉 Z
            if timestamp_str.endswith('+00:00'):
                timestamp_str = timestamp_str[:-6]  # 去掉 +00:00
            dt = datetime.fromisoformat(timestamp_str)
            unix_timestamp = int(dt.timestamp())
            # Ensure timestamp is within valid range for uint64
            if unix_timestamp < 0:
                unix_timestamp = 0
            elif unix_timestamp > 0xFFFFFFFFFFFFFFFF:  # Max uint64
                unix_timestamp = 0xFFFFFFFFFFFFFFFF
            
            # Correct field order: chunk_index, offset, length, data_offset, unix_timestamp
            binary_data = struct.pack('<IIIIQ', 
                self.chunk_index,
                self.offset, 
                self.length,
                self.data_offset,
                unix_timestamp
            )
            
            print_debug(f"[DEBUG] Serializing LogEntry: chunk={self.chunk_index}, offset={self.offset}, length={self.length}, data_offset={self.data_offset}")
            print_debug(f"[DEBUG] Binary data: {binary_data.hex()}")
            
            return binary_data
        except Exception as e:
            print_error(f"[ERROR] Failed to convert LogEntry to binary: {e}")
            print_error(f"[ERROR] Timestamp: {self.timestamp}")
            print_error(f"[ERROR] Entry: {self.to_dict()}")
            raise e

    @classmethod
    def from_binary(cls, data: bytes) -> 'LogEntry':
        """Create LogEntry from binary data"""
        try:
            # Correct field order: chunk_index, offset, length, data_offset, unix_timestamp
            chunk_index, offset, length, data_offset, unix_timestamp = struct.unpack('<IIIIQ', data)
            
            print_debug(f"[DEBUG] Deserializing LogEntry from binary: {data.hex()}")
            print_debug(f"[DEBUG] Raw values: chunk={chunk_index}, offset={offset}, length={length}, data_offset={data_offset}, timestamp={unix_timestamp}")
            
            # Ensure timestamp is within valid range
            if unix_timestamp < 0:
                unix_timestamp = 0
            elif unix_timestamp > 0xFFFFFFFFFFFFFFFF:  # Max uint64
                unix_timestamp = 0xFFFFFFFFFFFFFFFF
            # Convert Unix timestamp back to ISO format with microsecond precision
            try:
                dt = datetime.utcfromtimestamp(unix_timestamp)
                # 添加微秒部分以保持与construct_log_entries中的格式一致
                timestamp = dt.replace(microsecond=0).isoformat() + "Z"
            except (OSError, ValueError) as e:
                print_warning(f"[WARNING] Invalid timestamp {unix_timestamp}, using current time: {e}")
                timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat() + "Z"
            
            entry = cls(chunk_index, offset, length, data_offset, timestamp)
            print_debug(f"[DEBUG] Loaded LogEntry: {entry.to_dict()}")
            return entry
            
        except Exception as e:
            print_error(f"[ERROR] Failed to deserialize LogEntry from binary: {e}")
            print_error(f"[ERROR] Binary data: {data.hex()}")
            raise e

    @classmethod
    def from_dict(cls, data: dict) -> 'LogEntry':
        """Create LogEntry from dictionary"""
        return cls(
            chunk_index=data["chunk_index"],
            offset=data["offset"],
            length=data["length"],
            data_offset=data["data_offset"],
            timestamp=data["timestamp"]
        )