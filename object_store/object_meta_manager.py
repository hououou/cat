import io
import json
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Set
from botocore.exceptions import ClientError
from object_store.log.log_manager import LogManager
from s3_utils.s3_boto3 import S3Boto3
from object_store.chunk.chunk_meta import ChunkMetaInfo
from object_store.chunk.chunk_manager import ChunkManager
from object_store.chunk.chunk_meta_manager import ChunkMetaManager
from common.constants import LOG_SLOT_A, LOG_SLOT_B, CHUNK_SIZE
from object_store.log.log_meta import LogMetaHeader
from object_store.log.log_meta_manager import LogMetaManager
from common.logger import print_info, print_debug, print_warning, print_error
from common.range import Range
"""
Improved object metadata manager
Provides more complete chunk meta and log meta information recording
Supports efficient data location query and status management
"""

class ObjectMetaManager:
    def __init__(self, s3_client: S3Boto3, bucket_name: str, object_key: str, file_size: int, chunk_size: int, log_manager: LogManager, chunk_manager: ChunkManager):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.chunk_size = chunk_size
        self.file_size = file_size
        self.log_manager = log_manager  # 新增：用于获取log summary等信息
        self.chunk_manager = chunk_manager
        
        # Version control
        self.version = 1
        # 统一使用UTC时间
        self.created_at = datetime.now(timezone.utc)
        self.last_modified = datetime.now(timezone.utc)  # 合并时间戳字段
        
        # Todo(Jiamin): Chunk metadata - Improved data structure should be managed by chunk manager
        # self.chunk_metas: Dict[int, ChunkMetaInfo] = {}  # chunk_index -> ChunkMetaInfo
        # self.total_chunk_nums = 0  
        
        # Log metadata - Improved data structure
        # self.log_metas= Dict[str, LogMetaManager] #slot
        # self.active_log_slot = LOG_SLOT_A
        # self.total_log_nums = 0  
        
        # Data integrity
        self.etag = None
        self.checksum = None
        self.content_type = None
        self.custom_metadata: Dict[str, str] = {}
        
        # Status management
        self.status = "unknown"  # unknown, compacting, active, inactive
        self.encryption_enabled = False
    
        # Snapshot bucket support
        self.snapshot_bucket: str = ""
        self.snapshot_key: str = ""
        self.snapshot_etag: str = ""
        self.snapshot_size: int = 0
        self.snapshot_created_at: Optional[datetime] = None
        self.has_snapshot: bool = False
        
        # Initialize default log slots
        self.modified_flag = True
    
    ###########basic info###########
    def update_file_size(self, file_size: int):
        if file_size < 0:
            print_warning(f"Invalid file size: {file_size}, keeping current size: {self.file_size}")
            return
        old_size = self.file_size
        self.file_size = file_size
        if old_size != file_size:
            print_info(f"File size updated: {old_size} -> {file_size} bytes")
            self.modified_flag = True
    
    #############chunk meta############
    def chunk_prefix_key(self) -> str:
        return f"{self.object_key}_data/chunks"
    
    # def log_prefix_key(self) -> str:
    #     return f"{self.object_key}_data/logs"
    
    # def meta_key(self) -> str:
    #     return f"{self.object_key}"

    def get_chunk_meta(self, chunk_index: int) -> Optional[ChunkMetaInfo]:
        """Get metadata for a specified chunk"""
        return self.chunk_metas.get(chunk_index)
        
    def update_chunk_meta(self, chunk_info: ChunkMetaInfo):
        """
        Update chunk metadata - if exists, update it; otherwise add new
        """
        chunk_index = chunk_info.chunk_index
        if chunk_index in self.chunk_metas:
            # 更新现有的 chunk meta
            existing_meta = self.chunk_metas[chunk_index]
            # 保留一些原有信息，更新其他信息
            existing_meta.version_nums = chunk_info.version_nums
            existing_meta.bucket_status = chunk_info.bucket_status
            existing_meta.storage_key = chunk_info.storage_key
            existing_meta.etag = chunk_info.etag
            existing_meta.size = chunk_info.size
            existing_meta.encrypted = chunk_info.encrypted
            existing_meta.checksum = chunk_info.checksum
            # 更新时间戳
            existing_meta.created_at = chunk_info.created_at
            print_debug(f"Updated existing chunk meta for chunk {chunk_index}: version={chunk_info.version_nums}, status={chunk_info.bucket_status}")
        else:
            # 添加新的 chunk meta
            self.chunk_metas[chunk_index] = chunk_info
            self.total_chunk_nums = len(self.chunk_metas)
            print_debug(f"Added new chunk meta for chunk {chunk_index}: version={chunk_info.version_nums}, status={chunk_info.bucket_status}")
    
    def get_chunks_for_range(self, start_offset: int, end_offset: int) -> List[ChunkMetaInfo]:
        """Get all chunks covered by the specified range"""
        chunks = []
        for chunk_meta in self.chunk_metas.values():
            if (chunk_meta.start_offset < end_offset and 
                chunk_meta.end_offset > start_offset):
                chunks.append(chunk_meta)
        return sorted(chunks, key=lambda x: x.chunk_index)

    ################active slot log meta#############
    def get_local_meta(self)->LogMetaManager:
        return self.log_manager.get_local_meta()

    def log_meta_serialize(self)->str:
        return self.get_local_meta().header_to_json()
    
    def log_meta_deserialize(self, json_str: str)->LogMetaManager:
        return self.get_local_meta().from_header(json.loads(json_str))
    
    def get_log_nums(self)->int:
        return self.get_local_meta().header.entry_count
    
    def get_active_log_slot(self)->str:
        return self.get_local_meta().header.slot
    
    def switch_log_slot(self):
        self.log_manager.switch_log_slot()
        self.modified_flag = True

    ###########serialize###########
    def meta_to_json(self) -> str:
        """
        Serialize to JSON
        """
        meta = {
            "version": self.version,
            "bucket_name": self.bucket_name,
            "object_key": self.object_key,
            "file_size": self.file_size,
            "chunk_size": self.chunk_size,
            "created_at": self.created_at.isoformat(),
            "last_modified": self.last_modified.isoformat() if self.last_modified else None,
            # Chunk metadata
            # "total_chunk_nums": self.total_chunk_nums,
            "chunk_metas": self.chunk_manager.serialize(),
            # Log metadata
            "active_log_slot": self.log_manager.active_slot,
            "log_metas": self.log_manager.log_meta_serialize(),
            # "total_log_nums": self.get_log_nums(),
            # Data integrity
            "etag": self.etag,
            "checksum": self.checksum,
            "content_type": self.content_type,
            "custom_metadata": self.custom_metadata,
            # Status management
            "status": self.status,
            "encryption_enabled": self.encryption_enabled,
            # Snapshot bucket support
            "snapshot_bucket": self.snapshot_bucket,
            "snapshot_key": self.snapshot_key,
            "snapshot_etag": self.snapshot_etag,
            "snapshot_size": self.snapshot_size,
            "snapshot_created_at": self.snapshot_created_at.isoformat() if self.snapshot_created_at else None,
            "has_snapshot": self.has_snapshot
        }
        return json.dumps(meta, indent=2)

    @staticmethod
    def json_to_meta(json_str: str, s3_client: S3Boto3) -> 'ObjectMetaManager':
        """
        Deserialize from JSON
        """
        data = json.loads(json_str)
        # create log meta manager
        log_manager = LogManager.from_meta_header(
            s3_client=s3_client,
            bucket=data.get("bucket_name", ""),
            key_prefix=data.get("object_key", ""),
            active_slot=data.get("active_log_slot", LOG_SLOT_A),
            header_dicts=data.get("log_metas", {})
        )
        chunk_manager = ChunkManager.from_meta(
            s3_client=s3_client,
            bucket=data.get("bucket_name", ""),
            key_prefix=data.get("object_key", ""),
            chunk_size=data.get("chunk_size", 0),
            file_size=data.get("file_size", 0),
            data=data.get("chunk_metas", "")
        )
        # Basic attributes
        meta = ObjectMetaManager(
            bucket_name=data.get("bucket_name", ""),
            object_key=data.get("object_key", ""),
            file_size=data.get("file_size", 0),
            chunk_size=data.get("chunk_size", 0),
            log_manager=log_manager,
            chunk_manager=chunk_manager
        )
        meta.version = data.get("version", 1)
        meta.file_size = data.get("file_size", 0)
        meta.created_at = datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(timezone.utc)
        meta.last_modified = datetime.fromisoformat(data["last_modified"]) if data.get("last_modified") else datetime.now(timezone.utc)
        # Data integrity
        meta.etag = data.get("etag")
        meta.checksum = data.get("checksum")
        meta.content_type = data.get("content_type")
        meta.custom_metadata = data.get("custom_metadata", {})
        # Status management
        meta.status = data.get("status", "active")
        meta.encryption_enabled = data.get("encryption_enabled", False)
        # Snapshot bucket support
        meta.snapshot_bucket = data.get("snapshot_bucket", "")
        meta.snapshot_key = data.get("snapshot_key", "")
        meta.snapshot_etag = data.get("snapshot_etag", "")
        meta.snapshot_size = data.get("snapshot_size", 0)
        if data.get("snapshot_created_at"):
            meta.snapshot_created_at = datetime.fromisoformat(data["snapshot_created_at"])
        meta.has_snapshot = data.get("has_snapshot", False)
        return meta
    
    @staticmethod
    def create_initial_object_meta_manager(s3_client: S3Boto3, bucket_name: str, object_key: str, file_size: int, chunk_size: int)->'ObjectMetaManager':
        chunk_meta_manager = ChunkMetaManager(
            chunk_size=chunk_size,
            file_size=file_size
        )
        chunk_manager = ChunkManager(
            s3_client=s3_client,
            bucket=bucket_name,
            key_prefix=object_key,
            chunk_meta_manager=chunk_meta_manager
        )
        log_manager = LogManager(
            s3_client=s3_client,
            bucket=bucket_name,
            key_prefix=object_key
        )
        object_meta_manager = ObjectMetaManager(
            s3_client=s3_client,
            bucket_name=bucket_name,
            object_key=object_key,
            file_size=file_size,
            chunk_size=chunk_size,
            log_manager=log_manager,
            chunk_manager=chunk_manager
        )   
        return object_meta_manager
    
    def check_modified(self)->bool:
        return self.modified_flag and self.chunk_manager.check_modified() and self.log_manager.check_modified()
    
    async def save_to_s3(self, s3_client: S3Boto3) -> bool:
        if not self.check_modified():
            print_warning("Object meta not modified, skipping save")
            return True
        try:
            # 在保存前更新last_modified，因为调用save_to_s3意味着meta数据发生了更新
            original_last_modified = self.last_modified
            self.last_modified = datetime.now(timezone.utc)
            print_info(f"Saving object meta to S3, self.meta_to_json(): {self.meta_to_json()}")
            
            meta_bytes = self.meta_to_json().encode("utf-8")
            success = await s3_client.put_object_crt(
                bucket=self.bucket_name,
                key=self.object_key,
                data=meta_bytes
            )
            if success:
                self.modified_flag = False
                print_debug(f"Object meta saved successfully, updated last_modified to: {self.last_modified}")
            else:
                # 保存失败，回滚last_modified
                self.last_modified = original_last_modified
                print_error("Failed to save object meta to S3, rolled back last_modified")
            return success
        except Exception as e:
            # 发生异常，回滚last_modified
            self.last_modified = original_last_modified
            print_error(f"Error saving object meta to S3: {e}")
            return False

    ###########Snapshot bucket related methods###########
    def should_snapshot_chunks(self, threshold: int = 1000) -> bool:
        """Determine if snapshot is needed"""
        active_log = self.get_active_log_meta()
        return active_log.entry_count > threshold

    def get_snapshot_bucket_name(self) -> str:
        """Get snapshot bucket name"""
        return f"{self.bucket_name}-snapshot"

    def set_snapshot_info(self, snapshot_key: str, snapshot_size: int, snapshot_etag: str = ""):
        """Set snapshot information"""
        self.snapshot_bucket = self.get_snapshot_bucket_name()
        self.snapshot_key = snapshot_key
        self.snapshot_size = snapshot_size
        self.snapshot_etag = snapshot_etag
        self.snapshot_created_at = datetime.now(timezone.utc)
        # 注意：last_modified 应该在成功保存到 S3 后才更新，不在这里更新
        self.has_snapshot = True

    def get_snapshot_info(self) -> Dict:
        """Get snapshot information"""
        return {
            "snapshot_bucket": self.get_snapshot_bucket_name(),
            "snapshot_key": self.snapshot_key,
            "snapshot_size": self.snapshot_size,
            "snapshot_etag": self.snapshot_etag,
            "snapshot_created_at": self.snapshot_created_at.isoformat() if self.snapshot_created_at else None,
            "has_snapshot": self.has_snapshot
        }

    def clear_snapshot_info(self):
        """Clear snapshot information"""
        self.snapshot_key = ""
        self.snapshot_etag = ""
        self.snapshot_size = 0
        self.snapshot_created_at = None
        # 注意：last_modified 应该在成功保存到 S3 后才更新，不在这里更新
        self.has_snapshot = False


    # def update_object_size(self, file_size: int):
    #     """
    #     Update object size with validation
    #     """
    #     if file_size < 0:
    #         print_warning(f"Invalid file size: {file_size}, keeping current size: {self.file_size}")
    #         return
        
    #     old_size = self.file_size
    #     self.file_size = file_size
    #     if old_size != file_size:
    #         print_info(f"File size updated: {old_size} -> {file_size} bytes")
    #     else:
    #         print_debug(f"File size unchanged: {file_size} bytes")

    # def mark_dirty_range(self, offset: int, length: int):
    #     """
    #     Mark dirty data range (for backward compatibility)
    #     This method is reserved for backward compatibility, but it is recommended to use the new chunk metadata management.
    #     """
    #     pass

    # def get_dirty_chunks(self) -> List[int]:
    #     """
    #     Get dirty chunks (for backward compatibility)
    #     This method is reserved for backward compatibility.
    #     """
    #     return [chunk.chunk_index for chunk in self.chunk_metas.values() if chunk.bucket_status == "pending"]
