from selectors import SelectorKey
from threading import local
from typing import Dict, Optional, Tuple
from common.constants import LOG_SLOT_A, LOG_SLOT_B
from object_store.log.log_meta_manager import LogMetaManager
from object_store.log.log_meta import LogEntry
from s3_utils.s3_boto3 import S3Boto3
from common.models import Range
from typing import List
from common.local_file_utils import read_log_data_from_file
import asyncio
import json
import struct
from common.logger import print_info, print_debug, print_warning, print_error
import traceback
from object_store.object_meta_manager import ObjectMetaManager
import time
"""
Responsible for uploading, reading, and deleting log data (actual interaction with S3)
Calls S3 write interface, updates LogMeta state, does not manage the list
"""

class LogManager:
    def __init__(self, s3_client: S3Boto3, bucket: str, key_prefix: str):
        self.s3_client = s3_client
        self.bucket = bucket
        self.key_prefix = key_prefix
        self.active_slot = LOG_SLOT_A
        self._local_meta_cache: Dict[str, LogMetaManager] = {} # slot -> LogMetaManager

    ############log slot ############
    def get_active_slot(self) -> str:
        # return self.object_meta_manager.active_log_slot
        return self.active_slot

    def switch_log_slot(self):
        """Switch to another log slot"""
        current_slot = self.get_active_slot
        new_slot = LOG_SLOT_B if current_slot==LOG_SLOT_A else LOG_SLOT_A
        self._local_meta_cache[new_slot] = LogMetaManager(version=new_slot)
        self._local_meta_cache[new_slot].header.status = "active"
        self.active_log_slot=new_slot
    
    ############log meta ############
    def get_log_meta_key(self, slot: Optional[str] = None) -> str:
        if slot is None:
            slot = self.get_active_slot
        return f"{self.key_prefix}/{slot}.meta"

    def update_log_data_size(self, data_size: int):
        """Update log data size"""
        self._local_meta_cache[self.active_slot].update_log_data_size(data_size)
    
    def append_log_data_size(self, data_size: int):
        self._local_meta_cache[self.active_slot].append_log_data_size(data_size)
    
    async def get_local_meta(self, slot: Optional[str] = None) -> LogMetaManager:
        """
        Get local metadata cache for the specified slot.
        If slot is None, returns the active slot's metadata.
        """
        # 获取或创建本地缓存
        if slot is None:
            slot = self.active_slot
        fetch_flag=True
        if slot in self._local_meta_cache:
            if self._local_meta_cache(slot).header.has_s3_file==False:
                print_debug(f"No existing metadata, creating new LogMetaManager")
                self._local_meta_cache[slot] = LogMetaManager(version=slot)
                fetch_flag=False
                return self._local_meta_cache[slot]
            else:
                if len(self._local_meta_cache[slot].entries)==0:
                    fetch_flag=True
                else:
                    fetch_flag=False
        if(fetch_flag):    
            try:
                current_meta_bytes = await self.s3_client.get_object(
                    bucket=self.bucket,
                    key=self.get_log_meta_key(slot)
                )
                if current_meta_bytes and len(current_meta_bytes) > 0:
                    print_debug(f"Loading existing metadata from S3...")
                    existing_meta = LogMetaManager.deserialize(current_meta_bytes)
                    self._local_meta_cache[slot] = existing_meta
                    print_debug(f"Loaded {len(existing_meta.entries)} existing entries")
                else:
                    print_debug(f"No existing metadata, creating new LogMetaManager")
                    self._local_meta_cache[slot] = LogMetaManager(version=slot)
            except Exception as e:
                print_error(f"Error getting metadata from S3: {e}")
                self._local_meta_cache[slot] = LogMetaManager(version=slot)
                return self._local_meta_cache[slot]
        return self._local_meta_cache[slot]

     ############log entries ############
    
    async def append_log_meta(self, log_entries: List[LogEntry])->bool:
        """
        Append new log entries to local metadata cache.
        No S3 operations are performed - use sync_to_s3() to persist changes.
        """
        try:
            print_debug(f"Starting append_log_meta for slot {self.active_slot}")
            local_meta = await self.get_local_meta()
            if not local_meta:
                print_error(f"Failed to get local metadata for slot {self.active_slot}")
                return False
            print_debug(f"Local metadata has {len(local_meta.entries)} entries")       
            local_meta.add_entires(log_entries)       
            return True
        except Exception as e:
            print_error(f"Exception during append of log meta: {e}")
            print_error(traceback.format_exc())
            return False

    async def sync_to_s3(self, slot: Optional[str] = None) -> bool:
        """
        Synchronize local cache to S3.
        If slot is None, syncs the active slot.
        Returns True if all operations succeed, False otherwise.
        """
        if slot is None:
            slot = self.active_slot     
        try:
            print_debug(f"Syncing slot {slot} to S3...")
            success = True
            # 同步 metadata
            if slot in self._local_meta_cache and self._local_meta_cache[slot].modified_flag:
                print_debug(f"Syncing metadata for slot {slot}...")
                meta_data = self._local_meta_cache[slot].serialize()
                meta_success = await self.s3_client.put_object(
                    bucket=self.bucket,
                    key=self.get_log_meta_key(slot),
                    data=meta_data
                )
                if meta_success:
                    self._local_meta_cache[slot].modified_flag = False
                    print_debug(f"Metadata synced successfully for slot {slot}")
                else:
                    success = False
                    print_error(f"Failed to sync metadata for slot {slot}")
            else:
                print_warning(f"No local metadata cache for slot {slot}")
            return success
        except Exception as e:
            print_error(f"Exception during sync to S3: {e}")
            return False

    ############log entries ############
    async def get_log_entry_by_chunk_index(self, chunk_index: int, slot: Optional[str] = None) -> List[LogEntry]:
        """
        Get log entries by chunk index.
        Returns a list of LogEntry objects for the specified chunk.
        """
        target_slot = slot if slot is not None else self.active_slot
        local_meta = self._local_meta_cache[target_slot]
        return local_meta.get_entries_for_chunk(chunk_index)
    

    #############upload log data ###########
    async def upload_log_data(self, ranges: List[Range], data_provider)->bool:
        """
        Upload log data to S3.
        """
        start_time = time.time()
        async def upload_range(idx, range_info, s3_key):
            data = await data_provider(range_info)
            success = await self.s3_client.put_object(self.bucket, s3_key, data)
            return (idx, s3_key if success else None, success)
        ts = int(time.time() * 1000)
        upload_tasks = [
            upload_range(idx, r, f"{self.key_prefix}/range_{r.offset}_{r.length}_{ts}_{idx}")
            for idx, r in enumerate(ranges)
        ]
        upload_results = await asyncio.gather(*upload_tasks)
        s3_object_paths = [None] * len(ranges)
        for idx, s3_key, success in upload_results:
            if not success:
                print_warning(f"Failed to upload range {idx}")
                return False
            s3_object_paths[idx] = s3_key
        print(f"upload log data time: {time.time() - start_time:.2f}s")

        # Step 2: Update log meta info
        self.append_log_data_size(sum(r.length for r in ranges))
        new_log_entries = self._local_meta_cache[self.active_slot].construct_log_entries(ranges, s3_object_paths=s3_object_paths)
        log_meta_success = await self.append_log_meta(new_log_entries)
        if not log_meta_success:
            print_warning("Failed to append log meta to local cache")
            return False
        await self.sync_to_s3()
        return True
    
   ############serialize###########
    def log_meta_serialize(self)->Dict[str, str]:
        return {slot: self._local_meta_cache[slot].header_to_json() for slot in self._local_meta_cache}
    
    def log_meta_deserialize(self, log_meta_dict: Dict[str, str])->Dict[str, LogMetaManager]:
        for slot, json_str in log_meta_dict.items():
            self._local_meta_cache[slot] = LogMetaManager.from_header(json.loads(json_str))
        return self._local_meta_cache.items()
    
    def check_modified(self)->bool:
        return self._local_meta_cache[self.active_slot].modified_flag
        
    ###########deserialize###########
    @staticmethod
    def from_meta_header(s3_client: S3Boto3, bucket: str, key_prefix: str, active_slot: str, header_dicts: Dict[str, str])->'LogManager':
        print_info(f"from_header: {header_dicts}")
        log_manager = LogManager(s3_client, bucket, key_prefix)
        for slot, header_dict in header_dicts.items():
            log_manager._local_meta_cache[slot] = LogMetaManager.from_header(json.loads(header_dict)) 
        log_manager.active_slot = active_slot
        return log_manager