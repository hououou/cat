import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, Response

from core.dependencies import get_object_manager
from common.models import AtomicPutRequest, AppendRequest, MultiRangePutRequest
from botocore.exceptions import ClientError
from object_store.object_manager import ConditionalRangePutError
from object_store.objects_manager import ObjectsManager
from object_store.workflows import atomic_put, object_sync as run_object_sync


router = APIRouter()


async def _run_update(operation):
    try:
        return await operation
    except ConditionalRangePutError as exc:
        raise HTTPException(exc.status_code, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 500)
        raise HTTPException(status, exc.response.get("Error", {}).get("Code", "S3 error")) from exc


@router.post("/rangeput/{bucket}/{key:path}")
async def range_put(
    bucket: str, key: str, request: AtomicPutRequest,
    objects_manager: ObjectsManager = Depends(get_object_manager),
):
    if request.file_path and request.file_size is not None:
        return await _run_update(objects_manager.write_range_object_from_file_path(
            bucket, key, request.ranges, request.file_path, request.file_size,
            expected_mstate_id=request.mstate_id, return_result=True,
        ))
    if request.data is None:
        raise HTTPException(400, "RangePut requires data or file_path+file_size")
    return await _run_update(objects_manager.range_put(
        bucket, key, request.ranges, request.data, request.mstate_id,
    ))


@router.post("/multirangeput/{bucket}/{key:path}")
async def multi_range_put(
    bucket: str, key: str, request: MultiRangePutRequest,
    objects_manager: ObjectsManager = Depends(get_object_manager),
):
    return await _run_update(objects_manager.multi_range_put(
        bucket, key, request.ranges, request.data, request.mstate_id,
    ))


@router.post("/append/{bucket}/{key:path}")
async def append_object(
    bucket: str, key: str, request: AppendRequest,
    objects_manager: ObjectsManager = Depends(get_object_manager),
):
    return await _run_update(objects_manager.append(bucket, key, request.data))


@router.get("/aget/{bucket}/{key:path}")
async def aget(
    bucket: str, key: str, offset: int = Query(0, ge=0),
    length: Optional[int] = Query(None, ge=0),
    objects_manager: ObjectsManager = Depends(get_object_manager),
):
    result = await _run_update(objects_manager.aget(bucket, key, offset, length))
    return Response(result.data, media_type="application/octet-stream", headers={
        "ETag": f'"{result.mstate_id}"',
        "x-amz-meta-mstate-id": result.mstate_id,
        "x-amz-meta-logical-size": str(result.size),
    })


@router.post("/put_object/{bucket}/{key:path}")
async def put_object(
    bucket: str,
    key: str,
    request: AtomicPutRequest,
    objects_manager: ObjectsManager = Depends(get_object_manager)
):
    payload_size = request.file_size if request.file_path else (
        len(request.data) if request.data is not None else None
    )
    full_write = (
        request.mstate_id is None and request.total_modified_size is not None
        and request.total_modified_size > 0 and request.total_modified_size == payload_size
    )
    if not full_write and request.file_path and request.file_size is not None and request.ranges:
        return await range_put(bucket, key, request, objects_manager)
    if not full_write and request.data is not None and (request.ranges or request.mstate_id is not None):
        return await _run_update(objects_manager.range_put(
            bucket, key, request.ranges, request.data, request.mstate_id,
        ))
    try:
        return await atomic_put(
            objects_manager,
            bucket,
            key,
            ranges=request.ranges,
            file_path=request.file_path,
            file_size=request.file_size,
            total_modified_size=request.total_modified_size,
            data=request.data,
            expected_mstate_id=request.mstate_id,
        )
    except ConditionalRangePutError as e:
        return JSONResponse({
            "message": str(e),
            "success": False
        }, status_code=e.status_code)
    except ValueError:
        print("[DEBUG] invalid put_object request: missing file_path+file_size or data")
        return JSONResponse({
            "message": "Invalid request: must provide file_path+file_size or data"
        }, status_code=400)


@router.get("/get_object_range/{bucket}/{key:path}")
async def get_object_range(
    bucket: str,
    key: str,
    offset: int = Query(..., description="Starting byte offset"),
    length: int = Query(..., description="Read length"),
    objects_manager: ObjectsManager = Depends(get_object_manager)
):
    """
    Read a range of data from an object
    
    This endpoint reads a specific range of data from the object, handling both
    chunked storage and log-based patches efficiently.
    
    Args:
        bucket: S3 bucket name
        key: Object key name
        offset: Starting byte offset
        length: Number of bytes to read
        
    Returns:
        Response: Raw data bytes
        
    Raises:
        400: Invalid offset or length
        404: Object not found
        500: Error during read process
    """
    if offset < 0 or length <= 0:
        raise HTTPException(status_code=400, detail="Invalid offset or length")
    start_time = time.time()
    result = await _run_update(objects_manager.aget(bucket, key, offset, length))
    data = result.data
    end_time = time.time()
    print(f"[DEBUG] data size: {len(data)}, time: {end_time - start_time}")
    if data is None:
        raise HTTPException(status_code=404, detail="Object not found or read failed")

    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={
            "Content-Range": f"bytes {offset}-{offset + len(data) - 1}/{result.size}",
            "ETag": f'"{result.mstate_id}"',
            "x-amz-meta-mstate-id": result.mstate_id,
        }
    )


@router.post("/object_sync/{bucket}/{key:path}")
async def object_sync(
    bucket: str,
    key: str,
    request: Optional[AtomicPutRequest] = Body(default=None),
    objects_manager: ObjectsManager = Depends(get_object_manager)
):
    try:
        success = await run_object_sync(
            objects_manager,
            bucket,
            key,
            ranges=request.ranges if request is not None else None,
            file_path=request.file_path if request is not None else None,
            file_size=request.file_size if request is not None else None,
            data=request.data if request is not None else None,
        )
        print(f"[DEBUG] compact_file results: {success}")
        if success:
            return JSONResponse({
                "message": "Object sync completed",
                "success": success
            }, status_code=200)
        else:
            return JSONResponse({
                "message": "Object sync failed",
                "success": success
            }, status_code=500)
    except ValueError as e:
        return JSONResponse({
            "message": f"Invalid request: {e}",
            "success": False
        }, status_code=400)
    except Exception as e:
        return JSONResponse({
            "message": "Error during object sync",
            "error": str(e)
        }, status_code=500)


@router.head("/{bucket}/{key:path}")
async def proxy_head(bucket: str, key: str, objects_manager: ObjectsManager = Depends(get_object_manager)):
    meta = await _run_update(objects_manager.head_object(bucket, key))
    if meta is None:
        return Response(status_code=404)

    headers = {}
    headers["Content-Type"] = meta.get("content_type", "application/octet-stream")
    if meta.get("size") is not None:
        headers["Content-Length"] = str(meta["size"])
    if meta.get("last_modified"):
        try:
            if hasattr(meta["last_modified"], "strftime"):
                headers["Last-Modified"] = meta["last_modified"].strftime("%a, %d %b %Y %H:%M:%S GMT")
            else:
                dt = datetime.fromisoformat(meta["last_modified"])
                headers["Last-Modified"] = dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
        except Exception:
            pass
    if meta.get("etag"):
        headers["ETag"] = f'"{meta["etag"]}"'
    if meta.get("mstate_id"):
        headers["x-amz-meta-mstate-id"] = meta["mstate_id"]
    if meta.get("base_etag"):
        headers["x-amz-meta-base-etag"] = meta["base_etag"]
    for k, v in meta.get("custom_metadata", {}).items():
        headers[f"x-amz-meta-{k.lower()}"] = v

    headers = {k: str(v) for k, v in headers.items() if v is not None}
    print(f"[DEBUG] headers: {headers}")
    return Response(status_code=200, headers=headers)


@router.delete("/delete_object/{bucket}/{key:path}")
async def delete_object(
    bucket: str,
    key: str,
    objects_manager: ObjectsManager = Depends(get_object_manager)
):
    """
    Delete the object and all related metadata, chunk data, and snapshot data.
    """
    try:
        result = await objects_manager.delete_object_all(bucket, key)
        return {"result": result}
    except Exception as e:
        return {"error": str(e)}
