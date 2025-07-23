from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import subprocess
import os
import json
import asyncio
import uuid
import hashlib
import redis
from datetime import datetime
import tempfile
import logging
import sys
import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="OncoTwin Simplified API", version="1.0.0")

# Redis connection
try:
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        decode_responses=True
    )
    redis_client.ping()
    logger.info("Redis connection established")
except Exception as e:
    logger.warning(f"Redis connection failed: {e}. Using fallback in-memory storage.")
    redis_client = None

# Fallback in-memory storage when Redis is not available
job_status_memory = {}
cache_memory = {}

class SimplePipelineRequest(BaseModel):
    doctor_id: Optional[int] = None
    patient_ids: List[str]
    refresh: bool = False

class JobStatus(BaseModel):
    job_id: str
    status: str  # "pending", "running", "completed", "failed"
    message: str
    created_at: str
    doctor_id: Optional[int] = None
    completed_at: Optional[str] = None
    error: Optional[str] = None
    output_files: Optional[Dict[str, str]] = None  # {"json": "path", "excel": "path"}
    cache_key: Optional[str] = None

def get_cache_key(request: SimplePipelineRequest) -> str:
    """Generate a cache key based on request parameters"""
    content = f"{request.doctor_id}_{sorted(request.patient_ids)}"
    return hashlib.md5(content.encode()).hexdigest()

def get_job_status(job_id: str) -> Optional[Dict]:
    """Get job status from Redis or memory"""
    if redis_client:
        try:
            status_data = redis_client.get(f"job_status:{job_id}")
            return json.loads(status_data) if status_data else None
        except Exception as e:
            logger.error(f"Redis get error: {e}")
            return job_status_memory.get(job_id)
    else:
        return job_status_memory.get(job_id)

def set_job_status(job_id: str, status_data: Dict):
    """Set job status in Redis or memory"""
    if redis_client:
        try:
            redis_client.setex(f"job_status:{job_id}", 3600, json.dumps(status_data))  # 1 hour TTL
        except Exception as e:
            logger.error(f"Redis set error: {e}")
            job_status_memory[job_id] = status_data
    else:
        job_status_memory[job_id] = status_data

def get_cache(cache_key: str) -> Optional[Dict]:
    """Get cached result from Redis or memory"""
    if redis_client:
        try:
            cached_data = redis_client.get(f"cache:{cache_key}")
            return json.loads(cached_data) if cached_data else None
        except Exception as e:
            logger.error(f"Redis cache get error: {e}")
            return cache_memory.get(cache_key)
    else:
        return cache_memory.get(cache_key)

def set_cache(cache_key: str, data: Dict, ttl: int = 7200):  # 2 hours TTL
    """Set cached result in Redis or memory"""
    if redis_client:
        try:
            redis_client.setex(f"cache:{cache_key}", ttl, json.dumps(data))
        except Exception as e:
            logger.error(f"Redis cache set error: {e}")
            cache_memory[cache_key] = data
    else:
        cache_memory[cache_key] = data

def create_samples_file(patient_ids: List[str], job_id: str) -> str:
    """Create a temporary samples file for the pipeline"""
    samples_file = f"temp_samples_{job_id}.txt"
    with open(samples_file, 'w') as f:
        for patient_id in patient_ids:
            f.write(f"{patient_id}\n")
    return samples_file

def generate_excel_output(json_data: Dict, job_id: str) -> str:
    """Convert JSON results to Excel format"""
    excel_file = f"results_{job_id}.xlsx"
    
    try:
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Process each data type
            for key, data in json_data.items():
                if isinstance(data, list) and data:
                    df = pd.DataFrame(data)
                    sheet_name = key[:31]  # Excel sheet name limit
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                elif isinstance(data, dict):
                    # Convert dict to DataFrame
                    df = pd.DataFrame([data])
                    sheet_name = key[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        return excel_file
    except Exception as e:
        logger.error(f"Error creating Excel file: {e}")
        return None

async def run_pipeline_async(job_id: str, request: SimplePipelineRequest):
    """Run the pipeline in background, using the refactored, portable scripts."""
    samples_file = None
    job_data = get_job_status(job_id)
    
    # Define a dedicated output directory for this job to keep results isolated.
    base_output_dir = os.path.join(os.getcwd(), "api_outputs")
    job_output_dir = os.path.join(base_output_dir, job_id)
    os.makedirs(job_output_dir, exist_ok=True)

    try:
        # Update status to running
        job_data["status"] = "running"
        job_data["message"] = "Pipeline execution started"
        set_job_status(job_id, job_data)
        
        # Check cache first
        cache_key = get_cache_key(request)
        if not request.refresh:
            cached_result = get_cache(cache_key)
            if cached_result:
                # To ensure files are accessible, we can't just use cached paths.
                # A more robust cache would handle this, but for now, we re-run if files are gone.
                cached_files = cached_result.get("output_files", {})
                if all(os.path.exists(p) for p in cached_files.values()):
                    logger.info(f"Job {job_id}: Using cached result with existing files.")
                    job_data.update({ "status": "completed", "message": "Completed using cached result", "completed_at": datetime.now().isoformat(), "output_files": cached_files, "cache_key": cache_key, })
                    set_job_status(job_id, job_data)
                    return
                else:
                    logger.warning(f"Job {job_id}: Cache hit, but output files are missing. Re-running.")

        # Create a temporary file for the patient IDs
        samples_file = create_samples_file(request.patient_ids, job_id)
        
        current_dir = os.getcwd()
        python_executable = sys.executable
        pipeline_script = os.path.join(current_dir, "run_pipeline_pq.py")
        
        # Build the command for the refactored orchestrator script
        cmd = [
            python_executable, 
            pipeline_script, 
            "--samples", samples_file, 
            "--output_dir", job_output_dir, 
            "--json_output" # Ensure JSON is created for API consumption
        ]
        # Add doctor_id to the command if it was provided in the request
        if request.doctor_id is not None:
            cmd.extend(["--doctor_id", str(request.doctor_id)])
        
        logger.info(f"Job {job_id}: Running command: {' '.join(cmd)}")
        
        process = subprocess.run(
            cmd, capture_output=True, text=True, cwd=current_dir, timeout=7200
        )
        
        job_data["stdout"] = process.stdout
        job_data["stderr"] = process.stderr

        if process.returncode == 0:
            logger.info(f"Job {job_id}: Pipeline script finished successfully.")
            
            output_files = {}
            json_results = {}
            
            # Look for the specific output files in the job's dedicated output directory
            json_path = os.path.join(job_output_dir, "matches_consolidated.json")
            excel_path = os.path.join(job_output_dir, "matches_scoring_consolidated.xlsx")

            if os.path.exists(json_path):
                output_files['json'] = json_path
                try:
                    with open(json_path, 'r') as f:
                        json_results = json.load(f)
                    logger.info(f"Job {job_id}: Found JSON output: {json_path}")
                except Exception as e:
                    logger.error(f"Job {job_id}: Failed to read JSON output {json_path}: {e}")
            
            if os.path.exists(excel_path):
                output_files['excel'] = excel_path
                logger.info(f"Job {job_id}: Found Excel output: {excel_path}")
            
            # Check if the primary output was actually found
            if not json_results:
                job_data.update({
                    "status": "failed",
                    "message": "Pipeline completed but did not generate the expected JSON output.",
                    "error": f"File not found: {json_path}",
                    "completed_at": datetime.now().isoformat()
                })
            else:
                job_data.update({
                    "status": "completed",
                    "message": "Pipeline completed successfully.",
                    "completed_at": datetime.now().isoformat(),
                    "output_files": output_files
                })
                # Cache the results with the correct file paths
                set_cache(cache_key, {
                    "output_files": output_files,
                    "json_results": json_results,
                    "cached_at": datetime.now().isoformat()
                })
        else:
            # Failure case
            error_msg = f"Pipeline failed with return code {process.returncode}"
            logger.error(f"Job {job_id}: {error_msg}\nSTDERR: {process.stderr.strip()}")
            job_data.update({
                "status": "failed",
                "message": error_msg,
                "error": process.stderr.strip() or "No error message captured.",
                "completed_at": datetime.now().isoformat()
            })
            
    except Exception as e:
        error_msg = f"An unexpected error occurred in the API worker: {str(e)}"
        logger.error(f"Job {job_id}: {error_msg}", exc_info=True)
        job_data.update({ "status": "failed", "message": error_msg, "error": str(e), "completed_at": datetime.now().isoformat() })
        
    finally:
        if samples_file and os.path.exists(samples_file):
            try:
                os.remove(samples_file)
            except Exception as e:
                logger.error(f"Job {job_id}: Failed to clean up samples file: {e}")
        
        set_job_status(job_id, job_data)

@app.get("/")
async def root():
    return {
        "message": "OncoTwin Simplified API", 
        "version": "1.0.0",
        "features": ["Redis caching", "Excel/JSON output", "Job tracking"]
    }

@app.get("/health")
async def health_check():
    redis_status = "connected" if redis_client else "disconnected"
    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "redis": redis_status
    }

@app.post("/process")
async def process_patients(
    request: SimplePipelineRequest,
    background_tasks: BackgroundTasks
):
    """Process a list of patient IDs and return both Excel and JSON results"""
    
    # Validate input
    if not request.patient_ids:
        raise HTTPException(status_code=400, detail="Patient IDs list cannot be empty")
    
    # Generate unique job ID
    job_id = str(uuid.uuid4())
    cache_key = get_cache_key(request)
    
    # Initialize job status
    job_data = {
        "job_id": job_id,
        "doctor_id": request.doctor_id,
        "status": "pending",
        "message": "Job queued for execution",
        "created_at": datetime.now().isoformat(),
        "completed_at": None,
        "error": None,
        "output_files": None,
        "cache_key": cache_key
    }
    
    set_job_status(job_id, job_data)
    
    # Check cache first if not refreshing
    if not request.refresh:
        cached_result = get_cache(cache_key)
        if cached_result:
            logger.info(f"Job {job_id}: Found cached result")
            job_data["status"] = "completed"
            job_data["message"] = "Completed using cached result"
            job_data["completed_at"] = datetime.now().isoformat()
            job_data["output_files"] = cached_result["output_files"]
            set_job_status(job_id, job_data)
            
            return {
                "job_id": job_id,
                "message": "Job completed using cached result",
                "status": "completed",
                "output_files": cached_result["output_files"],
                "cache_used": True
            }
    
    # Start background task
    background_tasks.add_task(run_pipeline_async, job_id, request)
    
    return {
        "job_id": job_id,
        "message": "Pipeline job started",
        "status": "pending",
        "doctor_id": request.doctor_id,
        "patient_count": len(request.patient_ids),
        "refresh": request.refresh,
        "cache_key": cache_key
    }

@app.get("/status/{job_id}")
async def get_job_status_endpoint(job_id: str):
    """Get status of a specific job"""
    job_data = get_job_status(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job_data

@app.get("/download/{job_id}/{format}")
async def download_results(job_id: str, format: str):
    """Download results in specified format (json or excel)"""
    job_data = get_job_status(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job_data["status"] != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")
    
    if not job_data.get("output_files"):
        raise HTTPException(status_code=404, detail="No output files found")
    
    if format not in ["json", "excel"]:
        raise HTTPException(status_code=400, detail="Format must be 'json' or 'excel'")
    
    file_path = job_data["output_files"].get(format)
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"{format.upper()} file not found")
    
    filename = f"results_{job_id}.{format if format != 'excel' else 'xlsx'}"
    return FileResponse(file_path, filename=filename)

@app.get("/results/{job_id}")
async def get_results_json(job_id: str):
    """Get results in JSON format directly. The doctor_id is now part of the source file."""
    job_data = get_job_status(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found")
    
    logger.info(f"Getting results for job {job_id}, status: {job_data.get('status')}")
    
    if job_data["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job not completed. Current status: {job_data['status']}")
    
    # The inject_doctor_id function is no longer needed as the ID comes from the source file.

    # Check cache first
    cache_key = job_data.get("cache_key")
    if cache_key:
        cached_result = get_cache(cache_key)
        if cached_result and "json_results" in cached_result:
            logger.info(f"Returning cached results for job {job_id}")
            results = cached_result["json_results"]
            if not results:
                logger.warning(f"Cached results are empty for job {job_id}")
            return results
    
    # Fallback to reading from file
    output_files = job_data.get("output_files", {})
    logger.info(f"Output files for job {job_id}: {output_files}")
    
    json_file = output_files.get("json")
    
    if not json_file:
        if cache_key:
            cached_result = get_cache(cache_key)
            if cached_result and "json_results" in cached_result:
                return cached_result["json_results"]
        
        return { "error": "No JSON results file found", "debug_info": { "job_data": job_data, "cache_key": cache_key, "available_files": output_files } }
    
    if not os.path.exists(json_file):
        logger.error(f"JSON file {json_file} does not exist for job {job_id}")
        raise HTTPException(status_code=404, detail=f"JSON file not found: {json_file}")
    
    try:
        with open(json_file, 'r') as f:
            results = json.load(f)
        logger.info(f"Successfully read JSON file for job {job_id}, size: {len(str(results))} chars")
        if not results:
            logger.warning(f"JSON file is empty for job {job_id}")
        return results
    except Exception as e:
        logger.error(f"Error reading JSON file {json_file} for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading results: {str(e)}")

@app.delete("/job/{job_id}")
async def delete_job(job_id: str):
    """Delete a job and its status"""
    job_data = get_job_status(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Delete from Redis or memory
    if redis_client:
        try:
            redis_client.delete(f"job_status:{job_id}")
        except Exception as e:
            logger.error(f"Redis delete error: {e}")
    
    if job_id in job_status_memory:
        del job_status_memory[job_id]
    
    return {"message": f"Job {job_id} deleted successfully"}

@app.get("/cache/clear")
async def clear_cache():
    """Clear all cached results"""
    cleared_count = 0
    
    if redis_client:
        try:
            # Get all cache keys
            cache_keys = redis_client.keys("cache:*")
            if cache_keys:
                cleared_count = redis_client.delete(*cache_keys)
        except Exception as e:
            logger.error(f"Redis cache clear error: {e}")
    
    # Clear memory cache
    cache_memory.clear()
    
    return {"message": f"Cache cleared", "items_cleared": cleared_count}

@app.get("/cache/stats")
async def cache_stats():
    """Get cache statistics"""
    stats = {"memory_cache_size": len(cache_memory)}
    
    if redis_client:
        try:
            cache_keys = redis_client.keys("cache:*")
            job_keys = redis_client.keys("job_status:*")
            stats.update({
                "redis_cache_size": len(cache_keys),
                "redis_job_status_size": len(job_keys),
                "redis_connected": True
            })
        except Exception as e:
            stats["redis_connected"] = False
            stats["redis_error"] = str(e)
    else:
        stats["redis_connected"] = False
    
    return stats

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 