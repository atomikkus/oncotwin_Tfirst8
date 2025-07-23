from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import Optional, List
import subprocess
import os
import json
import asyncio
import uuid
from datetime import datetime
import tempfile
import shutil
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="OncoTwin Pipeline API", version="1.0.0")

# In-memory storage for job status (in production, use Redis or database)
job_status = {}

class PipelineRequest(BaseModel):
    samples_file_path: Optional[str] = None
    single_patient: Optional[str] = None
    json_output: bool = False
    resume: bool = False
    refresh: bool = False
    skip_genomic: bool = False
    skip_clinical: bool = False
    skip_matching: bool = False
    id_filename: str = "samples.txt"

class JobStatus(BaseModel):
    job_id: str
    status: str  # "pending", "running", "completed", "failed"
    message: str
    created_at: str
    completed_at: Optional[str] = None
    error: Optional[str] = None
    output_files: Optional[List[str]] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    command: Optional[str] = None

def run_pipeline_async(job_id: str, request: PipelineRequest, samples_content: Optional[str] = None):
    """Run the pipeline in background"""
    temp_samples_file = None
    try:
        # Update status to running
        job_status[job_id]["status"] = "running"
        job_status[job_id]["message"] = "Pipeline execution started"
        
        # Get current working directory and Python executable
        current_dir = os.getcwd()
        python_executable = sys.executable
        
        logger.info(f"Job {job_id}: Current directory: {current_dir}")
        logger.info(f"Job {job_id}: Python executable: {python_executable}")
        
        # Prepare command - use full path to Python and script
        pipeline_script = os.path.join(current_dir, "run_pipeline_pq.py")
        cmd = [python_executable, pipeline_script]
        
        # Handle samples file
        if samples_content:
            # Create temporary file for uploaded samples
            temp_samples_file = f"temp_samples_{job_id}.txt"
            temp_samples_path = os.path.join(current_dir, temp_samples_file)
            with open(temp_samples_path, 'w') as f:
                f.write(samples_content)
            cmd.extend(["--samples", temp_samples_path])
            logger.info(f"Job {job_id}: Created temp samples file: {temp_samples_path}")
        elif request.samples_file_path:
            # Use absolute path if not already absolute
            samples_path = request.samples_file_path
            if not os.path.isabs(samples_path):
                samples_path = os.path.join(current_dir, samples_path)
            cmd.extend(["--samples", samples_path])
            logger.info(f"Job {job_id}: Using samples file: {samples_path}")
        else:
            raise ValueError("Either samples_file_path or samples content must be provided")
        
        # Add optional arguments
        if request.single_patient:
            cmd.extend(["--single", request.single_patient])
        if request.json_output:
            cmd.append("--json_output")
        if request.resume:
            cmd.append("--resume")
        if request.refresh:
            cmd.append("--refresh")
        if request.skip_genomic:
            cmd.append("--skip_genomic")
        if request.skip_clinical:
            cmd.append("--skip_clinical")
        if request.skip_matching:
            cmd.append("--skip_matching")
        if request.id_filename != "samples.txt":
            cmd.extend(["--id_filename", request.id_filename])
        
        # Store command for debugging
        job_status[job_id]["command"] = " ".join(cmd)
        logger.info(f"Job {job_id}: Executing command: {' '.join(cmd)}")
        
        # Set environment variables to ensure consistent behavior
        env = os.environ.copy()
        env['PYTHONPATH'] = current_dir
        env['PYTHONUNBUFFERED'] = '1'  # Ensure output is not buffered
        
        # Run the pipeline with extended timeout and better error handling
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=7200,  # 2 hour timeout
            cwd=current_dir,  # Ensure we're in the right directory
            env=env
        )
        
        # Store stdout and stderr for debugging
        job_status[job_id]["stdout"] = result.stdout
        job_status[job_id]["stderr"] = result.stderr
        
        logger.info(f"Job {job_id}: Return code: {result.returncode}")
        logger.info(f"Job {job_id}: STDOUT: {result.stdout}")
        if result.stderr:
            logger.error(f"Job {job_id}: STDERR: {result.stderr}")
        
        if result.returncode == 0:
            # Success
            job_status[job_id]["status"] = "completed"
            job_status[job_id]["message"] = "Pipeline completed successfully"
            job_status[job_id]["completed_at"] = datetime.now().isoformat()
            
            # Determine output files based on request
            output_files = []
            if request.single_patient:
                if request.json_output:
                    output_files.append(f"genomic_data_parquet/matches_{request.single_patient}.json")
                else:
                    output_files.append(f"genomic_data_parquet/matches_scoring_{request.single_patient}.xlsx")
            else:
                if request.json_output:
                    output_files.append("genomic_data_parquet/matches_consolidated.json")
                else:
                    output_files.append("genomic_data_parquet/matches_scoring_consolidated.xlsx")
            
            # Always include these if they exist
            potential_files = [
                "genomic_data_parquet/retrieved_list.txt",
                "genomic_data_parquet/snv_cdss_input.parquet",
                "genomic_data_parquet/cnv_cdss_input.parquet",
                "genomic_data_parquet/fusion_cdss_input.parquet",
                "genomic_data_parquet/clinical_Details.parquet"
            ]
            
            existing_files = []
            for file_path in output_files + potential_files:
                full_path = os.path.join(current_dir, file_path)
                if os.path.exists(full_path):
                    existing_files.append(file_path)
            
            job_status[job_id]["output_files"] = existing_files
            logger.info(f"Job {job_id}: Found output files: {existing_files}")
        else:
            # Failure
            job_status[job_id]["status"] = "failed"
            job_status[job_id]["message"] = f"Pipeline execution failed with return code {result.returncode}"
            job_status[job_id]["error"] = result.stderr or "No error message available"
            job_status[job_id]["completed_at"] = datetime.now().isoformat()
            logger.error(f"Job {job_id}: Pipeline failed with return code {result.returncode}")
        
    except subprocess.TimeoutExpired:
        job_status[job_id]["status"] = "failed"
        job_status[job_id]["message"] = "Pipeline execution timed out"
        job_status[job_id]["error"] = "Process timed out after 2 hours"
        job_status[job_id]["completed_at"] = datetime.now().isoformat()
        logger.error(f"Job {job_id}: Pipeline timed out")
    except Exception as e:
        job_status[job_id]["status"] = "failed"
        job_status[job_id]["message"] = "Pipeline execution failed with exception"
        job_status[job_id]["error"] = str(e)
        job_status[job_id]["completed_at"] = datetime.now().isoformat()
        logger.error(f"Job {job_id}: Exception occurred: {str(e)}")
    finally:
        # Clean up temporary file
        if temp_samples_file:
            temp_path = os.path.join(os.getcwd(), temp_samples_file)
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                    logger.info(f"Job {job_id}: Cleaned up temp file: {temp_path}")
                except Exception as e:
                    logger.error(f"Job {job_id}: Failed to clean up temp file: {str(e)}")

@app.get("/")
async def root():
    return {"message": "OncoTwin Pipeline API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.post("/pipeline/run")
async def run_pipeline(
    background_tasks: BackgroundTasks,
    request: PipelineRequest
):
    """Run the pipeline with specified parameters"""
    # Generate unique job ID
    job_id = str(uuid.uuid4())
    
    # Initialize job status
    job_status[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "message": "Job queued for execution",
        "created_at": datetime.now().isoformat(),
        "completed_at": None,
        "error": None,
        "output_files": None,
        "stdout": None,
        "stderr": None,
        "command": None
    }
    
    # Start background task
    background_tasks.add_task(run_pipeline_async, job_id, request)
    
    return {"job_id": job_id, "message": "Pipeline job started", "status": "pending"}

@app.post("/pipeline/run-with-file")
async def run_pipeline_with_file(
    background_tasks: BackgroundTasks,
    samples_file: UploadFile = File(...),
    single_patient: Optional[str] = Form(None),
    json_output: bool = Form(False),
    resume: bool = Form(False),
    refresh: bool = Form(False),
    skip_genomic: bool = Form(False),
    skip_clinical: bool = Form(False),
    skip_matching: bool = Form(False),
    id_filename: str = Form("samples.txt")
):
    """Run the pipeline with uploaded samples file"""
    # Generate unique job ID
    job_id = str(uuid.uuid4())
    
    # Read uploaded file content
    try:
        samples_content = await samples_file.read()
        samples_content = samples_content.decode('utf-8')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading uploaded file: {str(e)}")
    
    # Create request object
    request = PipelineRequest(
        single_patient=single_patient,
        json_output=json_output,
        resume=resume,
        refresh=refresh,
        skip_genomic=skip_genomic,
        skip_clinical=skip_clinical,
        skip_matching=skip_matching,
        id_filename=id_filename
    )
    
    # Initialize job status
    job_status[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "message": "Job queued for execution",
        "created_at": datetime.now().isoformat(),
        "completed_at": None,
        "error": None,
        "output_files": None,
        "stdout": None,
        "stderr": None,
        "command": None
    }
    
    # Start background task
    background_tasks.add_task(run_pipeline_async, job_id, request, samples_content)
    
    return {"job_id": job_id, "message": "Pipeline job started with uploaded file", "status": "pending"}

@app.get("/pipeline/status/{job_id}")
async def get_job_status(job_id: str):
    """Get status of a specific job"""
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job_status[job_id]

@app.get("/pipeline/jobs")
async def list_jobs():
    """List all jobs and their status"""
    return {"jobs": list(job_status.values())}

@app.get("/pipeline/download/{job_id}/{filename}")
async def download_file(job_id: str, filename: str):
    """Download output file from a completed job"""
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = job_status[job_id]
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")
    
    if not job["output_files"] or filename not in [os.path.basename(f) for f in job["output_files"]]:
        raise HTTPException(status_code=404, detail="File not found in job outputs")
    
    # Find the full path
    file_path = None
    for output_file in job["output_files"]:
        if os.path.basename(output_file) == filename:
            file_path = output_file
            break
    
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found on disk")
    
    return FileResponse(file_path, filename=filename)

@app.get("/pipeline/results/{job_id}")
async def get_results(job_id: str):
    """Get results from a completed job (JSON format only)"""
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = job_status[job_id]
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail="Job not completed")
    
    # Look for JSON result files
    json_files = [f for f in job.get("output_files", []) if f.endswith('.json')]
    
    if not json_files:
        raise HTTPException(status_code=404, detail="No JSON results found for this job")
    
    results = {}
    for json_file in json_files:
        if os.path.exists(json_file):
            with open(json_file, 'r') as f:
                file_key = os.path.basename(json_file).replace('.json', '')
                results[file_key] = json.load(f)
    
    return results

@app.delete("/pipeline/job/{job_id}")
async def delete_job(job_id: str):
    """Delete a job and its status"""
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    del job_status[job_id]
    return {"message": f"Job {job_id} deleted successfully"}

@app.get("/pipeline/state")
async def get_pipeline_state():
    """Get current pipeline state information"""
    state_file = "pipeline_state.json"
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            state = json.load(f)
        return state
    else:
        return {"message": "No previous pipeline state found"}

@app.get("/pipeline/debug/{job_id}")
async def get_job_debug_info(job_id: str):
    """Get detailed debug information for a job"""
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = job_status[job_id]
    debug_info = {
        "job_status": job,
        "current_directory": os.getcwd(),
        "python_executable": sys.executable,
        "environment_vars": {
            "PYTHONPATH": os.environ.get("PYTHONPATH"),
            "PATH": os.environ.get("PATH"),
            "USER_EMAIL": os.environ.get("USER_EMAIL"),
            "USER_PASSWORD": os.environ.get("USER_PASSWORD")
        },
        "file_exists": {
            "run_pipeline_pq.py": os.path.exists("run_pipeline_pq.py"),
            "workbench_retrieval.py": os.path.exists("workbench_retrieval.py"),
            "ecrf_extract_pq.py": os.path.exists("ecrf_extract_pq.py"),
            "twin_algo_pq.py": os.path.exists("twin_algo_pq.py"),
            "key.json": os.path.exists("key.json"),
            ".env": os.path.exists(".env")
        }
    }
    
    return debug_info

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 