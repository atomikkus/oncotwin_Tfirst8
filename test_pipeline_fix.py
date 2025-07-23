#!/usr/bin/env python3
"""
Test script to validate the pipeline fixes.
This tests the argument passing and file path handling without calling the actual APIs.
"""

import tempfile
import os
import subprocess
import sys

def test_workbench_retrieval_args():
    """Test that workbench_retrieval.py accepts the --samples argument correctly"""
    print("Testing workbench_retrieval.py argument handling...")
    
    # Create a temporary samples file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp_file:
        tmp_file.write("TEST001\nTEST002\nTEST003\n")
        tmp_samples_path = tmp_file.name
    
    try:
        # Test the help message to ensure argument is defined
        result = subprocess.run([
            sys.executable, 'workbench_retrieval.py', '--help'
        ], capture_output=True, text=True, timeout=10)
        
        if '--samples' in result.stdout:
            print("✓ workbench_retrieval.py correctly accepts --samples argument")
        else:
            print("✗ workbench_retrieval.py does not accept --samples argument")
            return False
            
        # Test that the script can read the samples file argument
        # (This will fail due to authentication, but should validate the file path)
        result = subprocess.run([
            sys.executable, 'workbench_retrieval.py', '--samples', tmp_samples_path
        ], capture_output=True, text=True, timeout=10)
        
        # Check if it attempted to load the file
        if f"Loading sample IDs from '{tmp_samples_path}'" in result.stdout:
            print("✓ workbench_retrieval.py correctly loads custom samples file")
            return True
        else:
            print("✗ workbench_retrieval.py failed to load custom samples file")
            print(f"stdout: {result.stdout}")
            print(f"stderr: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ workbench_retrieval.py test timed out")
        return False
    except Exception as e:
        print(f"✗ Error testing workbench_retrieval.py: {e}")
        return False
    finally:
        # Clean up temporary file
        if os.path.exists(tmp_samples_path):
            os.unlink(tmp_samples_path)

def test_run_pipeline_args():
    """Test that run_pipeline_pq.py passes arguments correctly"""
    print("\nTesting run_pipeline_pq.py argument handling...")
    
    # Create a temporary samples file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp_file:
        tmp_file.write("TEST001\nTEST002\nTEST003\n")
        tmp_samples_path = tmp_file.name
    
    try:
        # Test the help message
        result = subprocess.run([
            sys.executable, 'run_pipeline_pq.py', '--help'
        ], capture_output=True, text=True, timeout=10)
        
        if '--samples' in result.stdout:
            print("✓ run_pipeline_pq.py correctly accepts --samples argument")
        else:
            print("✗ run_pipeline_pq.py does not accept --samples argument")
            return False
            
        # Test dry run to check argument passing
        result = subprocess.run([
            sys.executable, 'run_pipeline_pq.py', 
            '--samples', tmp_samples_path,
            '--skip_clinical', '--skip_matching'
        ], capture_output=True, text=True, timeout=30)
        
        # Check if it passes the samples argument to workbench_retrieval.py
        expected_cmd = f"workbench_retrieval.py --samples {tmp_samples_path}"
        if expected_cmd in result.stdout:
            print("✓ run_pipeline_pq.py correctly passes --samples to workbench_retrieval.py")
            return True
        else:
            print("✗ run_pipeline_pq.py does not pass --samples correctly")
            print(f"stdout: {result.stdout}")
            print(f"stderr: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ run_pipeline_pq.py test timed out")
        return False
    except Exception as e:
        print(f"✗ Error testing run_pipeline_pq.py: {e}")
        return False
    finally:
        # Clean up temporary file
        if os.path.exists(tmp_samples_path):
            os.unlink(tmp_samples_path)

def main():
    print("=== Pipeline Fix Validation Test ===\n")
    
    test1_passed = test_workbench_retrieval_args()
    test2_passed = test_run_pipeline_args()
    
    print(f"\n=== Test Results ===")
    print(f"workbench_retrieval.py args: {'PASS' if test1_passed else 'FAIL'}")
    print(f"run_pipeline_pq.py args: {'PASS' if test2_passed else 'FAIL'}")
    
    if test1_passed and test2_passed:
        print("\n✓ All tests passed! Pipeline fixes are working correctly.")
        return 0
    else:
        print("\n✗ Some tests failed. Please check the pipeline configuration.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 