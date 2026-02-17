Debug a failed or misbehaving pipeline run.

## Arguments
- Output directory path (required) - the pipeline's output directory

## User's request
Debug pipeline at: $ARGUMENTS

## Instructions

1. **Check pipeline status**:
   ```bash
   tigerflow status <output_dir>
   ```

2. **Examine the output directory structure**:
   - Look for `.err` files (task failures with tracebacks)
   - Check `.pid` file (process info)
   - Count completed vs pending files

3. **Read error files**:
   - Find all `*.err` files and read their contents
   - Identify common failure patterns

4. **Check logs** (if available):
   - Look for log files in the output directory
   - Search for ERROR or WARNING entries

5. **For Slurm jobs**:
   - Check job status: `squeue -u $USER` or `sacct -j <job_id>`
   - Look for Slurm output files (`slurm-*.out`) in the working directory
   - Check Dask scheduler/worker logs if using distributed processing
   - Common Slurm issues:
     - Job pending due to resource limits (`squeue` shows PD state)
     - Job failed due to time limit (`sacct` shows TIMEOUT)
     - Node failures (`sacct` shows NODE_FAIL)
     - Memory exceeded (`sacct` shows OUT_OF_MEMORY)
   - Check cluster-specific logs: `scontrol show job <job_id>`

6. **Analyze the issue**:
   - Is it a code bug in a task's `run()` method?
   - Is it a configuration issue (wrong extensions, missing dirs)?
   - Is it a resource issue (permissions, disk space, Slurm allocation)?

7. **Suggest fixes**:
   - Point to specific code locations if it's a task bug
   - Suggest config changes if it's configuration
   - Recommend cleanup steps if needed (remove `.err` files to retry)
   - For Slurm: suggest partition changes, memory/time adjustments

8. **If the pipeline is stuck** (not processing new files):
   - Check if the process is running: `ps aux | grep tigerflow`
   - For Slurm: verify jobs are running with `squeue`
   - Check for file permission issues
   - Verify input files match expected `input_ext`
