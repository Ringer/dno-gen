# Error Handling Policy - DNO Generator

## Data Integrity First

The DNO generator now follows a **fail-fast** policy to ensure data integrity.

## What Changed

### Previous Behavior ❌
- If an NPA failed to fetch (e.g., timeout, network error), the script would:
  - Print an error message
  - **Continue** to the next NPA
  - Generate output files with **incomplete data**
  - Risk: Incorrect DNO assignments due to missing data

### New Behavior ✅
- If any NPA fails to fetch, the script will:
  - Print a clear error message
  - **Exit immediately** (sys.exit(1))
  - Prevent writing incomplete data
  - Ensure data integrity

## Example Error Output

```
ERROR: Failed to process NPA 412: <urlopen error [Errno 60] Operation timed out>
Data integrity cannot be guaranteed with missing NPAs.
Please fix the issue and re-run the script.

To resume from NPA 412, you can modify the script to start from this NPA.
```

## Why This Matters

Missing even a single NPA means:
- Thousands of phone numbers could be incorrectly marked as unassigned
- These numbers might then be used for origination when they shouldn't be
- Potential regulatory compliance issues

## Retry Mechanism Still Active

Before failing, the script still attempts:
- **3 retry attempts** per API call
- Exponential backoff between retries
- 45-second timeout per request

Failures only occur after all retry attempts are exhausted.

## What To Do When It Fails

1. **Check the error message** - It will show which NPA failed
2. **Verify network connectivity**
3. **Check API status**
4. **Re-run the script** - It will start fresh

## Future Enhancement Ideas

If needed, we could add:
- Checkpoint/resume functionality to continue from failed NPA
- Automatic fallback to legacy method for failed NPAs
- Configurable failure tolerance (e.g., allow N failures)

For now, the fail-fast approach ensures we never generate incorrect data.
