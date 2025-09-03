# DNO Generator Performance Guide

## Quick Start

The script now runs at **maximum speed by default** with no rate limiting:

```bash
python dno_gen.py
```

## Performance Settings

### Default Behavior (No Rate Limiting)
- The script runs at maximum speed by default
- No artificial delays between API calls
- Processes ~800 NXX combinations per NPA in ~50 seconds
- Estimated completion time for all 800 NPAs: ~11 hours

### Optional Environment Variables

```bash
# Enable debug output to see detailed progress
export DNO_DEBUG=true

# Enable rate limiting if you encounter API throttling (unlikely)
export DNO_RATE_LIMIT=true

# Run with both debug and rate limiting
export DNO_DEBUG=true DNO_RATE_LIMIT=true
python dno_gen.py
```

## Performance Metrics

Based on testing with the LERG API:

| Configuration | Time per NPA | Time for 800 NPAs | API Calls/Second |
|--------------|--------------|-------------------|------------------|
| Default (no rate limit) | ~50 seconds | ~11 hours | ~16/sec |
| With rate limiting | ~80 seconds | ~18 hours | ~10/sec |
| Old version (v1) | ~540 seconds | ~120 hours | ~1.5/sec |

## API Response Times

The LERG API is fast and stable:
- Average response time: **~60ms** per request
- Can handle 20+ requests/second without issues
- No rate limiting errors observed during testing

## Troubleshooting

If the script stalls or runs slowly:

1. **Check network connection** - Ensure stable internet connection
2. **Enable debug mode** - `export DNO_DEBUG=true` to see where it's stalling
3. **Try with rate limiting** - `export DNO_RATE_LIMIT=true` if you see 429 errors
4. **Check API token** - Ensure your API_TOKEN is valid in the .env file

## Upload to API

To automatically upload results after generation:

```bash
# Upload with confirmation prompt
python dno_gen.py --upload

# Skip confirmation (automated runs)
python dno_gen.py --upload --yes
```
