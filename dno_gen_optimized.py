#!/usr/bin/env python3
"""
Optimized DNO Generator using bulk API queries with pagination
This version fetches all npa,nxx,block_id combinations in a single query per NPA
instead of making separate calls for each NXX
"""
import urllib.request
import urllib.error
import csv
import json
import time
import sys
from datetime import datetime, timezone
import subprocess
import os
import argparse
from dotenv import load_dotenv
import socket

# Load environment variables from .env file
load_dotenv()

API_TOKEN = os.environ.get('API_TOKEN')
API_BASE_URL = 'https://api-dev.ringer.tel/v1/telique/lerg/lerg_6'

# Enable debug logging with environment variable
DEBUG_MODE = os.environ.get('DNO_DEBUG', '').lower() in ('true', '1', 'yes')

# Optimized settings for bulk fetching
MAX_LIMIT_PER_REQUEST = 10000  # Maximum allowed by API
REQUEST_TIMEOUT = 60  # Increased timeout for large responses

# Performance tracking
total_api_calls = 0
start_time = None


def make_api_request(url, headers=None):
    """Make an API request with proper error handling"""
    global total_api_calls
    total_api_calls += 1
    
    try:
        req = urllib.request.Request(url)
        if headers:
            for key, value in headers.items():
                req.add_header(key, value)
        
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as response:
            return json.loads(response.read().decode('utf-8'))
    
    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.reason} for URL: {url[:100]}...", file=sys.stderr)
        return None
    except urllib.error.URLError as e:
        print(f"URL Error: {e} for URL: {url[:100]}...", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return None


def fetch_all_blocks_for_npa_optimized(npa):
    """
    Fetch ALL npa,nxx,block_id combinations for a given NPA using bulk queries.
    This replaces the two-step process with a single paginated query.
    """
    all_records = []
    offset = 0
    page = 1
    
    if DEBUG_MODE:
        print(f"\n  Fetching all NPA-NXX-Block combinations for NPA {npa}")
        print(f"  Using optimized bulk fetch with limit={MAX_LIMIT_PER_REQUEST}")
    
    while True:
        # Direct query for all npa,nxx,block_id combinations for this NPA
        url = f"{API_BASE_URL}/npa,nxx,block_id/npa={npa}?limit={MAX_LIMIT_PER_REQUEST}&offset={offset}"
        
        if DEBUG_MODE:
            print(f"    Page {page}: Fetching records {offset}-{offset + MAX_LIMIT_PER_REQUEST}...")
        
        response_data = make_api_request(url, headers={'x-api-token': API_TOKEN})
        
        if response_data is None:
            print(f"  ERROR: Failed to fetch page {page} for NPA {npa}", file=sys.stderr)
            break
        
        data = response_data.get('data', [])
        
        if DEBUG_MODE and page == 1:
            total_unique = response_data.get('total_unique', 'N/A')
            print(f"    Total unique records available: {total_unique}")
        
        if not data:
            if DEBUG_MODE:
                print(f"    No more data at page {page}")
            break
        
        # Process the records
        for record in data:
            npa_val = str(record.get('npa', '')).zfill(3)
            nxx_val = str(record.get('nxx', '')).zfill(3)
            block_id = str(record.get('block_id', ''))
            
            if npa_val and nxx_val and block_id:
                all_records.append({
                    'npa': npa_val,
                    'nxx': nxx_val,
                    'block_id': block_id,
                    'full': f"{npa_val}-{nxx_val}-{block_id}"
                })
        
        records_fetched = len(data)
        
        if DEBUG_MODE:
            print(f"    Page {page}: Fetched {records_fetched} records (total so far: {len(all_records)})")
        
        # Check if we got less than the limit (last page)
        if records_fetched < MAX_LIMIT_PER_REQUEST:
            if DEBUG_MODE:
                print(f"    Last page reached (got {records_fetched} < {MAX_LIMIT_PER_REQUEST})")
            break
        
        offset += MAX_LIMIT_PER_REQUEST
        page += 1
    
    if DEBUG_MODE:
        print(f"  Total records fetched for NPA {npa}: {len(all_records)}")
        print(f"  Total API calls for this NPA: {page}")
    
    return all_records


def process_npa_records(records):
    """
    Process the raw records to determine assigned combinations.
    Handles the special 'A' block logic.
    """
    # Group by NPA-NXX to analyze blocks
    npa_nxx_blocks = {}
    
    for record in records:
        key = f"{record['npa']}-{record['nxx']}"
        if key not in npa_nxx_blocks:
            npa_nxx_blocks[key] = {'numeric': set(), 'has_a': False}
        
        if record['block_id'] == 'A':
            npa_nxx_blocks[key]['has_a'] = True
        else:
            npa_nxx_blocks[key]['numeric'].add(record['block_id'])
    
    # Build assigned list
    assigned = set()
    
    for npa_nxx, blocks in npa_nxx_blocks.items():
        if blocks['has_a'] and len(blocks['numeric']) == 0:
            # A-only: all blocks 0-9 are assigned
            for i in range(10):
                assigned.add(f"{npa_nxx}-{i}")
        else:
            # Add only the numeric blocks we found
            for block in blocks['numeric']:
                assigned.add(f"{npa_nxx}-{block}")
    
    return assigned, npa_nxx_blocks


def generate_all_possible_npa():
    """Generate all possible NPA codes (N=2-9, X=0-9, X=0-9)"""
    npas = []
    for n in range(2, 10):  # First digit: 2-9
        for x1 in range(0, 10):  # Second digit: 0-9
            for x2 in range(0, 10):  # Third digit: 0-9
                npas.append(f"{n}{x1}{x2}")
    return npas


def test_single_npa(npa="201"):
    """Test the optimized approach with a single NPA"""
    print(f"\nTesting optimized approach with NPA {npa}...")
    
    start = time.time()
    records = fetch_all_blocks_for_npa_optimized(npa)
    fetch_time = time.time() - start
    
    assigned, npa_nxx_blocks = process_npa_records(records)
    total_time = time.time() - start
    
    print(f"\n✅ Results for NPA {npa}:")
    print(f"  - Fetch time: {fetch_time:.2f}s")
    print(f"  - Total time: {total_time:.2f}s")
    print(f"  - API calls made: {total_api_calls}")
    print(f"  - Raw records fetched: {len(records)}")
    print(f"  - Unique NPA-NXX combinations: {len(npa_nxx_blocks)}")
    print(f"  - Assigned combinations: {len(assigned)}")
    
    # Show performance comparison
    old_api_calls = len(npa_nxx_blocks) + 1  # Old method: 1 for NXX list + 1 per NXX
    print(f"\n📊 Performance Comparison:")
    print(f"  - Old approach: ~{old_api_calls} API calls")
    print(f"  - New approach: {total_api_calls} API calls")
    print(f"  - Reduction: {(1 - total_api_calls/old_api_calls)*100:.1f}%")
    
    return records, assigned


def main():
    """Main function for testing the optimized approach"""
    global start_time, total_api_calls
    
    parser = argparse.ArgumentParser(description='Test optimized DNO generation')
    parser.add_argument('--npa', help='Test with specific NPA (default: 201)', default='201')
    parser.add_argument('--all', action='store_true', help='Process all NPAs (WARNING: ~8+ hours)')
    parser.add_argument('--compare', action='store_true', help='Compare with old approach')
    args = parser.parse_args()
    
    if not API_TOKEN:
        print("ERROR: API_TOKEN not found in .env file")
        sys.exit(1)
    
    print("=" * 60)
    print("DNO GENERATOR - OPTIMIZED BULK FETCH APPROACH")
    print("=" * 60)
    print(f"API Max Limit: {MAX_LIMIT_PER_REQUEST} records per request")
    print(f"Debug Mode: {'ENABLED' if DEBUG_MODE else 'DISABLED'}")
    
    if args.compare:
        # Compare old vs new approach
        print("\n📊 PERFORMANCE COMPARISON TEST")
        print("-" * 40)
        
        # Test old approach
        print("\n1. Old Two-Step Approach:")
        from dno_gen import fetch_assigned_for_npa
        
        total_api_calls = 0  # Reset counter
        start = time.time()
        old_assigned, old_blocks = fetch_assigned_for_npa(args.npa)
        old_time = time.time() - start
        old_api_calls = total_api_calls
        
        # Test new approach
        print("\n2. New Bulk Fetch Approach:")
        total_api_calls = 0  # Reset counter
        start = time.time()
        records = fetch_all_blocks_for_npa_optimized(args.npa)
        new_assigned, new_blocks = process_npa_records(records)
        new_time = time.time() - start
        new_api_calls = total_api_calls
        
        # Compare results
        print("\n" + "=" * 60)
        print("COMPARISON RESULTS")
        print("=" * 60)
        print(f"NPA {args.npa} Processing:")
        print(f"  Old approach: {old_time:.2f}s with {old_api_calls} API calls")
        print(f"  New approach: {new_time:.2f}s with {new_api_calls} API calls")
        print(f"  Speed improvement: {old_time/new_time:.1f}x faster")
        print(f"  API call reduction: {(1 - new_api_calls/old_api_calls)*100:.1f}%")
        print(f"\nData consistency check:")
        print(f"  Old: {len(old_assigned)} assigned combinations")
        print(f"  New: {len(new_assigned)} assigned combinations")
        if old_assigned == new_assigned:
            print("  ✅ Results match perfectly!")
        else:
            print(f"  ⚠️ Difference detected: {len(old_assigned.symmetric_difference(new_assigned))} differences")
    
    elif args.all:
        print("\n⚠️ WARNING: Processing all 800 NPAs")
        confirm = input("This will take several hours. Continue? (y/N): ")
        if confirm.lower() != 'y':
            print("Cancelled.")
            return
        
        all_npas = generate_all_possible_npa()
        all_assigned = set()
        
        start_time = time.time()
        
        for i, npa in enumerate(all_npas, 1):
            npa_start = time.time()
            
            print(f"\nProcessing NPA {npa} ({i}/{len(all_npas)})...")
            
            records = fetch_all_blocks_for_npa_optimized(npa)
            assigned, _ = process_npa_records(records)
            all_assigned.update(assigned)
            
            npa_time = time.time() - npa_start
            elapsed = time.time() - start_time
            avg_time = elapsed / i
            eta = avg_time * (len(all_npas) - i)
            
            print(f"  Completed in {npa_time:.1f}s | Total: {len(all_assigned)} | ETA: {eta/60:.1f} min")
        
        total_time = time.time() - start_time
        print(f"\n✅ Completed all NPAs in {total_time/60:.1f} minutes")
        print(f"  Total assigned combinations: {len(all_assigned)}")
        print(f"  Total API calls: {total_api_calls}")
        print(f"  Average API calls per NPA: {total_api_calls/len(all_npas):.1f}")
    
    else:
        # Test single NPA
        test_single_npa(args.npa)
        
        # Estimate for all NPAs
        estimated_time = (total_api_calls * 0.1 * 800) / 60  # Assuming 100ms per API call
        print(f"\n📈 Estimated time for all 800 NPAs:")
        print(f"  - API calls: ~{total_api_calls * 800:,}")
        print(f"  - Time: ~{estimated_time:.1f} minutes")


if __name__ == "__main__":
    main()
