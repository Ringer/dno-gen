#!/usr/bin/env python3
import urllib.request
import urllib.error
import csv
import json
from itertools import product
import time
import sys
from datetime import datetime, timezone
import subprocess
import os
import argparse
from dotenv import load_dotenv
import random
import socket

# Load environment variables from .env file
load_dotenv()

# SendGrid imports (optional - will work without if not installed)
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Content, From, To
    SENDGRID_AVAILABLE = True
except ImportError:
    SENDGRID_AVAILABLE = False
    print("Note: SendGrid not installed. Email notifications will be skipped.")
    print("      Install with: pip install sendgrid")

API_TOKEN = os.environ.get('API_TOKEN')
API_BASE_URL = 'https://api-dev.ringer.tel/v1/telique/lerg/lerg_6'

# SendGrid configuration
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
SENDGRID_FROM_EMAIL = os.environ.get('SENDGRID_FROM_EMAIL', 'dno-generator@teliax.com')
SENDGRID_TO_EMAIL = os.environ.get('SENDGRID_TO_EMAIL', 'engineering@teliax.com')
ENABLE_EMAIL_NOTIFICATIONS = os.environ.get('DNO_EMAIL_NOTIFICATIONS', 'true').lower() in ('true', '1', 'yes')

# Enable debug logging with environment variable
DEBUG_MODE = os.environ.get('DNO_DEBUG', '').lower() in ('true', '1', 'yes')

# API retry configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1.0  # seconds
MAX_RETRY_DELAY = 30.0  # seconds
REQUEST_TIMEOUT = 45  # increased from 30

# Rate limiting configuration (DISABLED by default)
# The API responds quickly (~60ms) and handles rapid requests well
# To enable rate limiting, set environment variable: DNO_RATE_LIMIT=true
API_CALL_DELAY = 0.01  # Minimal delay between API calls (10ms) when enabled
BATCH_DELAY = 0.1  # Small delay after batch of requests (100ms) when enabled
BATCH_SIZE = 100  # Larger batch size since API is fast

# Bulk fetch configuration
MAX_LIMIT_PER_REQUEST = 10000  # Maximum records per API request (API limit)
USE_BULK_FETCH = os.environ.get('DNO_BULK_FETCH', 'true').lower() in ('true', '1', 'yes')

# Global request counter for rate limiting
request_counter = 0
last_request_time = 0

def generate_all_possible_npa():
    """Generate all possible NPA codes (N=2-9, X=0-9, X=0-9)"""
    npas = []
    for n in range(2, 10):  # First digit: 2-9
        for x1 in range(0, 10):  # Second digit: 0-9
            for x2 in range(0, 10):  # Third digit: 0-9
                npas.append(f"{n}{x1}{x2}")
    return npas

def generate_all_possible_npa_nxx_block():
    """Generate all possible NPA-NXX-block_id combinations (numeric blocks only)"""
    combinations = set()
    # NPA: N=2-9, X=0-9, X=0-9
    for n1 in range(2, 10):
        for x1 in range(0, 10):
            for x2 in range(0, 10):
                npa = f"{n1}{x1}{x2}"
                # NXX: N=2-9, X=0-9, X=0-9
                for n2 in range(2, 10):
                    for x3 in range(0, 10):
                        for x4 in range(0, 10):
                            nxx = f"{n2}{x3}{x4}"
                            # block_id: 0-9 only (excluding A)
                            for block in range(0, 10):
                                combinations.add(f"{npa}-{nxx}-{block}")
    return combinations

def apply_rate_limiting():
    """Apply minimal rate limiting between API calls"""
    global request_counter, last_request_time
    
    # Rate limiting is DISABLED by default - only enable if explicitly requested
    # Set DNO_RATE_LIMIT=true to enable rate limiting if needed
    if not os.environ.get('DNO_RATE_LIMIT', '').lower() in ('true', '1', 'yes'):
        return  # Skip rate limiting by default
    
    current_time = time.time()
    time_since_last = current_time - last_request_time
    
    # Apply minimum delay between requests (only if needed)
    if time_since_last < API_CALL_DELAY:
        sleep_time = API_CALL_DELAY - time_since_last
        # Only log if delay is significant (> 50ms)
        if DEBUG_MODE and sleep_time > 0.05:
            print(f"    DEBUG: Rate limiting - sleeping {sleep_time:.3f}s")
        time.sleep(sleep_time)
    
    request_counter += 1
    
    # Apply longer delay after batch of requests
    if request_counter % BATCH_SIZE == 0:
        if DEBUG_MODE:
            print(f"    DEBUG: Batch pause - {BATCH_DELAY}s after {request_counter} requests")
        time.sleep(BATCH_DELAY)
    
    last_request_time = time.time()

def make_api_request_with_retry(url, headers=None):
    """Make an API request with retry logic and exponential backoff"""
    retry_delay = INITIAL_RETRY_DELAY
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            # Apply rate limiting (only on first attempt to avoid excessive delays on retries)
            if attempt == 0:
                apply_rate_limiting()
            
            if DEBUG_MODE and attempt > 0:
                print(f"    DEBUG: Retry attempt {attempt + 1}/{MAX_RETRIES} for URL: {url[:100]}...")
            
            req = urllib.request.Request(url)
            if headers:
                for key, value in headers.items():
                    req.add_header(key, value)
            
            # Set socket timeout as well as urlopen timeout
            socket.setdefaulttimeout(REQUEST_TIMEOUT)
            
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as response:
                response_data = response.read().decode('utf-8')
                return json.loads(response_data)
                
        except (urllib.error.URLError, socket.timeout, socket.error) as e:
            last_exception = e
            error_msg = str(e)
            
            # Check if it's a timeout
            if 'timed out' in error_msg.lower() or isinstance(e, socket.timeout):
                print(f"\n  WARNING: Request timeout (attempt {attempt + 1}/{MAX_RETRIES}): {url[:80]}...", file=sys.stderr)
            # Check if it's a rate limit error (429)
            elif hasattr(e, 'code') and e.code == 429:
                print(f"\n  WARNING: Rate limited (attempt {attempt + 1}/{MAX_RETRIES}), backing off...", file=sys.stderr)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)  # Double the delay
            else:
                print(f"\n  WARNING: Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {error_msg}", file=sys.stderr)
            
            if attempt < MAX_RETRIES - 1:
                # Add jitter to prevent thundering herd
                jittered_delay = retry_delay + random.uniform(0, retry_delay * 0.1)
                print(f"  Retrying in {jittered_delay:.1f} seconds...", file=sys.stderr)
                time.sleep(jittered_delay)
                retry_delay = min(retry_delay * 1.5, MAX_RETRY_DELAY)  # Exponential backoff
            else:
                print(f"\n  ERROR: All {MAX_RETRIES} attempts failed for URL: {url[:100]}...", file=sys.stderr)
                raise last_exception
                
        except json.JSONDecodeError as e:
            print(f"\n  ERROR: Failed to parse JSON response: {e}", file=sys.stderr)
            if attempt < MAX_RETRIES - 1:
                print(f"  Retrying in {retry_delay:.1f} seconds...", file=sys.stderr)
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, MAX_RETRY_DELAY)
            else:
                raise e
    
    return None

def fetch_nxx_combinations_for_npa(npa):
    """Fetch all NXX combinations for a given NPA (Step 1)"""
    nxx_combinations = set()
    offset = 0
    limit = 1000
    total_fetched = 0
    
    while True:
        url = f"https://api-dev.ringer.tel/v1/telique/lerg/lerg_6/npa,nxx/npa={npa}?limit={limit}&offset={offset}"
        
        try:
            if DEBUG_MODE:
                print(f"  DEBUG: Fetching NXX for NPA {npa}, offset={offset}, limit={limit}")
            
            response_data = make_api_request_with_retry(url, headers={'x-api-token': API_TOKEN})
            
            if response_data is None:
                error_msg = f"Failed to fetch NXX data for NPA {npa} at offset {offset} - API request failed after {MAX_RETRIES} attempts"
                print(f"\n  ERROR: {error_msg}", file=sys.stderr)
                raise Exception(error_msg)
            
            data = response_data.get('data', [])
            
            if DEBUG_MODE and offset == 0:
                print(f"  DEBUG: NPA {npa} - Step 1: Total unique NXX combinations: {response_data.get('total_unique', 'N/A')}")
            
            if not data or len(data) == 0:
                if DEBUG_MODE:
                    print(f"  DEBUG: No more data for NPA {npa} at offset {offset}")
                break
                
            for record in data:
                npa_val = record.get('npa', '')
                nxx_val = record.get('nxx', '')
                
                if npa_val and nxx_val:
                    npa_str = str(npa_val).zfill(3)
                    nxx_str = str(nxx_val).zfill(3)
                    nxx_combinations.add(f"{npa_str}-{nxx_str}")
            
            total_fetched += len(data)
            
            if DEBUG_MODE:
                print(f"  DEBUG: Fetched {len(data)} records, total so far: {total_fetched}, unique NXX: {len(nxx_combinations)}")
            
            if len(data) < limit:
                if DEBUG_MODE:
                    print(f"  DEBUG: Last page reached for NPA {npa} (got {len(data)} < {limit})")
                break
                
            offset += limit
            
        except Exception as e:
            print(f"\n  ERROR: Unexpected error fetching NXX for NPA {npa}: {e}", file=sys.stderr)
            break
    
    if DEBUG_MODE:
        print(f"  DEBUG: Completed NPA {npa} - found {len(nxx_combinations)} unique NXX combinations")
    
    return nxx_combinations

def fetch_blocks_for_npa_nxx(npa, nxx):
    """Fetch all block_id values for a specific NPA-NXX combination (Step 2)"""
    blocks = {'numeric': set(), 'has_a': False}
    offset = 0
    limit = 1000
    
    while True:
        url = f"https://api-dev.ringer.tel/v1/telique/lerg/lerg_6/npa,nxx,block_id/npa={npa}&nxx={nxx}?limit={limit}&offset={offset}"
        
        try:
            response_data = make_api_request_with_retry(url, headers={'x-api-token': API_TOKEN})
            
            if response_data is None:
                error_msg = f"Failed to fetch blocks for NPA {npa}-NXX {nxx} at offset {offset} - API request failed after {MAX_RETRIES} attempts"
                print(f"\n  ERROR: {error_msg}", file=sys.stderr)
                raise Exception(error_msg)
            
            data = response_data.get('data', [])
            
            if not data or len(data) == 0:
                break
                
            for record in data:
                block_id_val = str(record.get('block_id', ''))
                
                if block_id_val:
                    if block_id_val == 'A':
                        blocks['has_a'] = True
                    else:
                        blocks['numeric'].add(block_id_val)
            
            if len(data) < limit:
                break
                
            offset += limit
            
        except Exception as e:
            print(f"\n  ERROR: Unexpected error fetching blocks for NPA {npa}-NXX {nxx}: {e}", file=sys.stderr)
            break
    
    return blocks

def fetch_assigned_for_npa_bulk(npa):
    """
    Optimized: Fetch ALL npa,nxx,block_id combinations for a given NPA in bulk.
    Uses pagination with maximum limit to minimize API calls.
    """
    all_records = []
    offset = 0
    page = 1
    
    if DEBUG_MODE:
        print(f"  DEBUG: NPA {npa} - Using BULK fetch (limit={MAX_LIMIT_PER_REQUEST})")
    
    while True:
        # Direct query for all npa,nxx,block_id combinations
        url = f"{API_BASE_URL}/npa,nxx,block_id/npa={npa}?limit={MAX_LIMIT_PER_REQUEST}&offset={offset}"
        
        response_data = make_api_request_with_retry(url, headers={'x-api-token': API_TOKEN})
        
        if response_data is None:
            error_msg = f"Failed to fetch page {page} for NPA {npa} - API request failed after {MAX_RETRIES} attempts"
            print(f"\n  ERROR: {error_msg}", file=sys.stderr)
            raise Exception(error_msg)
        
        data = response_data.get('data', [])
        
        if DEBUG_MODE and page == 1:
            total_unique = response_data.get('total_unique', 'N/A')
            print(f"  DEBUG: NPA {npa} - Total unique records: {total_unique}")
        
        if not data:
            break
        
        all_records.extend(data)
        
        if DEBUG_MODE:
            print(f"  DEBUG: NPA {npa} - Page {page}: Fetched {len(data)} records (total: {len(all_records)})")
        
        if len(data) < MAX_LIMIT_PER_REQUEST:
            break
        
        offset += MAX_LIMIT_PER_REQUEST
        page += 1
    
    # Process records to build assigned combinations
    npa_nxx_blocks = {}
    assigned = []
    
    for record in all_records:
        npa_val = str(record.get('npa', '')).zfill(3)
        nxx_val = str(record.get('nxx', '')).zfill(3)
        block_id = str(record.get('block_id', ''))
        
        if npa_val and nxx_val and block_id:
            key = f"{npa_val}-{nxx_val}"
            
            if key not in npa_nxx_blocks:
                npa_nxx_blocks[key] = {'numeric': set(), 'has_a': False}
            
            if block_id == 'A':
                npa_nxx_blocks[key]['has_a'] = True
            else:
                npa_nxx_blocks[key]['numeric'].add(block_id)
    
    # Handle A-block logic
    for npa_nxx_key, block_info in npa_nxx_blocks.items():
        if block_info['has_a'] and len(block_info['numeric']) == 0:
            # A-only: all blocks 0-9 are assigned
            for i in range(10):
                assigned.append(f"{npa_nxx_key}-{i}")
        else:
            # Add only the numeric blocks
            for block in block_info['numeric']:
                assigned.append(f"{npa_nxx_key}-{block}")
    
    if DEBUG_MODE:
        print(f"  DEBUG: NPA {npa} - API calls: {page}, Records: {len(all_records)}, Assigned: {len(assigned)}")
    
    return set(assigned), npa_nxx_blocks

def fetch_assigned_for_npa_legacy(npa):
    """Legacy: Fetch all assigned NPA-NXX-block_id combinations using two-step approach"""
    assigned = []
    
    # Step 1: Get all NXX combinations for this NPA
    if DEBUG_MODE:
        print(f"  DEBUG: NPA {npa} - Step 1: Fetching NXX combinations...")
    
    nxx_combinations = fetch_nxx_combinations_for_npa(npa)
    
    if DEBUG_MODE:
        print(f"  DEBUG: NPA {npa} - Step 1: Found {len(nxx_combinations)} NXX combinations")
    
    # Step 2: For each NPA-NXX, get all blocks
    npa_nxx_blocks = {}
    
    if DEBUG_MODE:
        print(f"  DEBUG: NPA {npa} - Step 2: Fetching blocks for {len(nxx_combinations)} NXX combinations...")
    
    for i, npa_nxx in enumerate(sorted(nxx_combinations), 1):
        npa_part, nxx_part = npa_nxx.split('-')
        
        if DEBUG_MODE:
            if i <= 5 or i % 10 == 0 or i == len(nxx_combinations):  # Show progress periodically
                print(f"  DEBUG: NPA {npa} - Step 2: Progress {i}/{len(nxx_combinations)} - Fetching blocks for {npa_nxx}")
        
        blocks = fetch_blocks_for_npa_nxx(npa_part, nxx_part)
        npa_nxx_blocks[npa_nxx] = blocks
        
        # Add numeric blocks to assigned list
        for block_id in blocks['numeric']:
            assigned.append(f"{npa_nxx}-{block_id}")
        
        # Progress indicator for non-debug mode
        if not DEBUG_MODE and i % 50 == 0:
            print(f"    Progress: {i}/{len(nxx_combinations)} NXX processed for NPA {npa}")
    
    # Process A-block handling (unchanged logic)
    a_only_count = 0
    for npa_nxx_key, block_info in npa_nxx_blocks.items():
        if block_info['has_a'] and len(block_info['numeric']) == 0:
            # This NPA-NXX only has 'A' record, no numeric blocks
            # Add all blocks 0-9 as assigned
            a_only_count += 1
            
            for block in range(0, 10):
                assigned.append(f"{npa_nxx_key}-{block}")
    
    if DEBUG_MODE:
        print(f"  DEBUG: NPA {npa} - Found {a_only_count} NPA-NXX with A-only blocks")
        print(f"  DEBUG: NPA {npa} - Total assigned: {len(assigned)}")
    
    # Convert to set to ensure uniqueness before returning
    return set(assigned), npa_nxx_blocks

def fetch_assigned_for_npa(npa):
    """Wrapper function for backwards compatibility - uses bulk fetch by default"""
    if USE_BULK_FETCH:
        return fetch_assigned_for_npa_bulk(npa)
    else:
        return fetch_assigned_for_npa_legacy(npa)

def fetch_itg_traceback_data():
    """
    Fetch ITG traceback data from BigQuery and normalize phone numbers.
    Returns a list of dictionaries with normalized phone data.
    """
    itg_data = []
    
    try:
        # Execute BigQuery command with max_rows to get all records
        cmd = "bq query --use_legacy_sql=false --max_rows=10000 'SELECT phoneNumber, createDate FROM `teliax.com:teliax.DNO.2025_08`'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            print(f"Error executing BigQuery: {result.stderr}", file=sys.stderr)
            return itg_data
        
        # Parse the output
        lines = result.stdout.strip().split('\n')
        
        # Skip header lines (table formatting)
        data_started = False
        for line in lines:
            if '|' not in line:
                continue
            
            parts = [p.strip() for p in line.split('|')]
            
            # Skip header and separator lines
            if 'phoneNumber' in parts or '---' in line:
                data_started = True
                continue
            
            if not data_started or len(parts) < 3:
                continue
            
            phone_number = parts[1]
            create_date = parts[2]
            
            if phone_number and create_date:
                # Normalize phone number
                # Remove country code if present
                if phone_number.startswith('1') and len(phone_number) == 11:
                    phone_number = phone_number[1:]
                
                # Special handling for short codes
                if len(phone_number) < 10:
                    # For short codes like 611, we'll store them as-is
                    itg_data.append({
                        'digits': phone_number,
                        'src': 'ITG',
                        'created_at': create_date
                    })
                elif len(phone_number) == 10:
                    # Store the full 10-digit phone number
                    itg_data.append({
                        'digits': phone_number,
                        'src': 'ITG',
                        'created_at': create_date
                    })
        
        print(f"\nFetched {len(itg_data)} ITG traceback records")
        
    except subprocess.TimeoutExpired:
        print("BigQuery command timed out", file=sys.stderr)
    except Exception as e:
        print(f"Error fetching ITG data: {e}", file=sys.stderr)
    
    return itg_data

def upload_to_api(file_path, api_url="https://api-dev.ringer.tel/v1/telique/admin/dno/upload"):
    """
    Upload CSV file to the API endpoint using curl command.
    Returns True if successful, False otherwise.
    """
    try:
        print(f"\nUploading {file_path} to API...")
        
        # Build the curl command
        cmd = [
            'curl',
            '-s',  # Silent mode
            '-H', f'x-api-token: {API_TOKEN}',
            api_url,
            '-F', f'file=@{file_path}',
            '-w', '\\n%{http_code}'  # Output HTTP status code
        ]
        
        # Execute the command
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        # Split output and status code
        output_lines = result.stdout.strip().split('\n')
        http_code = output_lines[-1] if output_lines else ''
        response_body = '\n'.join(output_lines[:-1]) if len(output_lines) > 1 else ''
        
        if http_code == '200':
            print(f"✓ Upload successful (HTTP {http_code})")
            if response_body:
                print(f"  Response: {response_body}")
            return True
        else:
            print(f"✗ Upload failed (HTTP {http_code})")
            if response_body:
                print(f"  Response: {response_body}")
            if result.stderr:
                print(f"  Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ Upload timed out after 60 seconds")
        return False
    except Exception as e:
        print(f"✗ Upload error: {e}")
        return False

def send_email_notification(subject, html_content, text_content=None):
    """
    Send email notification using SendGrid
    
    Args:
        subject: Email subject
        html_content: HTML content of the email
        text_content: Plain text content (optional, will be derived from HTML if not provided)
    
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    if not ENABLE_EMAIL_NOTIFICATIONS:
        if DEBUG_MODE:
            print("Email notifications disabled (DNO_EMAIL_NOTIFICATIONS=false)")
        return False
    
    if not SENDGRID_AVAILABLE:
        if DEBUG_MODE:
            print("SendGrid not available - skipping email notification")
        return False
    
    if not SENDGRID_API_KEY:
        print("Warning: SENDGRID_API_KEY not found in environment - skipping email notification")
        return False
    
    try:
        sg = SendGridAPIClient(api_key=SENDGRID_API_KEY)
        
        # Create email
        from_email = From(SENDGRID_FROM_EMAIL)
        to_email = To(SENDGRID_TO_EMAIL)
        
        # If text content not provided, create simple version
        if text_content is None:
            text_content = html_content.replace('<br>', '\n').replace('<b>', '').replace('</b>', '')
            # Remove HTML tags
            import re
            text_content = re.sub('<[^<]+?>', '', text_content)
        
        mail = Mail(
            from_email=from_email,
            to_emails=to_email,
            subject=subject,
            plain_text_content=text_content,
            html_content=html_content
        )
        
        # Send email
        response = sg.send(mail)
        
        if response.status_code == 202:
            print(f"✓ Email notification sent to {SENDGRID_TO_EMAIL}")
            return True
        else:
            print(f"Warning: Email send returned status {response.status_code}")
            return False
            
    except Exception as e:
        error_msg = str(e)
        if "401" in error_msg or "Unauthorized" in error_msg:
            print(f"Warning: SendGrid API authentication failed. Please verify your API key.")
            print(f"  Current key starts with: {SENDGRID_API_KEY[:10]}..." if SENDGRID_API_KEY else "  No key found")
        elif "403" in error_msg:
            print(f"Warning: SendGrid API forbidden. Check sender email permissions.")
        else:
            print(f"Warning: Failed to send email notification: {e}")
        if DEBUG_MODE:
            print(f"  From: {SENDGRID_FROM_EMAIL}")
            print(f"  To: {SENDGRID_TO_EMAIL}")
        return False

def send_success_email(stats):
    """
    Send success email with DNO generation statistics
    
    Args:
        stats: Dictionary containing run statistics
    """
    runtime = stats.get('runtime', 0)
    runtime_str = f"{runtime/60:.1f} minutes" if runtime > 60 else f"{runtime:.0f} seconds"
    
    # Customize subject based on upload status
    if stats.get('upload_requested', False):
        if stats.get('upload_success', False):
            subject = f"✅ DNO Generation Completed & Uploaded - {stats.get('total_assigned', 0):,} Assigned"
        elif stats.get('upload_attempted', False) and not stats.get('upload_success', False):
            subject = f"⚠️ DNO Generation Completed (Upload Failed) - {stats.get('total_assigned', 0):,} Assigned"
        elif stats.get('upload_cancelled', False):
            subject = f"✅ DNO Generation Completed (Upload Skipped) - {stats.get('total_assigned', 0):,} Assigned"
        else:
            subject = f"✅ DNO Generation Completed - {stats.get('total_assigned', 0):,} Assigned"
    else:
        subject = f"✅ DNO Generation Completed - {stats.get('total_assigned', 0):,} Assigned"
    
    # Determine upload status message
    upload_section = ""
    if stats.get('upload_requested', False):
        if stats.get('upload_cancelled', False):
            upload_section = """
    <h3>Upload Status</h3>
    <ul>
        <li>⚠️ <b>Upload cancelled by user</b></li>
    </ul>
    """
        elif stats.get('upload_success', False):
            upload_section = """
    <h3>Upload Status</h3>
    <ul>
        <li>✅ <b>Successfully uploaded to DNO API</b></li>
        <li>File: unassigned_npa_nxx_x.csv</li>
    </ul>
    """
        elif stats.get('upload_attempted', False):
            upload_section = """
    <h3>Upload Status</h3>
    <ul>
        <li>❌ <b>Failed to upload to DNO API</b></li>
        <li>Manual upload may be required</li>
    </ul>
    """
        else:
            upload_section = """
    <h3>Upload Status</h3>
    <ul>
        <li>⚠️ <b>Upload was requested but not attempted</b></li>
    </ul>
    """
    
    html_content = f"""
    <h2>DNO Generation Completed Successfully</h2>
    
    <h3>Summary</h3>
    <ul>
        <li><b>Runtime:</b> {runtime_str}</li>
        <li><b>NPAs Processed:</b> {stats.get('npas_processed', 0)}</li>
        <li><b>API Calls Made:</b> {stats.get('api_calls', 'N/A')}</li>
        <li><b>Fetch Mode:</b> {stats.get('fetch_mode', 'BULK')}</li>
    </ul>
    
    <h3>Results</h3>
    <ul>
        <li><b>Total Theoretically Possible:</b> {stats.get('total_possible', 0):,}</li>
        <li><b>Currently Assigned (LERG):</b> {stats.get('total_assigned', 0):,} ({stats.get('assigned_percent', 0):.2f}%)</li>
        <li><b>Currently Unassigned:</b> {stats.get('total_unassigned', 0):,} ({stats.get('unassigned_percent', 0):.2f}%)</li>
        <li><b>Condensed Unassigned Entries:</b> {stats.get('condensed_unassigned', 0):,}</li>
        <li><b>ITG Traceback Records:</b> {stats.get('itg_records', 0):,}</li>
    </ul>
    
    <h3>Output Files Generated</h3>
    <ul>
        <li>assigned_npa_nxx_x.csv - {stats.get('total_assigned', 0):,} records</li>
        <li>unassigned_npa_nxx_x.csv - {stats.get('total_output_records', 0):,} records</li>
        <li>a_block_analysis.csv</li>
        <li>lerg_summary.csv</li>
    </ul>
    
    {upload_section}
    
    <p><small>Generated at {datetime.now(timezone.utc).isoformat()}</small></p>
    """
    
    send_email_notification(subject, html_content)

def send_failure_email(npa, error_message, stats=None):
    """
    Send failure email with error details
    
    Args:
        npa: The NPA that failed
        error_message: The error message
        stats: Optional statistics dictionary for partial progress
    """
    subject = f"❌ DNO Generation Failed at NPA {npa}"
    
    progress = ""
    if stats:
        progress = f"""
        <h3>Progress Before Failure</h3>
        <ul>
            <li><b>NPAs Processed:</b> {stats.get('npas_processed', 0)} of {stats.get('total_npas', 800)}</li>
            <li><b>Elapsed Time:</b> {stats.get('runtime', 0):.1f} seconds</li>
            <li><b>Records Collected:</b> {stats.get('records_collected', 0):,}</li>
        </ul>
        """
    
    html_content = f"""
    <h2>DNO Generation Failed</h2>
    
    <h3>Error Details</h3>
    <ul>
        <li><b>Failed NPA:</b> {npa}</li>
        <li><b>Error:</b> {error_message}</li>
        <li><b>Time of Failure:</b> {datetime.now(timezone.utc).isoformat()}</li>
    </ul>
    
    {progress}
    
    <h3>Next Steps</h3>
    <ol>
        <li>Check network connectivity</li>
        <li>Verify API is responding</li>
        <li>Review error logs</li>
        <li>Re-run the script: <code>python dno_gen.py</code></li>
    </ol>
    
    <p><small>Data integrity check prevented incomplete data from being written.</small></p>
    """
    
    send_email_notification(subject, html_content)

def condense_unassigned(unassigned_set):
    """
    Condense unassigned combinations to lowest common denominator.
    Returns a list of condensed entries where:
    - If entire NPA is unassigned (all NXX and blocks), return just "NPA"
    - If entire NPA-NXX is unassigned (all blocks 0-9), return just "NPA-NXX"
    - Otherwise return individual "NPA-NXX-X" entries
    """
    # Parse all unassigned into a structured format
    npa_data = {}
    
    for entry in unassigned_set:
        parts = entry.split('-')
        npa = parts[0]
        nxx = parts[1]
        block = parts[2]
        
        if npa not in npa_data:
            npa_data[npa] = {}
        if nxx not in npa_data[npa]:
            npa_data[npa][nxx] = set()
        npa_data[npa][nxx].add(block)
    
    condensed = []
    
    # Check each NPA
    for npa in sorted(npa_data.keys()):
        # Check if entire NPA is unassigned (all possible NXX values)
        all_nxx_unassigned = True
        expected_nxx_count = 0
        
        # Generate all possible NXX for this NPA
        for n in range(2, 10):  # First digit of NXX: 2-9
            for x1 in range(0, 10):  # Second digit: 0-9
                for x2 in range(0, 10):  # Third digit: 0-9
                    nxx = f"{n}{x1}{x2}"
                    expected_nxx_count += 1
                    
                    if nxx not in npa_data[npa]:
                        all_nxx_unassigned = False
                        break
                    # Check if all blocks (0-9) are present for this NXX
                    if len(npa_data[npa][nxx]) != 10:
                        all_nxx_unassigned = False
                        break
                    for block in range(0, 10):
                        if str(block) not in npa_data[npa][nxx]:
                            all_nxx_unassigned = False
                            break
                    if not all_nxx_unassigned:
                        break
                if not all_nxx_unassigned:
                    break
            if not all_nxx_unassigned:
                break
        
        if all_nxx_unassigned and len(npa_data[npa]) == expected_nxx_count:
            # Entire NPA is unassigned
            condensed.append(npa)
        else:
            # Check each NXX within this NPA
            for nxx in sorted(npa_data[npa].keys()):
                # Check if all blocks (0-9) are unassigned for this NPA-NXX
                if len(npa_data[npa][nxx]) == 10:
                    # Verify it's really all blocks 0-9
                    all_blocks_present = all(str(i) in npa_data[npa][nxx] for i in range(10))
                    if all_blocks_present:
                        # Entire NPA-NXX is unassigned
                        condensed.append(f"{npa}-{nxx}")
                    else:
                        # Some blocks missing, list individually
                        for block in sorted(npa_data[npa][nxx]):
                            condensed.append(f"{npa}-{nxx}-{block}")
                else:
                    # Only some blocks are unassigned, list them individually
                    for block in sorted(npa_data[npa][nxx]):
                        condensed.append(f"{npa}-{nxx}-{block}")
    
    return condensed

def main(args=None):
    if DEBUG_MODE:
        print("DEBUG MODE ENABLED (set DNO_DEBUG=false to disable)")
        print("=" * 50)
    
    # Show configuration status
    rate_limit_enabled = os.environ.get('DNO_RATE_LIMIT', '').lower() in ('true', '1', 'yes')
    if USE_BULK_FETCH:
        print(f"Fetch mode: BULK (optimized, {MAX_LIMIT_PER_REQUEST} records/request)")
        print(f"Expected time: ~2-3 minutes for all NPAs")
    else:
        print("Fetch mode: LEGACY (two-step approach)")
        print("Expected time: ~8-10 hours for all NPAs")
    
    if rate_limit_enabled:
        print("Rate limiting: ENABLED (set DNO_RATE_LIMIT=false to disable)")
    elif DEBUG_MODE:
        print("Rate limiting: DISABLED (running at maximum speed)")
    
    print("Generating all possible NPA codes...")
    all_npas = generate_all_possible_npa()
    print(f"Total NPAs to query: {len(all_npas)}")
    
    print("\nFetching assigned NPA-NXX-X combinations from LERG_6...")
    all_assigned = set()
    all_npa_nxx_blocks = {}
    
    start_time = time.time()
    
    for i, npa in enumerate(all_npas, 1):
        npa_start_time = time.time()
        
        if DEBUG_MODE:
            print(f"\n{'='*60}")
            print(f"Processing NPA {npa} ({i}/{len(all_npas)})...")
            print(f"Elapsed time: {(time.time() - start_time):.1f}s")
        else:
            # More informative progress for non-debug mode
            elapsed = time.time() - start_time
            if i > 1:
                avg_time = elapsed / (i - 1)
                eta = avg_time * (len(all_npas) - i)
                print(f"Processing NPA {npa} ({i}/{len(all_npas)}) - Elapsed: {elapsed:.0f}s, ETA: {eta:.0f}s", end='\r')
            else:
                print(f"Processing NPA {npa} ({i}/{len(all_npas)})...", end='\r')
        
        try:
            # Use bulk fetch by default, or legacy if disabled
            if USE_BULK_FETCH:
                assigned_for_npa, npa_nxx_blocks = fetch_assigned_for_npa_bulk(npa)
            else:
                assigned_for_npa, npa_nxx_blocks = fetch_assigned_for_npa_legacy(npa)
            
            # Track progress
            size_before = len(all_assigned)
            
            all_assigned.update(assigned_for_npa)
            size_after = len(all_assigned)
            
            npa_elapsed = time.time() - npa_start_time
            
            if DEBUG_MODE:
                new_unique = size_after - size_before
                duplicates = len(assigned_for_npa) - new_unique
                print(f"  DEBUG: NPA {npa} completed in {npa_elapsed:.1f}s")
                print(f"  DEBUG: Added {new_unique} unique entries ({duplicates} duplicates across NPAs)")
                print(f"  DEBUG: Total assigned so far: {len(all_assigned)}")
            
            all_npa_nxx_blocks.update(npa_nxx_blocks)
            
        except Exception as e:
            print(f"\nERROR: Failed to process NPA {npa}: {e}", file=sys.stderr)
            print(f"Data integrity cannot be guaranteed with missing NPAs.", file=sys.stderr)
            print(f"Please fix the issue and re-run the script.", file=sys.stderr)
            print(f"\nTo resume from NPA {npa}, you can modify the script to start from this NPA.", file=sys.stderr)
            
            # Send failure email
            failure_stats = {
                'npas_processed': i - 1,
                'total_npas': len(all_npas),
                'runtime': time.time() - start_time,
                'records_collected': len(all_assigned)
            }
            send_failure_email(npa, str(e), failure_stats)
            
            sys.exit(1)
    
    print(f"\n\nTotal assigned NPA-NXX-X combinations found: {len(all_assigned)}")
    
    # Count NPA-NXX combinations with only A blocks
    npa_nxx_with_a_only = sum(1 for block_info in all_npa_nxx_blocks.values() 
                               if block_info['has_a'] and len(block_info['numeric']) == 0)
    print(f"NPA-NXX combinations with A blocks only (all blocks assigned): {npa_nxx_with_a_only}")
    
    print("\nGenerating all possible NPA-NXX-block combinations (numeric blocks only)...")
    all_possible = generate_all_possible_npa_nxx_block()
    print(f"Total possible NPA-NXX-block combinations: {len(all_possible)}")
    
    # Calculate unassigned: all possible minus explicitly assigned
    print("\nCalculating unassigned combinations...")
    
    unassigned = all_possible - all_assigned
    print(f"Total unassigned NPA-NXX-X combinations: {len(unassigned)}")
    
    print("\nCondensing unassigned combinations to lowest common denominator...")
    condensed_unassigned = condense_unassigned(unassigned)
    print(f"Condensed from {len(unassigned)} to {len(condensed_unassigned)} entries")
    
    # Write results to CSV files
    print("\nWriting results to CSV files...")
    
    # Write assigned combinations
    with open('assigned_npa_nxx_x.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['NPA-NXX-X', 'Status'])
        for combo in sorted(all_assigned):
            writer.writerow([combo, 'Assigned'])
    print(f"Assigned combinations written to: assigned_npa_nxx_x.csv")
    
    # Write diagnostic info about A blocks
    with open('a_block_analysis.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['NPA-NXX', 'Has_A_Block', 'Numeric_Blocks_Explicitly_Listed', 'Status'])
        for npa_nxx_key, block_info in sorted(all_npa_nxx_blocks.items()):
            if block_info['has_a']:
                assigned_nums = sorted(block_info['numeric'])
                if len(block_info['numeric']) == 0:
                    status = "All blocks (0-9) assigned via A-only rule"
                else:
                    status = "Mixed: A block + explicit numeric blocks"
                writer.writerow([
                    npa_nxx_key,
                    'Yes',
                    ','.join(assigned_nums) if assigned_nums else 'None',
                    status
                ])
    print(f"A block analysis written to: a_block_analysis.csv")
    
    # Fetch ITG traceback data
    print("\nFetching ITG traceback data from BigQuery...")
    itg_data = fetch_itg_traceback_data()
    
    # Write combined unassigned combinations
    with open('unassigned_npa_nxx_x.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        # No header line - upload endpoint expects no headers
        current_datestamp = datetime.now(timezone.utc).isoformat()
        
        # Track invalid records for reporting
        invalid_records = []
        valid_records = 0
        
        # Write LERG unassigned data
        for combo in condensed_unassigned:
            # Remove dashes from the combo
            combo_no_dashes = combo.replace('-', '')
            # Validate length (3=NPA, 6=NPA-NXX, 7=NPA-NXX-X, 10=full phone)
            if len(combo_no_dashes) in [3, 6, 7, 10]:
                writer.writerow([combo_no_dashes, 'LERG Unassigned', current_datestamp])
                valid_records += 1
            else:
                invalid_records.append(('LERG', combo_no_dashes, len(combo_no_dashes)))
        
        # Write ITG traceback data
        for record in itg_data:
            digits = record['digits']
            # Validate length (3=short code, 6=NPA-NXX, 7=NPA-NXX-X, 10=full phone)
            if len(digits) in [3, 6, 7, 10]:
                writer.writerow([digits, record['src'], record['created_at']])
                valid_records += 1
            else:
                invalid_records.append(('ITG', digits, len(digits)))
    
    total_records = len(condensed_unassigned) + len(itg_data)
    print(f"Combined unassigned data written to: unassigned_npa_nxx_x.csv")
    print(f"  - LERG Unassigned: {len(condensed_unassigned)} entries")
    print(f"  - ITG Traceback: {len(itg_data)} entries")
    print(f"  - Valid records written: {valid_records}")
    
    if invalid_records:
        print(f"\nWARNING: {len(invalid_records)} invalid records found (not 3, 6, 7, or 10 digits):")
        for source, digits, length in invalid_records[:10]:  # Show first 10
            print(f"  - {source}: '{digits}' (length: {length})")
        if len(invalid_records) > 10:
            print(f"  ... and {len(invalid_records) - 10} more")
        print("\nThese records were excluded from the CSV file.")
    
    print(f"  - Total: {valid_records} valid entries (out of {total_records} total)")
    
    # Write summary
    with open('lerg_summary.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Category', 'Count', 'Percentage'])
        writer.writerow(['Total Theoretically Possible', len(all_possible), '100.00%'])
        writer.writerow(['Assigned (Including A-only blocks)', len(all_assigned), f'{(len(all_assigned)/len(all_possible)*100):.2f}%'])
        writer.writerow(['Unassigned', len(unassigned), f'{(len(unassigned)/len(all_possible)*100):.2f}%'])
        writer.writerow(['NPA-NXX with A-only (all blocks assigned)', npa_nxx_with_a_only, '-'])
        writer.writerow(['Condensed Unassigned Entries', len(condensed_unassigned), f'{(len(condensed_unassigned)/len(unassigned)*100) if unassigned else 0:.2f}% of original'])
    print(f"Summary written to: lerg_summary.csv")
    
    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    print(f"Total Theoretically Possible:     {len(all_possible):,}")
    print(f"Currently Assigned (LERG):        {len(all_assigned):,} ({(len(all_assigned)/len(all_possible)*100):.2f}%)")
    print(f"  - Via A-only blocks:            {npa_nxx_with_a_only * 10:,} blocks in {npa_nxx_with_a_only:,} NPA-NXX combos")
    print(f"Currently Unassigned:             {len(unassigned):,} ({(len(unassigned)/len(all_possible)*100):.2f}%)")
    print(f"Condensed LERG Unassigned:        {len(condensed_unassigned):,} ({(1 - len(condensed_unassigned)/len(unassigned))*100 if unassigned else 0:.2f}% reduction)")
    if 'itg_data' in locals():
        print(f"ITG Traceback Records:            {len(itg_data):,}")
    
    # Handle automatic upload if requested
    upload_attempted = False
    upload_success = False
    upload_cancelled = False
    
    if args and args.upload:
        unassigned_file = 'unassigned_npa_nxx_x.csv'
        print("\n" + "="*50)
        print("UPLOAD TO API")
        print("="*50)
        
        if not args.yes:
            print(f"\nReady to upload {unassigned_file} to the DNO API.")
            confirmation = input("Proceed with upload? (y/N): ").strip().lower()
            if confirmation not in ['y', 'yes']:
                print("Upload cancelled.")
                upload_cancelled = True
            else:
                upload_attempted = True
        else:
            upload_attempted = True
        
        if upload_attempted:
            upload_success = upload_to_api(unassigned_file)
            
            if upload_success:
                print("\n✓ Data successfully uploaded to DNO API")
            else:
                print("\n✗ Failed to upload data to DNO API")
    
    # Send success email with upload status
    success_stats = {
        'runtime': time.time() - start_time,
        'npas_processed': len(all_npas),
        'api_calls': 'N/A',  # Would need to track this globally
        'fetch_mode': 'BULK' if USE_BULK_FETCH else 'LEGACY',
        'total_possible': len(all_possible),
        'total_assigned': len(all_assigned),
        'assigned_percent': (len(all_assigned)/len(all_possible)*100),
        'total_unassigned': len(unassigned),
        'unassigned_percent': (len(unassigned)/len(all_possible)*100),
        'condensed_unassigned': len(condensed_unassigned),
        'itg_records': len(itg_data) if 'itg_data' in locals() else 0,
        'total_output_records': valid_records if 'valid_records' in locals() else len(condensed_unassigned),
        'upload_requested': args.upload if args else False,
        'upload_attempted': upload_attempted,
        'upload_success': upload_success,
        'upload_cancelled': upload_cancelled
    }
    send_success_email(success_stats)
    
    # Exit with error if upload was requested but failed
    if upload_attempted and not upload_success:
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate DNO (Do Not Originate) data from LERG and ITG sources')
    parser.add_argument('--upload', '-u', action='store_true', 
                        help='Automatically upload the generated data to the API after completion')
    parser.add_argument('--yes', '-y', action='store_true',
                        help='Skip confirmation prompt when uploading (use with --upload)')
    args = parser.parse_args()
    
    main(args)