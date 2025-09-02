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

API_TOKEN = 'hUacbPaHaf7wA8DTwEpcO0Fd4EeIsuPI'
API_BASE_URL = 'https://api-dev.ringer.tel/v1/telique/lerg/lerg_6/npa,nxx,block_id'

# Enable debug logging with environment variable
DEBUG_MODE = os.environ.get('DNO_DEBUG', '').lower() in ('true', '1', 'yes')

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

def fetch_assigned_for_npa(npa):
    """Fetch all assigned NPA-NXX-block_id combinations for a given NPA"""
    assigned = []
    offset = 0
    limit = 1000
    
    # Track NPA-NXX combinations with their block types
    npa_nxx_blocks = {}  # {npa-nxx: {'numeric': set(), 'has_a': bool}}
    
    # Track unique records to handle API duplicates
    seen_records = set()
    duplicate_count = 0
    
    while True:
        url = f"{API_BASE_URL}/npa={npa}?limit={limit}&offset={offset}"
        
        try:
            req = urllib.request.Request(url)
            req.add_header('x-api-token', API_TOKEN)
            
            with urllib.request.urlopen(req, timeout=30) as response:
                response_data = json.loads(response.read().decode('utf-8'))
            
            # Handle the wrapped API response
            data = response_data.get('data', [])
            
            if DEBUG_MODE and offset == 0:
                print(f"  DEBUG: NPA {npa} - First API response has {len(data)} records")
                print(f"  DEBUG: NPA {npa} - Total unique: {response_data.get('total_unique', 'N/A')}")
            
            if not data or len(data) == 0:
                break
                
            for record in data:
                npa_val = record.get('npa', '')
                nxx_val = record.get('nxx', '')
                block_id_val = str(record.get('block_id', ''))
                
                if npa_val and nxx_val and block_id_val:
                    # Ensure NPA and NXX are always strings with proper padding
                    npa_str = str(npa_val).zfill(3)
                    nxx_str = str(nxx_val).zfill(3)
                    
                    # Create unique record key for duplicate detection
                    record_key = f"{npa_str}-{nxx_str}-{block_id_val}"
                    if record_key in seen_records:
                        duplicate_count += 1
                        if DEBUG_MODE:
                            print(f"  DEBUG: Duplicate record found: {record_key}")
                        
                        continue
                    seen_records.add(record_key)
                    
                    npa_nxx_key = f"{npa_str}-{nxx_str}"
                    
                    # Initialize tracking for this NPA-NXX if needed
                    if npa_nxx_key not in npa_nxx_blocks:
                        npa_nxx_blocks[npa_nxx_key] = {'numeric': set(), 'has_a': False}
                    
                    if block_id_val == 'A':
                        # Mark this NPA-NXX as having an 'A' record
                        npa_nxx_blocks[npa_nxx_key]['has_a'] = True
                    else:
                        # This is a numeric block that is explicitly assigned
                        npa_nxx_blocks[npa_nxx_key]['numeric'].add(block_id_val)
                        assigned.append(f"{npa_str}-{nxx_str}-{block_id_val}")
            
            if len(data) < limit:
                break
                
            offset += limit
            
        except urllib.error.URLError as e:
            print(f"Error fetching NPA {npa} at offset {offset}: {e}", file=sys.stderr)
            break
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON for NPA {npa}: {e}", file=sys.stderr)
            break
    
    # Process NPA-NXX combinations with A block handling
    # If an NPA-NXX has ONLY an A block and NO numeric blocks, all blocks 0-9 are considered assigned
    a_only_count = 0
    for npa_nxx_key, block_info in npa_nxx_blocks.items():
        if block_info['has_a'] and len(block_info['numeric']) == 0:
            # This NPA-NXX only has 'A' record, no numeric blocks
            # Add all blocks 0-9 as assigned
            a_only_count += 1
            
            for block in range(0, 10):
                assigned.append(f"{npa_nxx_key}-{block}")
    
    if DEBUG_MODE:
        print(f"  DEBUG: NPA {npa} - Found {duplicate_count} duplicate records")
        print(f"  DEBUG: NPA {npa} - Found {a_only_count} NPA-NXX with A-only blocks")
        print(f"  DEBUG: NPA {npa} - Total assigned: {len(assigned)}")
    
    # Convert to set to ensure uniqueness before returning
    return set(assigned), npa_nxx_blocks

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
    
    print("Generating all possible NPA codes...")
    all_npas = generate_all_possible_npa()
    print(f"Total NPAs to query: {len(all_npas)}")
    
    print("\nFetching assigned NPA-NXX-X combinations from LERG_6...")
    all_assigned = set()
    all_npa_nxx_blocks = {}
    
    for i, npa in enumerate(all_npas, 1):
        if DEBUG_MODE:
            print(f"\nProcessing NPA {npa} ({i}/{len(all_npas)})...")
        else:
            print(f"Processing NPA {npa} ({i}/{len(all_npas)})...", end='\r')
        
        assigned_for_npa, npa_nxx_blocks = fetch_assigned_for_npa(npa)
        
        # Track progress
        size_before = len(all_assigned)
        
        all_assigned.update(assigned_for_npa)
        size_after = len(all_assigned)
        
        if DEBUG_MODE:
            new_unique = size_after - size_before
            duplicates = len(assigned_for_npa) - new_unique
            print(f"  DEBUG: Added {new_unique} unique entries ({duplicates} duplicates across NPAs)")
        
        all_npa_nxx_blocks.update(npa_nxx_blocks)
        
        # Small delay to be respectful to the API
        if i % 10 == 0:
            time.sleep(0.5)
    
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
    with open('/tmp/assigned_npa_nxx_x.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['NPA-NXX-X', 'Status'])
        for combo in sorted(all_assigned):
            writer.writerow([combo, 'Assigned'])
    print(f"Assigned combinations written to: /tmp/assigned_npa_nxx_x.csv")
    
    # Write diagnostic info about A blocks
    with open('/tmp/a_block_analysis.csv', 'w', newline='') as f:
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
    print(f"A block analysis written to: /tmp/a_block_analysis.csv")
    
    # Fetch ITG traceback data
    print("\nFetching ITG traceback data from BigQuery...")
    itg_data = fetch_itg_traceback_data()
    
    # Write combined unassigned combinations
    with open('/tmp/unassigned_npa_nxx_x.csv', 'w', newline='') as f:
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
    print(f"Combined unassigned data written to: /tmp/unassigned_npa_nxx_x.csv")
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
    with open('/tmp/lerg_summary.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Category', 'Count', 'Percentage'])
        writer.writerow(['Total Theoretically Possible', len(all_possible), '100.00%'])
        writer.writerow(['Assigned (Including A-only blocks)', len(all_assigned), f'{(len(all_assigned)/len(all_possible)*100):.2f}%'])
        writer.writerow(['Unassigned', len(unassigned), f'{(len(unassigned)/len(all_possible)*100):.2f}%'])
        writer.writerow(['NPA-NXX with A-only (all blocks assigned)', npa_nxx_with_a_only, '-'])
        writer.writerow(['Condensed Unassigned Entries', len(condensed_unassigned), f'{(len(condensed_unassigned)/len(unassigned)*100) if unassigned else 0:.2f}% of original'])
    print(f"Summary written to: /tmp/lerg_summary.csv")
    
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
    if args and args.upload:
        unassigned_file = '/tmp/unassigned_npa_nxx_x.csv'
        print("\n" + "="*50)
        print("UPLOAD TO API")
        print("="*50)
        
        if not args.yes:
            print(f"\nReady to upload {unassigned_file} to the DNO API.")
            confirmation = input("Proceed with upload? (y/N): ").strip().lower()
            if confirmation not in ['y', 'yes']:
                print("Upload cancelled.")
                return
        
        upload_success = upload_to_api(unassigned_file)
        
        if upload_success:
            print("\n✓ Data successfully uploaded to DNO API")
        else:
            print("\n✗ Failed to upload data to DNO API")
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate DNO (Do Not Originate) data from LERG and ITG sources')
    parser.add_argument('--upload', '-u', action='store_true', 
                        help='Automatically upload the generated data to the API after completion')
    parser.add_argument('--yes', '-y', action='store_true',
                        help='Skip confirmation prompt when uploading (use with --upload)')
    args = parser.parse_args()
    
    main(args)