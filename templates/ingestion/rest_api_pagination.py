"""
Template for REST API ingestion with pagination support.

This template demonstrates the recommended pattern for:
- Fetching data from paginated REST APIs
- Handling pagination (page-based or cursor-based)
- Uploading data to S3 using presigned URLs
- Error handling and retry logic
"""

import requests
import os
import json
import time
from typing import List, Dict, Any

# Configuration from environment
API_URL = os.environ['API_URL']
S3_UPLOAD_URL = os.environ['S3_UPLOAD_URL']

# Optional pagination config
PAGE_SIZE = int(os.environ.get('PAGE_SIZE', '100'))
MAX_PAGES = int(os.environ.get('MAX_PAGES', '1000'))


def fetch_page(page_number: int) -> List[Dict[str, Any]]:
    """
    Fetch a single page of data from the API.
    
    Args:
        page_number: Page number to fetch (1-indexed)
        
    Returns:
        List of records from the page
    """
    params = {
        'page': page_number,
        'per_page': PAGE_SIZE
    }
    
    # Retry logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(API_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'data' in data:
                return data['data']
            elif isinstance(data, dict) and 'results' in data:
                return data['results']
            else:
                print(f"Warning: Unexpected response format: {type(data)}")
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page_number}, attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise
    
    return []


def fetch_all_data() -> List[Dict[str, Any]]:
    """
    Fetch all pages of data from the API.
    
    Returns:
        Combined list of all records
    """
    all_data = []
    page = 1
    
    while page <= MAX_PAGES:
        print(f"Fetching page {page}...")
        
        page_data = fetch_page(page)
        
        if not page_data:
            print(f"No more data at page {page}. Stopping pagination.")
            break
        
        all_data.extend(page_data)
        print(f"Fetched {len(page_data)} records (total: {len(all_data)})")
        
        page += 1
    
    return all_data


def upload_to_s3(data: List[Dict[str, Any]]) -> None:
    """
    Upload data to S3 using presigned URL.
    
    Args:
        data: List of records to upload
    """
    json_data = json.dumps(data, indent=2)
    
    response = requests.put(
        S3_UPLOAD_URL,
        data=json_data,
        headers={'Content-Type': 'application/json'}
    )
    
    response.raise_for_status()
    print(f"Successfully uploaded {len(data)} records to S3")


def main():
    """Main execution function."""
    try:
        print("Starting API ingestion...")
        
        # Fetch all data
        data = fetch_all_data()
        
        if not data:
            print("No data fetched. Exiting.")
            return
        
        # Upload to S3
        upload_to_s3(data)
        
        # Success summary
        print(json.dumps({
            'status': 'success',
            'records_fetched': len(data),
            'message': 'API ingestion completed successfully'
        }))
        
    except Exception as e:
        print(json.dumps({
            'status': 'error',
            'error': str(e),
            'message': 'API ingestion failed'
        }))
        raise


if __name__ == '__main__':
    main()
