"""
Template for data transformation with schema validation.

This template demonstrates the recommended pattern for:
- Downloading data from S3 using presigned URLs
- Transforming data with Pandas
- Schema type validation
- Uploading transformed data to S3
"""

import pandas as pd
import requests
import os
import json
from typing import Dict, Any, List

# Configuration from environment
S3_DOWNLOAD_URL = os.environ['S3_DOWNLOAD_URL']
S3_UPLOAD_URL = os.environ['S3_UPLOAD_URL']


def download_from_s3() -> pd.DataFrame:
    """
    Download data from S3 using presigned URL.
    
    Returns:
        DataFrame with source data
    """
    print("Downloading data from S3...")
    
    response = requests.get(S3_DOWNLOAD_URL, timeout=60)
    response.raise_for_status()
    
    # Parse JSON data
    data = response.json()
    
    # Convert to DataFrame
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict) and 'data' in data:
        df = pd.DataFrame(data['data'])
    else:
        df = pd.DataFrame([data])
    
    print(f"Downloaded {len(df)} records")
    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the data according to business logic.
    
    Args:
        df: Source DataFrame
        
    Returns:
        Transformed DataFrame
    """
    print("Transforming data...")
    
    # Example transformations (customize as needed):
    
    # 1. Handle missing values
    df = df.dropna(subset=['id'])  # Drop rows with missing IDs
    
    # 2. Type conversions
    if 'id' in df.columns:
        df['id'] = df['id'].astype('int64')
    
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'])
    
    # 3. Add derived columns
    if 'created_at' in df.columns:
        df['created_date'] = df['created_at'].dt.date
        df['created_year'] = df['created_at'].dt.year
    
    # 4. Filter data
    # df = df[df['status'] == 'active']
    
    print(f"Transformation complete: {len(df)} records")
    return df


def validate_schema(df: pd.DataFrame, expected_schema: Dict[str, str]) -> None:
    """
    Validate DataFrame schema matches expected types.
    
    Args:
        df: DataFrame to validate
        expected_schema: Dict mapping column names to expected types
        
    Raises:
        ValueError: If schema validation fails
    """
    print("Validating schema...")
    
    actual_dtypes = df.dtypes.astype(str).to_dict()
    mismatches = []
    
    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            mismatches.append(f'Missing column: {col}')
        else:
            actual = str(df[col].dtype)
            
            # Type checking with flexible matching
            if expected_type == 'int' and 'int' not in actual:
                mismatches.append(f'{col}: expected int, got {actual}')
            elif expected_type == 'str' and actual != 'object':
                mismatches.append(f'{col}: expected str, got {actual}')
            elif expected_type == 'float' and 'float' not in actual:
                mismatches.append(f'{col}: expected float, got {actual}')
            elif expected_type.startswith('datetime') and 'datetime' not in actual:
                mismatches.append(f'{col}: expected datetime, got {actual}')
    
    if mismatches:
        error_msg = f'Schema validation failed: {mismatches}'
        print(json.dumps({'error': error_msg, 'mismatches': mismatches}))
        raise ValueError(error_msg)
    
    print("Schema validation passed")


def upload_to_s3(df: pd.DataFrame) -> None:
    """
    Upload transformed data to S3 using presigned URL.
    
    Args:
        df: DataFrame to upload
    """
    print("Uploading to S3...")
    
    # Convert to JSON
    json_data = df.to_json(orient='records', date_format='iso')
    
    response = requests.put(
        S3_UPLOAD_URL,
        data=json_data,
        headers={'Content-Type': 'application/json'}
    )
    
    response.raise_for_status()
    print(f"Successfully uploaded {len(df)} records to S3")


def main():
    """Main execution function."""
    try:
        print("Starting transformation...")
        
        # 1. Download data
        df = download_from_s3()
        
        # 2. Transform data
        df_transformed = transform_data(df)
        
        # 3. Validate schema (customize expected_schema as needed)
        expected_schema = {
            'id': 'int',
            'created_at': 'datetime64[ns]',
            # Add more columns as needed
        }
        validate_schema(df_transformed, expected_schema)
        
        # 4. Upload to S3
        upload_to_s3(df_transformed)
        
        # Success summary
        print(json.dumps({
            'status': 'success',
            'records_processed': len(df_transformed),
            'message': 'Transformation completed successfully'
        }))
        
    except Exception as e:
        print(json.dumps({
            'status': 'error',
            'error': str(e),
            'message': 'Transformation failed'
        }))
        raise


if __name__ == '__main__':
    main()
