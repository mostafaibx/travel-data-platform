import json
import logging
import os
import datetime
from typing import Dict, Any, Optional, List
from google.cloud import storage
from .config import GCS_BUCKET_NAME, GCS_WIKI_RAW_PREFIX

# Configure logging
logger = logging.getLogger(__name__)

def upload_raw_wiki_data(destination_name: str, raw_data: Dict[str, Any]) -> bool:
    """
    Upload raw Wikipedia data to Google Cloud Storage
    
    Args:
        destination_name: Name of the destination
        raw_data: Raw Wikipedia data to upload
        
    Returns:
        Boolean indicating success or failure
    """
    if not raw_data:
        logger.warning(f"No raw data to upload for {destination_name}")
        return False
        
    try:
        # Create a storage client
        storage_client = storage.Client()
        
        # Get the bucket
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        
        # Create a timestamp for versioning
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate a blob name
        safe_name = destination_name.replace(' ', '_').lower()
        blob_name = f"{GCS_WIKI_RAW_PREFIX}/{safe_name}_{timestamp}.json"
        
        # Create a new blob
        blob = bucket.blob(blob_name)
        
        # Filter content to prevent storing massive HTML content
        upload_data = {
            'url': raw_data.get('url', ''),
            'status_code': raw_data.get('status_code', 0),
            'headers': raw_data.get('headers', {}),
            'encoding': raw_data.get('encoding', ''),
            'content_length': len(raw_data.get('content', '')) if 'content' in raw_data else 0,
            'timestamp': timestamp
        }
        
        # Save a separate blob with the full HTML content for archival purposes
        if 'content' in raw_data and raw_data['content']:
            content_blob_name = f"{GCS_WIKI_RAW_PREFIX}/{safe_name}_{timestamp}_full.html"
            content_blob = bucket.blob(content_blob_name)
            content_blob.upload_from_string(
                raw_data['content'],
                content_type='text/html'
            )
            logger.info(f"Uploaded full HTML content to {content_blob_name}")
            
            # Add reference to the full content blob
            upload_data['full_content_blob'] = content_blob_name
        
        # Upload the metadata as JSON
        blob.upload_from_string(
            json.dumps(upload_data, indent=2),
            content_type='application/json'
        )
        
        logger.info(f"Successfully uploaded raw data for {destination_name} to {blob_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading raw data to GCS for {destination_name}: {e}")
        return False

def upload_processed_wiki_data(destination_data: List[Dict[str, Any]]) -> bool:
    """
    Upload processed Wikipedia data to Google Cloud Storage
    
    Args:
        destination_data: List of processed destination data
        
    Returns:
        Boolean indicating success or failure
    """
    if not destination_data:
        logger.warning("No processed data to upload")
        return False
        
    try:
        # Create a storage client
        storage_client = storage.Client()
        
        # Get the bucket
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        
        # Create a timestamp for versioning
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate a blob name
        blob_name = f"{GCS_WIKI_RAW_PREFIX}/processed/destinations_{timestamp}.json"
        
        # Create a new blob
        blob = bucket.blob(blob_name)
        
        # Upload as JSON
        blob.upload_from_string(
            json.dumps(destination_data, indent=2, default=str),  # default=str handles serialization of dates
            content_type='application/json'
        )
        
        logger.info(f"Successfully uploaded processed data for {len(destination_data)} destinations to {blob_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading processed data to GCS: {e}")
        return False 