#!/usr/bin/env python3
"""
Utility script to set up GCP credentials.
This script helps users set up their GCP credentials for use with the travel data platform.
"""

import os
import sys
import json
import argparse
from pathlib import Path

# Add the project root directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pipelines.common.gcp_auth import (
    save_key_to_default_location,
    get_credentials,
    verify_credentials,
    DEFAULT_KEYS_DIR,
    DEFAULT_KEY_FILENAME
)


def setup_credentials():
    """Set up GCP credentials based on command-line arguments."""
    parser = argparse.ArgumentParser(description="Set up GCP credentials for travel data platform")
    
    # Command-line arguments
    parser.add_argument(
        "--key-file", "-k",
        help="Path to a service account key JSON file to use"
    )
    parser.add_argument(
        "--env-var", "-e",
        action="store_true",
        help="Set the GOOGLE_APPLICATION_CREDENTIALS environment variable to the default location"
    )
    parser.add_argument(
        "--verify", "-v",
        action="store_true",
        help="Verify that the credentials work by making a test API call"
    )
    
    args = parser.parse_args()
    
    # Handle key file if provided
    if args.key_file:
        if not os.path.exists(args.key_file):
            print(f"Error: Key file not found at {args.key_file}")
            sys.exit(1)
            
        try:
            # Load the key file
            with open(args.key_file, 'r') as f:
                key_data = f.read()
            
            # Try to parse it as JSON to validate
            try:
                json.loads(key_data)
            except json.JSONDecodeError:
                print("Error: The key file is not valid JSON")
                sys.exit(1)
                
            # Save to default location
            key_path = save_key_to_default_location(key_data)
            print(f"Saved service account key to: {key_path}")
            
        except Exception as e:
            print(f"Error saving key file: {str(e)}")
            sys.exit(1)
    
    # Set environment variable if requested
    if args.env_var:
        default_path = DEFAULT_KEYS_DIR / DEFAULT_KEY_FILENAME
        if not os.path.exists(default_path):
            print(f"Error: No key file found at default location: {default_path}")
            print("Use --key-file to specify a key file first")
            sys.exit(1)
            
        # For this session
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(default_path)
        print(f"Set GOOGLE_APPLICATION_CREDENTIALS for this session to: {default_path}")
        
        # Instructions for permanent setting
        if os.name == "posix":  # Linux/Mac
            print("\nTo set this permanently, add this to your ~/.bashrc or ~/.zshrc:")
            print(f"export GOOGLE_APPLICATION_CREDENTIALS=\"{default_path}\"")
        else:  # Windows
            print("\nTo set this permanently on Windows, run this command:")
            print(f"setx GOOGLE_APPLICATION_CREDENTIALS \"{default_path}\"")
    
    # Verify credentials if requested
    if args.verify:
        try:
            credentials = get_credentials()
            if verify_credentials(credentials):
                print(f"✅ Credentials verified successfully for project: {credentials.project_id}")
            else:
                print("❌ Credentials verification failed")
                sys.exit(1)
        except Exception as e:
            print(f"Error verifying credentials: {str(e)}")
            sys.exit(1)
    
    # If no arguments were provided, show help
    if not (args.key_file or args.env_var or args.verify):
        parser.print_help()


if __name__ == "__main__":
    setup_credentials() 