import configparser
import requests
import boto3
import json
import logging
import time
import os
from functools import wraps
from datetime import datetime

# --- Configuration ---
# Configure logging to provide informative output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- State Management ---

def save_job_state(state, file_path):
    """Saves the current job state (e.g., the next URL) to a file."""
    logging.info(f"Saving job state to {file_path}...")
    try:
        with open(file_path, 'w') as f:
            json.dump(state, f)
    except IOError as e:
        logging.error(f"Could not write to state file {file_path}: {e}")

def load_job_state(file_path):
    """Loads job state from a file if it exists."""
    if os.path.exists(file_path):
        logging.warning(f"Found existing state file {file_path}. Attempting to resume job.")
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logging.error(f"Could not read or parse state file {file_path}. Starting fresh. Error: {e}")
            return None
    return None

def clear_job_state(file_path):
    """Removes the state file upon successful completion."""
    if os.path.exists(file_path):
        logging.info(f"Clearing job state file {file_path}.")
        os.remove(file_path)

# --- Decorators for Resiliency ---

def retry_with_backoff(retries=5, backoff_in_seconds=1):
    """
    A decorator for retrying a function with exponential backoff in case of specific exceptions.
    """
    def rwb(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < retries:
                try:
                    return f(*args, **kwargs)
                except (requests.exceptions.RequestException, boto3.exceptions.Boto3Error) as e:
                    attempts += 1
                    if attempts >= retries:
                        logging.error(f"Function '{f.__name__}' failed after {retries} attempts. Giving up.")
                        raise
                    
                    sleep_duration = backoff_in_seconds * (2 ** (attempts - 1))
                    logging.warning(f"Function '{f.__name__}' failed with {e}. Retrying in {sleep_duration} seconds... (Attempt {attempts}/{retries})")
                    time.sleep(sleep_duration)
        return wrapper
    return rwb

# --- Core Functions ---

@retry_with_backoff()
def get_sf_access_token(config):
    """
    Authenticates with SuccessFactors using OAuth 2.0 to get an access token.
    Now wrapped with a retry decorator.
    """
    token_url = config.get('SuccessFactors', 'token_url')
    client_id = config.get('SuccessFactors', 'client_id')
    client_secret = config.get('SuccessFactors', 'client_secret')
    user_id = config.get('SuccessFactors', 'user_id')
    company_id = config.get('SuccessFactors', 'company_id')
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
        'company_id': company_id,
        'user_id': user_id,
    }
    
    logging.info(f"Requesting access token from {token_url}...")
    response = requests.post(token_url, headers=headers, data=payload, timeout=30)
    response.raise_for_status()
    token_data = response.json()
    logging.info("Successfully obtained access token.")
    return token_data['access_token']

def fetch_sf_data_in_chunks(config, access_token, start_url=None):
    """
    Fetches data from SuccessFactors, handling pagination and retries.
    This function acts as a generator, yielding data and the URL for the next page.
    """
    max_retries = config.getint('ETL_Process', 'max_retries')
    backoff_factor = config.getfloat('ETL_Process', 'backoff_factor')

    if start_url:
        next_url = start_url
        logging.info(f"Resuming data fetch from saved URL: {next_url}")
    else:
        api_base_url = config.get('SuccessFactors', 'api_base_url')
        entity = config.get('SuccessFactors', 'entity_name')
        select_fields = config.get('SuccessFactors', 'select_fields', fallback=None)
        next_url = f"{api_base_url}/odata/v2/{entity}"
        
    headers = {'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'}
    params = {'$format': 'json'}
    if select_fields and not start_url:
        params['$select'] = select_fields

    page_num = 1
    
    while next_url:
        logging.info(f"Fetching page {page_num}...")
        
        attempt = 0
        success = False
        while attempt < max_retries and not success:
            try:
                response = requests.get(next_url, headers=headers, params=params, timeout=60)
                params = None  # Params are only needed for the first request
                
                # Handle API rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 30))
                    logging.warning(f"Rate limit hit. Waiting for {retry_after} seconds.")
                    time.sleep(retry_after)
                    # Continue to next attempt without incrementing, as this is a controlled wait
                    continue

                response.raise_for_status()
                data = response.json()
                success = True
                
            except requests.exceptions.RequestException as e:
                attempt += 1
                if attempt >= max_retries:
                    logging.error(f"Failed to fetch data after {max_retries} attempts.")
                    raise
                sleep_duration = backoff_factor * (2 ** (attempt - 1))
                logging.warning(f"Error fetching data: {e}. Retrying in {sleep_duration} seconds...")
                time.sleep(sleep_duration)

        results = data.get('d', {}).get('results', [])
        next_page_url = data.get('d', {}).get('__next', None)

        if results:
            yield results, next_page_url
        
        next_url = next_page_url
        page_num += 1

@retry_with_backoff()
def upload_chunk_to_s3(s3_client, chunk_data, bucket, key, dead_letter_key):
    """
    Uploads a chunk of data to S3, handling potential "poison pill" records.
    """
    if not chunk_data:
        logging.warning("Skipping upload for empty data chunk.")
        return

    good_records = []
    bad_records = []

    # Handle "poison pill" records that can't be serialized to JSON
    for record in chunk_data:
        try:
            good_records.append(json.dumps(record))
        except TypeError as e:
            logging.error(f"Serialization failed for a record: {e}. Moving to dead-letter queue.")
            bad_records.append({"error": str(e), "record": str(record)})

    # Upload good records
    if good_records:
        jsonl_string = '\n'.join(good_records)
        body_bytes = jsonl_string.encode('utf-8')
        logging.info(f"Uploading {len(good_records)} records to s3://{bucket}/{key}...")
        s3_client.put_object(Bucket=bucket, Key=key, Body=body_bytes, ContentType='application/jsonl+json')
        logging.info(f"Successfully uploaded chunk to s3://{bucket}/{key}")
        
    # Upload bad records to a separate "dead-letter" location for inspection
    if bad_records:
        jsonl_string_bad = '\n'.join(json.dumps(rec) for rec in bad_records)
        body_bytes_bad = jsonl_string_bad.encode('utf-8')
        logging.warning(f"Uploading {len(bad_records)} malformed records to s3://{bucket}/{dead_letter_key}...")
        s3_client.put_object(Bucket=bucket, Key=dead_letter_key, Body=body_bytes_bad, ContentType='application/jsonl+json')

# --- Main Execution ---

def main():
    """ Main function to orchestrate the ETL process. """
    logging.info("--- Starting SuccessFactors to S3 ETL Job ---")
    
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Load config values
    s3_bucket = config.get('AWS', 's3_bucket')
    s3_prefix = config.get('AWS', 's3_prefix', fallback='successfactors-data')
    entity_name = config.get('SuccessFactors', 'entity_name')
    state_file = config.get('ETL_Process', 'state_file_path')

    job_state = load_job_state(state_file)
    start_url = job_state.get('next_url') if job_state else None
    chunk_number = job_state.get('chunk_number', 1) if job_state else 1
    total_records = job_state.get('total_records', 0) if job_state else 0

    try:
        access_token = get_sf_access_token(config)
        s3_client = boto3.client('s3')

        timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
        data_generator = fetch_sf_data_in_chunks(config, access_token, start_url=start_url)

        for chunk, next_url in data_generator:
            # State should point to the NEXT chunk to be processed.
            # Save state *before* processing to ensure we can resume if the upload fails.
            current_state = {
                'next_url': next_url,
                'chunk_number': chunk_number + 1,
                'total_records': total_records + len(chunk)
            }
            if next_url:
                save_job_state(current_state, state_file)
            else:
                # If there's no next_url, this is the last chunk.
                # We will clear the state file upon successful upload.
                pass

            s3_key = f"{s3_prefix}/{entity_name}/{timestamp}_part_{chunk_number:04d}.jsonl"
            dead_letter_key = f"{s3_prefix}/{entity_name}/dead-letter/{timestamp}_part_{chunk_number:04d}.jsonl"
            
            upload_chunk_to_s3(s3_client, chunk, s3_bucket, s3_key, dead_letter_key)

            total_records += len(chunk)
            chunk_number += 1

        # On successful completion of the entire job, clear the state file.
        clear_job_state(state_file)
        logging.info(f"--- ETL Job Finished Successfully ---")
        logging.info(f"Total records processed: {total_records}")

    except Exception as e:
        logging.critical(f"An unrecoverable error occurred during the ETL process: {e}", exc_info=True)
        logging.critical("--- ETL Job Failed ---")
        logging.critical(f"The job state is saved in '{state_file}'. To resume, simply run the script again.")

if __name__ == "__main__":
    main()

