import azure.functions as func
import logging
import os
import json
from datetime import datetime, timedelta
import requests
from azure.core.exceptions import HttpResponseError
from azure.identity import DefaultAzureCredential
from azure.monitor.ingestion import LogsIngestionClient


def main(mytimer: func.TimerRequest) -> None:
    app_logger = logging.getLogger()
    app_logger.setLevel(logging.INFO)

    # Environment variables (set these in Function App configuration)
    octopus_base_url = os.environ['OCTOPUS_BASE_URL']  # e.g., https://your-octopus.octopus.app
    octopus_api_key = os.environ['OCTOPUS_API_KEY']
    space_id = os.environ['OCTOPUS_SPACE_ID']
    dce_endpoint = os.environ['DATA_COLLECTION_ENDPOINT']  # From DCE resource
    dcr_rule_id = os.environ['LOGS_DCR_RULE_ID']  # From DCR resource
    dcr_stream_name = os.environ['LOGS_DCR_STREAM_NAME']  # e.g., Custom-OctopusEvents_CL
    hours_back = int(os.environ.get('EVENTS_HOURS_BACK', '1'))  # Default: last hour

    app_logger.info(f"Starting Octopus events pull for space {space_id}")

    # Fetch events from Octopus API with pagination
    events = fetch_octopus_events(octopus_base_url, octopus_api_key, space_id, hours_back)
    app_logger.info(f"Fetched {len(events)} events")

    if not events:
        app_logger.warning("No events to ingest")
        return

    # Format as logs for Azure Monitor
    logs = []
    for event in events:
        log_entry = {
            "Time": event.get('Occurred', datetime.utcnow().isoformat()),
            "EventId": event.get('Id', ''),
            "Category": event.get('Category', ''),
            "Message": event.get('Message', ''),
            "Severity": event.get('Severity', ''),
            "User": event.get('User', {}).get('DisplayName', '') if event.get('User') else '',
            "ProjectId": event.get('Project', {}).get('Id', '') if event.get('Project') else '',
            "ProjectName": event.get('Project', {}).get('Name', '') if event.get('Project') else '',
            "EnvironmentId": event.get('Environment', {}).get('Id', '') if event.get('Environment') else '',
            "EnvironmentName": event.get('Environment', {}).get('Name', '') if event.get('Environment') else '',
            "TenantId": event.get('Tenant', {}).get('Id', '') if event.get('Tenant') else '',
            "TenantName": event.get('Tenant', {}).get('Name', '') if event.get('Tenant') else ''
            # Add more fields as needed based on Octopus event schema
        }
        logs.append(log_entry)

    # Ingest to Azure Monitor Logs using managed identity
    try:
        credential = DefaultAzureCredential()
        client = LogsIngestionClient(endpoint=dce_endpoint, credential=credential, logging_enable=True)
        client.upload(rule_id=dcr_rule_id, stream_name=dcr_stream_name, logs=logs)
        app_logger.info(f"Successfully ingested {len(logs)} events to Sentinel")
    except HttpResponseError as e:
        app_logger.error(f"Ingestion failed: {e}")
    except Exception as e:
        app_logger.error(f"Unexpected error during ingestion: {e}")


def fetch_octopus_events(base_url: str, api_key: str, space_id: str, hours_back: int) -> list:
    """
    Fetches paginated events from Octopus Deploy API for the last N hours.
    """
    api_url = f"{base_url}/api/{space_id}/events"
    headers = {'X-Octopus-ApiKey': api_key}
    
    # Calculate from date (Octopus accepts MM/DD/YYYY or ISO)
    from_date = (datetime.utcnow() - timedelta(hours=hours_back)).strftime('%m/%d/%Y')
    params = {'from': from_date, 'take': 1000}  # Max per page is typically 1000
    
    all_events = []
    skip = 0
    
    while True:
        params['skip'] = skip
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        items = data.get('Items', [])
        all_events.extend(items)
        
        # Check for more pages
        if len(items) < params['take']:
            break
        
        skip += params['take']
    
    return all_events
