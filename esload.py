import argparse
import yaml
import json
import os
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from dateutil.parser import parse as dateparse
from datetime import datetime, timezone, timedelta
import sys
from dotenv import load_dotenv
import os

# Load configurations .env file
load_dotenv()

if not os.environ.get("ELASTICSEARCH_URL") and not os.environ.get("ELASTIC_CLOUD_ID"):
   print("Please provide ELASTICSEARCH_URL or ELASTIC_CLOUD_ID in the environment")
   sys.exit(1)


# Initialize Elasticsearch client
def create_es_client():
    if not os.environ.get("ELASTIC_CLOUD_ID"):
        return Elasticsearch(os.environ.get("ELASTICSEARCH_URL"),
                             api_key=os.environ.get("ELASTICSEARCH_API_KEY"))
    else:
        return Elasticsearch(
            cloud_id=os.environ.get("ELASTIC_CLOUD_ID"),
            api_key=os.environ.get("ELASTICSEARCH_API_KEY")
        )

# Load JSON file
def load_json_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Update timestamp of the documents
def update_timestamp(doc, shift):
    shifted_time = dateparse(doc['@timestamp']) + shift
    doc['@timestamp'] = shifted_time.isoformat()
    # print(doc['@timestamp'])
    return doc

# Get latest timestamp from the data
def latest_time(filename):
    with open(filename) as f:
        latest = None
        for line in f:
            doc = json.loads(line)
            if latest is None or dateparse(doc['@timestamp']) > dateparse(latest):
                latest = doc['@timestamp']
        # print(latest)
        return latest

# Get the required time shift to make documents current
def get_shift(latest):
    print(datetime.fromisoformat(latest))
    current_time = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-5), 'America/New_York'))
    print(current_time)
    shift = current_time - datetime.fromisoformat(latest)
    print(shift)
    return shift

# Load data into Elasticsearch
def make_actions(file_path):
    with open(file_path, 'r') as file:
        if file_path.endswith('.ndjson'):
            for line in file:
                yield json.loads(line)
        elif file_path.endswith('.csv'):
            import csv
            reader = csv.DictReader(file)
            for row in reader:
                yield row
        else:
            raise ValueError("Unsupported file format. Use .ndjson or .csv")

def load_data(es_client, index_name, actions):
    bulk(es_client, actions, index=index_name)

# Main function
def main():
    parser = argparse.ArgumentParser(description="Elasticsearch Data Loader")
    parser.add_argument('file', type=str, help="Path to the ndjson or csv file")
    parser.add_argument('--index', type=str, required=True, help="Target index name")
    parser.add_argument('--ingest-pipeline', type=str, help="Path to the ingest pipeline JSON file")
    parser.add_argument('--index-template', type=str, help="Path to the index template JSON file")
    parser.add_argument('--update-time', action='store_true', help="Update the @timestamp field values to current time")

    args = parser.parse_args()

    # Load configuration
    es_client = create_es_client()

    # Apply index template if provided
    if args.index_template:
        index_template = load_json_file(args.index_template)
        es_client.indices.put_index_template(name=args.index, body=index_template)

    # Apply ingest pipeline if provided
    if args.ingest_pipeline:
        ingest_pipeline = load_json_file(args.ingest_pipeline)
        es_client.ingest.put_pipeline(id=args.index, body=ingest_pipeline)

    # Update timestamp if required
    if args.update_time:
        shift = get_shift(latest_time(args.file))
        actions = (update_timestamp(doc, shift) for doc in make_actions(args.file))
    else:
        actions = make_actions(args.file)

    # Load data into Elasticsearch
    load_data(es_client, args.index, actions)

if __name__ == "__main__":
    main()
