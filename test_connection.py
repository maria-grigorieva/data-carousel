from opensearchpy import OpenSearch, RequestsHttpConnection, Search, Q, A
from requests_gssapi import HTTPSPNEGOAuth, OPTIONAL
import csv
import os
from datetime import datetime, timedelta
from configparser import ConfigParser

config = ConfigParser()
config.read("config.ini")

certpath = config["es_connection"]["certpath"]
eshost = config["es_connection"]["eshost"]

def create_es_connection():
    es_conn = OpenSearch(eshost,
                        use_ssl=True,
                        verify_certs=True,
                        ca_certs=certpath,
                        connection_class=RequestsHttpConnection,
                        http_auth=HTTPSPNEGOAuth(mutual_authentication=OPTIONAL),
                        )
    print(es_conn)



# Function to flatten nested dictionaries
def flatten(nested_dict, parent_key='', sep='_'):
    items = []
    for k, v in nested_dict.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):  # Recursively flatten nested dictionaries
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def query_es_and_export(index_name, output_file, batch_size=10000):
    es = create_es_connection()

    query = {
        "query": {
            "bool": {
                "must": [
                    { "match_phrase": { "message": "Submit new rule for" } },
                    {"exists": {"field": "dataset"}},
                    {"range": {"asctime": {"gt": "2022-01-01 00:00:00"}}}
                ]
            }
        }
    }

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        # Perform initial search to get the fields dynamically
        scroll = es.search(index=index_name, body=query, scroll='2m', size=batch_size)
        scroll_id = scroll['_scroll_id']
        records = scroll['hits']['hits']

        if records:
            # Flatten the first record's _source and get the headers
            flattened_record = flatten(records[0]['_source'])
            headers = list(flattened_record.keys())
            writer.writerow(headers)  # Write headers to CSV

        total_records = 0

        while records:
            for record in records:
                source = record['_source']
                # Flatten each record and write it to the CSV
                flattened_record = flatten(source)
                writer.writerow([flattened_record.get(field, '') for field in headers])

            total_records += len(records)
            # Perform scrolling to fetch more records
            scroll = es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = scroll['_scroll_id']
            records = scroll['hits']['hits']

    # Output the total records count and file path
    print(f"Total records: {total_records}")
    print(f"Results saved to {output_file}")

    return total_records


index_name = "atlas_prodsyslogs-*"
output_file = "data/results_august2025.csv"
print(output_file)
total_records = query_es_and_export(index_name, output_file)
# print(es.indices.exists(index=index_name))
#
# print(es.indices.get_alias("*").keys())
