"""
# Wikipedia Edits ClickHouse Streaming ETL Pipeline
"""

from clickhouse_driver import Client
from datetime import datetime
import IP2Location
import ipaddress
import json
import os
import requests
import signal
import sseclient
import sys
import typing

def get_geolocation_web(ip_address: str) -> dict:
    url = f"https://api.ipgeolocationapi.com/geolocate/{ip_address}"
    try:
        response_raw = requests.request("GET", url)
        response_json = json.loads(response_raw.text)
        result_record = {
            "continent": response_json["continent"],
            "region":    response_json["region"],
            "subregion": response_json["subregion"],
            "country":   response_json["alpha2"].lower(),
            "lat":       float(response_json["geo"]["latitude_dec"]),
            "lon":       float(response_json["geo"]["longitude_dec"])
        }
        return result_record
    except:
        return {}

IP2LOCATION_DB = IP2Location.IP2Location(os.path.join("data", "IP2LOCATION-LITE-DB5.BIN"))

def get_geolocation_local(ip_address: str) -> dict:
    try:
        record_raw = IP2LOCATION_DB.get_all(ip_address)
        result_record = {
            "continent": None, # NA
            "region":    record_raw.region,
            "subregion": None, # NA
            "country":   record_raw.country_short,
            "lat":       float(record_raw.latitude),
            "lon":       float(record_raw.longitude)
        }
        return result_record
    except:
        return {}

def geolocation_enricher_generator(generator: typing.Iterable[dict]) -> typing.Iterable[dict]:
    for change_record in generator:
        try: # check if "user" is an IP address...
            ipaddress.ip_address(change_record["user"])
        except:
            # no IP address, but a named user...
            change_record["anonymous"] = False
            pass
        else:
            # IP address, anonymous user, find geolocation...
            change_record["anonymous"] = True
            change_record.update(get_geolocation_local(change_record["user"]))
        yield change_record

def wikipedia_change_message_generator() -> typing.Iterable[dict]:
    sse_source_url = "https://stream.wikimedia.org/v2/stream/recentchange"
    recent_changes_stream = sseclient.SSEClient(sse_source_url)
    for raw_msg in recent_changes_stream:
        if len(raw_msg.data) == 0: continue # remove empty responses
        json_msg = json.loads(raw_msg.data)
        if json_msg["bot"]: continue # remove bot-generated edits
        if json_msg["type"] not in ["new", "edit"]: continue # only keep page edit and new page events
        result_record = { # create result record and fill it with fields from json_msg...
            "id":            json_msg["id"],
            "type":          json_msg["type"],
            "timestamp":     datetime.utcfromtimestamp(int(json_msg["timestamp"])),
            "user":          json_msg["user"],
            "title":         json_msg["title"],
            "comment":       json_msg["comment"],
            "minor":         json_msg["minor"],
            "length_old":    0 if json_msg["type"] == "new" else json_msg["length"]["old"],
            "length_new":    json_msg["length"]["new"],
            "revision_old":  0 if json_msg["type"] == "new" else json_msg["revision"]["old"],
            "revision_new":  json_msg["revision"]["new"],
            "server_name":   json_msg["server_name"],
            "wiki":          json_msg["wiki"],
            "parsedcomment": json_msg["parsedcomment"],      
            "anonymous":     None,
            "continent":     None,
            "region":        None,
            "subregion":     None,
            "country":       None,
            "lat":           None,
            "lon":           None
        }
        yield result_record

def batch_generator(iterable: typing.Iterable, n: int = 1) -> typing.Iterable:
    current_batch = []
    for item in iterable:
        current_batch.append(item)
        if len(current_batch) == n:
            yield current_batch
            current_batch = []
    yield current_batch # last, incomplete batch

def sigint_handler(signal_received, frame):
    print("wikipedia_edits_stream_loader: shutting down")
    ch.disconnect()
    print("wikipedia_edits_stream_loader: exiting")
    sys.exit(0)


if __name__ == '__main__':
    print("wikipedia_edits_stream_loader: starting up")
    signal.signal(signal.SIGINT, sigint_handler)

    ch = Client("localhost", password="RussiaDoesNotLikeSlow")
    print("wikipedia_edits_stream_loader: connected to ClickHouse")
    ch.execute("CREATE DATABASE IF NOT EXISTS clickhouse_experiments")
    #ch.execute("DROP TABLE IF EXISTS clickhouse_experiments.wikipedia_changes")
    ch.execute("""
CREATE TABLE IF NOT EXISTS clickhouse_experiments.wikipedia_changes
(
    id             UInt64,
    type           LowCardinality(String),
    timestamp      DateTime('UTC'),
    user           String,
    title          String,
    comment        String,
    minor          UInt8,
    length_old     Nullable(UInt64),
    length_new     UInt64,
    revision_old   Nullable(UInt64),
    revision_new   UInt64,
    server_name    LowCardinality(String),
    wiki           LowCardinality(String),
    parsedcomment  String,
    anonymous      UInt8,
    continent      Nullable(String),
    region         Nullable(String),
    subregion      Nullable(String),
    country        Nullable(String),
    lat            Nullable(Float64),
    lon            Nullable(Float64),
    h3index        Nullable(UInt64) MATERIALIZED isNull(lat) ? NULL : geoToH3(lon, lat, 12)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, wiki, id)
SAMPLE BY id
TTL timestamp + INTERVAL 3 YEAR
SETTINGS index_granularity=8192
    """)
    
    for row_batch in batch_generator(geolocation_enricher_generator(wikipedia_change_message_generator()), n=32):
        ch.execute("INSERT INTO clickhouse_experiments.wikipedia_changes VALUES", row_batch)
        print(f"wikipedia_edits_stream_loader: inserted a row_batch of len {len(row_batch)} with id {row_batch[0]['id']}")

