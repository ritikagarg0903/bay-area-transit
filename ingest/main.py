import os
import datetime as dt
import requests
from flask import Flask, Response
from google.cloud import bigquery
from google.transit import gtfs_realtime_pb2

app = Flask(__name__)

BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
TABLE_TRIP = os.environ["BQ_TABLE_TRIP"]
TABLE_VEH = os.environ["BQ_TABLE_VEH"]
API_KEY = os.environ["API_KEY"]

TRIP_URL = f"https://api.511.org/Transit/TripUpdates?api_key={API_KEY}&agency=RG"
VEH_URL  = f"https://api.511.org/Transit/VehiclePositions?api_key={API_KEY}&agency=RG"

bq = bigquery.Client(project=BQ_PROJECT)

def _parse_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r.content)
    return feed

def _ts_to_iso(ts: int | None):
    if not ts:
        return None
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat()

def build_trip_rows(feed: gtfs_realtime_pb2.FeedMessage):
    ingested_at = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    feed_ts = _ts_to_iso(feed.header.timestamp)

    rows = []
    for ent in feed.entity:
        if not ent.trip_update:
            continue

        tu = ent.trip_update
        trip = tu.trip

        for stu in tu.stop_time_update:
            stop_id = stu.stop_id if stu.stop_id else None
            stop_seq = int(stu.stop_sequence) if stu.stop_sequence else None

            if stu.arrival and stu.arrival.time:
                rows.append({
                    "ingested_at": ingested_at,
                    "feed_timestamp": feed_ts,
                    "source": "511_trip_updates_rg",
                    "entity_id": ent.id,
                    "trip_id": trip.trip_id,
                    "route_id": trip.route_id,
                    "start_date": trip.start_date,
                    "start_time": trip.start_time,
                    "stop_id": stop_id,
                    "stop_sequence": stop_seq,
                    "event_type": "ARRIVAL",
                    "event_time": _ts_to_iso(stu.arrival.time),
                    "delay_sec": int(stu.arrival.delay) if stu.arrival.delay else None,
                    "schedule_relationship": str(trip.schedule_relationship),
                    "raw_json": None,
                })

            if stu.departure and stu.departure.time:
                rows.append({
                    "ingested_at": ingested_at,
                    "feed_timestamp": feed_ts,
                    "source": "511_trip_updates_rg",
                    "entity_id": ent.id,
                    "trip_id": trip.trip_id,
                    "route_id": trip.route_id,
                    "start_date": trip.start_date,
                    "start_time": trip.start_time,
                    "stop_id": stop_id,
                    "stop_sequence": stop_seq,
                    "event_type": "DEPARTURE",
                    "event_time": _ts_to_iso(stu.departure.time),
                    "delay_sec": int(stu.departure.delay) if stu.departure.delay else None,
                    "schedule_relationship": str(trip.schedule_relationship),
                    "raw_json": None,
                })
    return rows

def build_vehicle_rows(feed: gtfs_realtime_pb2.FeedMessage):
    ingested_at = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    feed_ts = _ts_to_iso(feed.header.timestamp)

    rows = []
    for ent in feed.entity:
        if not ent.vehicle:
            continue

        v = ent.vehicle
        pos = v.position

        rows.append({
            "ingested_at": ingested_at,
            "feed_timestamp": feed_ts,
            "source": "511_vehicle_positions_rg",
            "entity_id": ent.id,
            "vehicle_id": v.vehicle.id if v.vehicle else None,
            "trip_id": v.trip.trip_id if v.trip else None,
            "route_id": v.trip.route_id if v.trip else None,
            "position_lat": float(pos.latitude) if pos else None,
            "position_lon": float(pos.longitude) if pos else None,
            "bearing": float(pos.bearing) if pos else None,
            "speed": float(pos.speed) if pos else None,
            "current_status": str(v.current_status),
            "current_stop_sequence": int(v.current_stop_sequence) if v.current_stop_sequence else None,
            "stop_id": v.stop_id if v.stop_id else None,
            "timestamp": _ts_to_iso(v.timestamp) if v.timestamp else None,
            "raw_json": None,
        })
    return rows

def insert_rows(table: str, rows: list[dict], chunk_size: int = 500):
    """
    Insert rows to BigQuery in chunks to avoid 413 (payload too large).
    """
    if not rows:
        return 0

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
    total = 0

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        errors = bq.insert_rows_json(table_id, chunk)

        if errors:
            raise RuntimeError(f"BigQuery insert errors (chunk {i}-{i+len(chunk)}): {errors}")

        total += len(chunk)

    return total


@app.get("/")
def health():
    return "ok"

@app.post("/run")
def run_ingest():
    trip_feed = _parse_feed(TRIP_URL)
    veh_feed = _parse_feed(VEH_URL)

    trip_rows = build_trip_rows(trip_feed)
    veh_rows = build_vehicle_rows(veh_feed)

    trip_n = insert_rows(TABLE_TRIP, trip_rows)
    veh_n = insert_rows(TABLE_VEH, veh_rows)

    return Response(
        f"Inserted trip_rows={trip_n}, vehicle_rows={veh_n}\n",
        mimetype="text/plain",
        status=200
    )
