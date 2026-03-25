import json
import os
import threading
import time
import uuid
from contextlib import closing
from datetime import datetime
from typing import Any

import psycopg2
import requests
from fastapi import FastAPI, HTTPException
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel

ALL_REGIONS = ("us", "eu", "apac")
REGION = os.getenv("REGION", "us")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "incidents")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pass")
REPLICATION_INTERVAL = float(os.getenv("REPLICATION_INTERVAL_SECONDS", "2"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "2"))
PEER_URLS = {
    key: value.rstrip("/")
    for key, value in {
        "us": os.getenv("REGION_US_URL", "http://region-us:3000"),
        "eu": os.getenv("REGION_EU_URL", "http://region-eu:3000"),
        "apac": os.getenv("REGION_APAC_URL", "http://region-apac:3000"),
    }.items()
    if key != REGION
}

app = FastAPI(title=f"region-{REGION}")
blocked_peers: set[str] = set()


class IncidentCreate(BaseModel):
    title: str
    description: str
    severity: str


class IncidentUpdate(BaseModel):
    title: str | None = None
    description: str | None = None
    status: str | None = None
    severity: str | None = None
    assigned_team: str | None = None
    vector_clock: dict[str, int]


class IncidentResolve(BaseModel):
    status: str
    assigned_team: str | None = None


class PeerToggle(BaseModel):
    peer_region: str


class ReplicationIncident(BaseModel):
    id: str
    title: str
    description: str
    status: str
    severity: str
    assigned_team: str | None = None
    vector_clock: dict[str, int]
    version_conflict: bool = False


def model_data(model: BaseModel) -> dict[str, Any]:
    return model.model_dump() if hasattr(model, "model_dump") else model.dict()


def field_names(model: BaseModel) -> set[str]:
    return set(getattr(model, "model_fields_set", getattr(model, "__fields_set__", set())))


def normalized_clock(vector_clock: dict[str, Any] | None) -> dict[str, int]:
    base = {region: 0 for region in ALL_REGIONS}
    if vector_clock:
        for region in ALL_REGIONS:
            base[region] = int(vector_clock.get(region, 0))
    return base


def compare_vectors(vc1: dict[str, int], vc2: dict[str, int]) -> str:
    less = False
    greater = False
    for region in ALL_REGIONS:
        left = vc1.get(region, 0)
        right = vc2.get(region, 0)
        if left < right:
            less = True
        if left > right:
            greater = True
    if less and greater:
        return "CONCURRENT"
    if less:
        return "BEFORE"
    if greater:
        return "AFTER"
    return "EQUAL"


def merge_vectors(vc1: dict[str, int], vc2: dict[str, int]) -> dict[str, int]:
    return {region: max(vc1.get(region, 0), vc2.get(region, 0)) for region in ALL_REGIONS}


def parse_vector_clock(value: Any) -> dict[str, int]:
    if isinstance(value, dict):
        return normalized_clock(value)
    if isinstance(value, str):
        return normalized_clock(json.loads(value))
    return normalized_clock(value or {})


def serialize_incident(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "title": row["title"],
        "description": row["description"],
        "status": row["status"],
        "severity": row["severity"],
        "assigned_team": row["assigned_team"],
        "vector_clock": parse_vector_clock(row["vector_clock"]),
        "version_conflict": row["version_conflict"],
        "updated_at": row["updated_at"].isoformat() if isinstance(row["updated_at"], datetime) else row["updated_at"],
    }


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=5,
        cursor_factory=RealDictCursor,
    )


def init_db():
    with closing(get_db_connection()) as connection:
        with connection, connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS incidents (
                    id UUID PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    description TEXT,
                    status VARCHAR(50) NOT NULL,
                    severity VARCHAR(50) NOT NULL,
                    assigned_team VARCHAR(100),
                    vector_clock JSONB NOT NULL,
                    version_conflict BOOLEAN NOT NULL DEFAULT FALSE,
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )


def fetch_incident(cursor, incident_id: str):
    cursor.execute(
        """
        SELECT id, title, description, status, severity, assigned_team, vector_clock, version_conflict, updated_at
        FROM incidents
        WHERE id = %s
        """,
        (incident_id,),
    )
    return cursor.fetchone()


def update_incident_row(cursor, incident_id: str, payload: dict[str, Any]):
    cursor.execute(
        """
        UPDATE incidents
        SET title = %s,
            description = %s,
            status = %s,
            severity = %s,
            assigned_team = %s,
            vector_clock = %s::jsonb,
            version_conflict = %s,
            updated_at = NOW()
        WHERE id = %s
        """,
        (
            payload["title"],
            payload["description"],
            payload["status"],
            payload["severity"],
            payload["assigned_team"],
            json.dumps(normalized_clock(payload["vector_clock"])),
            payload["version_conflict"],
            incident_id,
        ),
    )


def insert_incident_row(cursor, payload: dict[str, Any]):
    cursor.execute(
        """
        INSERT INTO incidents (
            id, title, description, status, severity, assigned_team, vector_clock, version_conflict
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        """,
        (
            payload["id"],
            payload["title"],
            payload["description"],
            payload["status"],
            payload["severity"],
            payload["assigned_team"],
            json.dumps(normalized_clock(payload["vector_clock"])),
            payload["version_conflict"],
        ),
    )


def replication_loop():
    session = requests.Session()
    while True:
        try:
            with closing(get_db_connection()) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT id, title, description, status, severity, assigned_team, vector_clock, version_conflict, updated_at
                        FROM incidents
                        ORDER BY updated_at DESC
                        """
                    )
                    incidents = [serialize_incident(row) for row in cursor.fetchall()]
            for peer_region, peer_url in PEER_URLS.items():
                if peer_region in blocked_peers:
                    continue
                for incident in incidents:
                    try:
                        session.post(f"{peer_url}/internal/replicate", json=incident, timeout=REQUEST_TIMEOUT)
                    except requests.RequestException:
                        continue
        except psycopg2.Error:
            pass
        time.sleep(REPLICATION_INTERVAL)


@app.on_event("startup")
def startup_event():
    while True:
        try:
            init_db()
            break
        except psycopg2.Error:
            time.sleep(2)
    threading.Thread(target=replication_loop, daemon=True).start()


@app.get("/health")
def health():
    return {"status": "ok", "region": REGION}


@app.get("/incidents")
def list_incidents():
    with closing(get_db_connection()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, title, description, status, severity, assigned_team, vector_clock, version_conflict, updated_at
                FROM incidents
                ORDER BY updated_at DESC
                """
            )
            return [serialize_incident(row) for row in cursor.fetchall()]


@app.get("/incidents/{incident_id}")
def get_incident(incident_id: str):
    with closing(get_db_connection()) as connection:
        with connection.cursor() as cursor:
            row = fetch_incident(cursor, incident_id)
            if not row:
                raise HTTPException(status_code=404, detail="Incident not found")
            return serialize_incident(row)


@app.post("/incidents", status_code=201)
def create_incident(payload: IncidentCreate):
    incident_id = str(uuid.uuid4())
    vector_clock = normalized_clock({})
    vector_clock[REGION] = 1
    record = {
        "id": incident_id,
        "title": payload.title,
        "description": payload.description,
        "status": "OPEN",
        "severity": payload.severity,
        "assigned_team": None,
        "vector_clock": vector_clock,
        "version_conflict": False,
    }
    with closing(get_db_connection()) as connection:
        with connection, connection.cursor() as cursor:
            insert_incident_row(cursor, record)
            row = fetch_incident(cursor, incident_id)
    return serialize_incident(row)


@app.put("/incidents/{incident_id}")
def update_incident(incident_id: str, payload: IncidentUpdate):
    with closing(get_db_connection()) as connection:
        with connection, connection.cursor() as cursor:
            existing = fetch_incident(cursor, incident_id)
            if not existing:
                raise HTTPException(status_code=404, detail="Incident not found")
            local_vector = parse_vector_clock(existing["vector_clock"])
            incoming_vector = normalized_clock(payload.vector_clock)
            comparison = compare_vectors(incoming_vector, local_vector)
            if comparison == "BEFORE":
                raise HTTPException(status_code=409, detail="Stale update")
            if comparison == "CONCURRENT":
                raise HTTPException(status_code=409, detail="Concurrent update conflict")
            next_vector = dict(local_vector)
            next_vector[REGION] += 1
            updated = {
                "title": payload.title if payload.title is not None else existing["title"],
                "description": payload.description if payload.description is not None else existing["description"],
                "status": payload.status if payload.status is not None else existing["status"],
                "severity": payload.severity if payload.severity is not None else existing["severity"],
                "assigned_team": payload.assigned_team if "assigned_team" in field_names(payload) else existing["assigned_team"],
                "vector_clock": next_vector,
                "version_conflict": existing["version_conflict"],
            }
            update_incident_row(cursor, incident_id, updated)
            row = fetch_incident(cursor, incident_id)
    return serialize_incident(row)


@app.post("/internal/replicate")
def replicate_incident(payload: ReplicationIncident):
    incoming = model_data(payload)
    incoming["vector_clock"] = normalized_clock(incoming["vector_clock"])
    with closing(get_db_connection()) as connection:
        with connection, connection.cursor() as cursor:
            existing = fetch_incident(cursor, incoming["id"])
            if not existing:
                insert_incident_row(cursor, incoming)
                return {}
            local_vector = parse_vector_clock(existing["vector_clock"])
            comparison = compare_vectors(incoming["vector_clock"], local_vector)
            if comparison in {"BEFORE", "EQUAL"}:
                return {}
            if comparison == "AFTER":
                merged = merge_vectors(incoming["vector_clock"], local_vector)
                if incoming.get("version_conflict") or existing["version_conflict"]:
                    conflict_payload = {
                        "title": existing["title"],
                        "description": existing["description"],
                        "status": existing["status"],
                        "severity": existing["severity"],
                        "assigned_team": existing["assigned_team"],
                        "vector_clock": merged,
                        "version_conflict": True,
                    }
                    update_incident_row(cursor, incoming["id"], conflict_payload)
                    return {}
                incoming["vector_clock"] = merged
                incoming["version_conflict"] = incoming.get("version_conflict", False)
                update_incident_row(cursor, incoming["id"], incoming)
                return {}
            merged = merge_vectors(incoming["vector_clock"], local_vector)
            conflict_payload = {
                "title": existing["title"],
                "description": existing["description"],
                "status": existing["status"],
                "severity": existing["severity"],
                "assigned_team": existing["assigned_team"],
                "vector_clock": merged,
                "version_conflict": True,
            }
            update_incident_row(cursor, incoming["id"], conflict_payload)
    return {}


@app.post("/incidents/{incident_id}/resolve")
def resolve_incident(incident_id: str, payload: IncidentResolve):
    with closing(get_db_connection()) as connection:
        with connection, connection.cursor() as cursor:
            existing = fetch_incident(cursor, incident_id)
            if not existing:
                raise HTTPException(status_code=404, detail="Incident not found")
            if not existing["version_conflict"]:
                raise HTTPException(status_code=409, detail="Incident does not have a version conflict")
            vector_clock = parse_vector_clock(existing["vector_clock"])
            vector_clock[REGION] += 1
            resolved = {
                "title": existing["title"],
                "description": existing["description"],
                "status": payload.status,
                "severity": existing["severity"],
                "assigned_team": payload.assigned_team,
                "vector_clock": vector_clock,
                "version_conflict": False,
            }
            update_incident_row(cursor, incident_id, resolved)
            row = fetch_incident(cursor, incident_id)
    return serialize_incident(row)


@app.post("/internal/peers/block")
def block_peer(payload: PeerToggle):
    if payload.peer_region not in PEER_URLS:
        raise HTTPException(status_code=400, detail="Unknown peer region")
    blocked_peers.add(payload.peer_region)
    return {"blocked_peers": sorted(blocked_peers)}


@app.post("/internal/peers/unblock")
def unblock_peer(payload: PeerToggle):
    blocked_peers.discard(payload.peer_region)
    return {"blocked_peers": sorted(blocked_peers)}
