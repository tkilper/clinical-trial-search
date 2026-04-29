"""
DAG: ingest_clinical_trials

Phase 1 ingestion pipeline:
  setup_schema -> fetch_and_stage -> normalize_trials

Trigger manually or on a schedule. Override max_trials via DAG Params in the
UI trigger dialog (default 10000 for development).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime

import psycopg2.extras
from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sql")
COMMIT_BATCH = 500


@dag(
    dag_id="ingest_clinical_trials",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={"max_trials": Param(10_000, type="integer", description="Max trials to fetch")},
    tags=["clinical-trials", "phase-1"],
)
def ingest_clinical_trials():

    @task()
    def setup_schema():
        from etl.db import get_conn

        with get_conn() as conn:
            cur = conn.cursor()
            for fname in ("01_staging.sql", "02_normalized.sql"):
                with open(os.path.join(SQL_DIR, fname)) as f:
                    cur.execute(f.read())
        log.info("Schema ready")

    @task()
    def fetch_and_stage(**context):
        """Fetch trials from ClinicalTrials.gov API and upsert into staging.raw_trials."""
        from etl.db import get_conn
        from etl.fetch import fetch_trials

        max_trials: int = context["params"]["max_trials"]
        batch: list[tuple] = []
        total = 0

        def _flush(conn, records):
            cur = conn.cursor()
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO staging.raw_trials (nct_id, raw_json)
                VALUES %s
                ON CONFLICT (nct_id) DO UPDATE
                    SET raw_json    = EXCLUDED.raw_json,
                        ingested_at = NOW()
                """,
                records,
            )

        with get_conn() as conn:
            for study in fetch_trials(max_trials):
                nct_id = (
                    study.get("protocolSection", {})
                    .get("identificationModule", {})
                    .get("nctId")
                )
                if not nct_id:
                    continue

                batch.append((nct_id, json.dumps(study)))

                if len(batch) >= COMMIT_BATCH:
                    _flush(conn, batch)
                    conn.commit()
                    total += len(batch)
                    log.info("Staged %d trials so far", total)
                    batch = []

            if batch:
                _flush(conn, batch)
                total += len(batch)

        log.info("Staging complete: %d trials loaded", total)
        return total

    @task()
    def normalize_trials():
        """Transform staging.raw_trials into the normalized relational schema."""
        from etl.db import get_conn
        from etl.normalize import (
            extract_conditions,
            extract_eligibility,
            extract_interventions,
            extract_trial,
        )

        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT nct_id, raw_json FROM staging.raw_trials")
            rows = cur.fetchall()

        log.info("Normalizing %d staged trials", len(rows))
        inserted = 0

        with get_conn() as conn:
            cur = conn.cursor()

            for nct_id, raw_json in rows:
                study = raw_json if isinstance(raw_json, dict) else json.loads(raw_json)
                trial = extract_trial(study)

                if not trial["nct_id"]:
                    continue

                cur.execute(
                    """
                    INSERT INTO trials
                        (nct_id, title, brief_summary, overall_status, phase, start_date, sponsor)
                    VALUES
                        (%(nct_id)s, %(title)s, %(brief_summary)s, %(overall_status)s,
                         %(phase)s, %(start_date)s, %(sponsor)s)
                    ON CONFLICT (nct_id) DO UPDATE SET
                        title          = EXCLUDED.title,
                        brief_summary  = EXCLUDED.brief_summary,
                        overall_status = EXCLUDED.overall_status,
                        phase          = EXCLUDED.phase,
                        start_date     = EXCLUDED.start_date,
                        sponsor        = EXCLUDED.sponsor,
                        ingested_at    = NOW()
                    RETURNING id
                    """,
                    trial,
                )
                (trial_id,) = cur.fetchone()

                cur.execute("DELETE FROM conditions    WHERE trial_id = %s", (trial_id,))
                cur.execute("DELETE FROM interventions WHERE trial_id = %s", (trial_id,))
                cur.execute("DELETE FROM eligibility   WHERE trial_id = %s", (trial_id,))

                conds = extract_conditions(study)
                if conds:
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO conditions (trial_id, condition_name) VALUES %s",
                        [(trial_id, c["condition_name"]) for c in conds],
                    )

                ivs = extract_interventions(study)
                if ivs:
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO interventions (trial_id, name, type, description) VALUES %s",
                        [(trial_id, iv["name"], iv["type"], iv["description"]) for iv in ivs],
                    )

                elig = extract_eligibility(study)
                if elig:
                    cur.execute(
                        """
                        INSERT INTO eligibility
                            (trial_id, criteria_text, min_age, max_age, gender, healthy_volunteers)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (trial_id, elig["criteria_text"], elig["min_age"],
                         elig["max_age"], elig["gender"], elig["healthy_volunteers"]),
                    )

                inserted += 1
                if inserted % COMMIT_BATCH == 0:
                    conn.commit()
                    log.info("Normalized %d trials", inserted)

        log.info("Normalization complete: %d trials", inserted)
        return inserted

    schema   = setup_schema()
    staged   = fetch_and_stage()
    normalized = normalize_trials()

    schema >> staged >> normalized


ingest_clinical_trials()
