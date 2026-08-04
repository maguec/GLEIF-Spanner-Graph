"""
Script to load GLEIF Entity Relationship (RR) data into Google Cloud Spanner.

Inserts relationship records (e.g., IS_FUND-MANAGED_BY, IS_ULTIMATELY_CONSOLIDATED_BY,
IS_DIRECTLY_CONSOLIDATED_BY, IS_SUBFUND_OF, IS_INTERNATIONAL_BRANCH_OF, IS_FEEDER_TO)
into the `EntityRelationships` table interleaved under `Entities`.

Configuration settings are automatically loaded from `.env` file (or system environment variables),
with command-line arguments taking highest priority.

Usage with `uv`:
    uv run load_relationships.py --dry-run
    uv run load_relationships.py
"""

import argparse
import datetime
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

# Attempt to load python-dotenv if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("spanner_rr_loader")


def parse_timestamp(iso_str: Optional[str]) -> Optional[datetime.datetime]:
    """Parse ISO datetime string into UTC datetime object for Spanner TIMESTAMP."""
    if not iso_str:
        return None
    try:
        if iso_str.endswith("Z"):
            iso_str = iso_str[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(iso_str)
    except Exception as e:
        logger.warning(f"Could not parse timestamp '{iso_str}': {e}")
        return None


def extract_text_field(obj: Any) -> Optional[str]:
    """Extract string value from LEI JSON dict wrapper {"$": "value"}."""
    if isinstance(obj, dict):
        return obj.get("$")
    if isinstance(obj, str):
        return obj
    return None


def process_rr_data(rr_json_path: str) -> List[Dict[str, Any]]:
    """Parse relationship JSON file into row dicts for EntityRelationships table."""
    logger.info(f"Loading relationship records from {rr_json_path}...")
    with open(rr_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("relations", [])
    logger.info(f"Loaded {len(records):,} relationship records from {rr_json_path}")

    relationship_rows = []
    seen_keys: Set[Tuple[str, str, str]] = set()

    for r in records:
        rec = r.get("RelationshipRecord", {})
        rel = rec.get("Relationship", {})
        reg = rec.get("Registration", {})

        st_node = rel.get("StartNode", {})
        end_node = rel.get("EndNode", {})

        st_lei = extract_text_field(st_node.get("NodeID"))
        end_lei = extract_text_field(end_node.get("NodeID"))
        rtype = extract_text_field(rel.get("RelationshipType"))

        if not st_lei or not end_lei or not rtype:
            continue

        key = (st_lei, end_lei, rtype)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        row = {
            "StartLEI": st_lei,
            "EndLEI": end_lei,
            "RelationshipType": rtype,
            "RelationshipStatus": extract_text_field(rel.get("RelationshipStatus")),
            "InitialRegistrationDate": parse_timestamp(extract_text_field(reg.get("InitialRegistrationDate"))),
            "LastUpdateDate": parse_timestamp(extract_text_field(reg.get("LastUpdateDate"))),
            "RegistrationStatus": extract_text_field(reg.get("RegistrationStatus")),
            "NextRenewalDate": parse_timestamp(extract_text_field(reg.get("NextRenewalDate"))),
            "ManagingLOU": extract_text_field(reg.get("ManagingLOU")),
            "ValidationSources": extract_text_field(reg.get("ValidationSources")),
        }
        relationship_rows.append(row)

    logger.info(f"Successfully processed {len(relationship_rows):,} valid relationship rows")
    return relationship_rows


def fetch_existing_leis(database) -> Set[str]:
    """Fetch set of existing LEIs from Spanner Entities table to ensure parent existence and FK validity."""
    logger.info("Fetching existing LEIs from Cloud Spanner Entities table...")
    existing_leis = set()
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("SELECT LEI FROM Entities")
        for row in results:
            existing_leis.add(row[0])
    logger.info(f"Retrieved {len(existing_leis):,} existing LEIs from Cloud Spanner")
    return existing_leis


def upload_to_spanner(
    instance_id: str,
    database_id: str,
    project_id: Optional[str],
    relationship_rows: List[Dict[str, Any]],
    batch_size: int = 1000,
    filter_existing: bool = True,
):
    """Write relationship rows into Cloud Spanner in transaction batches."""
    from google.cloud import spanner

    logger.info(f"Connecting to Cloud Spanner instance='{instance_id}', database='{database_id}'...")
    spanner_client = spanner.Client(project=project_id, disable_builtin_metrics=True)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    valid_rows = relationship_rows

    if filter_existing:
        existing_leis = fetch_existing_leis(database)
        if existing_leis:
            valid_rows = [
                r for r in relationship_rows
                if r["StartLEI"] in existing_leis and r["EndLEI"] in existing_leis
            ]
            logger.info(
                f"Filtered relationship rows matching loaded Entities: {len(valid_rows):,} / {len(relationship_rows):,} "
                f"({len(valid_rows)/max(1, len(relationship_rows))*100:.2f}%)"
            )

    if not valid_rows:
        logger.warning("No valid relationship rows to upload after filtering against existing Entities.")
        return

    total_rows = len(valid_rows)
    total_batches = (total_rows + batch_size - 1) // batch_size
    logger.info(f"Uploading {total_rows:,} relationships in {total_batches:,} batches of up to {batch_size} per transaction...")

    for i in range(0, total_rows, batch_size):
        batch = valid_rows[i : i + batch_size]

        def write_batch(transaction):
            transaction.insert_or_update(
                table="EntityRelationships",
                columns=list(batch[0].keys()),
                values=[[r[c] for c in batch[0].keys()] for r in batch],
            )

        database.run_in_transaction(write_batch)

        batch_num = i // batch_size + 1
        processed_cnt = min(i + batch_size, total_rows)
        if batch_num % 10 == 0 or processed_cnt == total_rows:
            logger.info(f"  Committed batch {batch_num:,} / {total_batches:,} ({processed_cnt:,} / {total_rows:,} relationships loaded)")

    logger.info(f"Successfully uploaded {total_rows:,} relationship edges into Cloud Spanner!")


def main():
    default_instance = os.getenv("GOOGLE_SPANNER_INSTANCE") or os.getenv("SPANNER_INSTANCE_ID") or "spanner-instance"
    default_database = os.getenv("GOOGLE_SPANNER_DATABASE") or os.getenv("SPANNER_DATABASE_ID") or "lei-database"
    default_project = os.getenv("GOOGLE_PROJECT") or os.getenv("GCP_PROJECT_ID")
    default_rr_json = os.getenv("RR_JSON_PATH") or "data/20260804-0800-gleif-goldencopy-rr-golden-copy.json"
    default_batch_size = int(os.getenv("BATCH_SIZE") or "1000")
    default_dry_run = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")

    parser = argparse.ArgumentParser(description="Load GLEIF Entity Relationship (RR) data into Cloud Spanner.")
    parser.add_argument("--rr-path", default=default_rr_json, help=f"Path to input RR JSON data file (default: '{default_rr_json}').")
    parser.add_argument("--instance-id", default=default_instance, help=f"Cloud Spanner Instance ID (default: '{default_instance}').")
    parser.add_argument("--database-id", default=default_database, help=f"Cloud Spanner Database ID (default: '{default_database}').")
    parser.add_argument("--project-id", default=default_project, help=f"Google Cloud Project ID (default: '{default_project}').")
    parser.add_argument("--batch-size", type=int, default=default_batch_size, help=f"Number of relationship records per Spanner commit batch (default: {default_batch_size}).")
    parser.add_argument(
        "--no-filter-existing",
        action="store_true",
        help="Skip filtering against existing LEIs in Spanner Entities table.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=default_dry_run,
        help="Parse and process data without inserting into Spanner.",
    )

    args = parser.parse_args()

    logger.info(f"Settings loaded: instance_id='{args.instance_id}', database_id='{args.database_id}', project_id='{args.project_id}', batch_size={args.batch_size}")

    relationship_rows = process_rr_data(args.rr_path)

    if relationship_rows:
        sample = relationship_rows[0]
        logger.info(
            f"Sample Relationship Row: StartLEI={sample['StartLEI']} -> [{sample['RelationshipType']}] -> EndLEI={sample['EndLEI']} "
            f"(Status={sample['RelationshipStatus']}, RegStatus={sample['RegistrationStatus']})"
        )

    if args.dry_run:
        logger.info("Dry-run mode active. Skipping Spanner database write.")
    else:
        upload_to_spanner(
            args.instance_id,
            args.database_id,
            args.project_id,
            relationship_rows,
            batch_size=args.batch_size,
            filter_existing=not args.no_filter_existing,
        )


if __name__ == "__main__":
    main()
