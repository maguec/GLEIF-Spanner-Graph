"""
Script to load GLEIF Entity Relationship (RR) data into Google Cloud Spanner using parallel worker threads.

Inserts relationship records (e.g., IS_FUND-MANAGED_BY, IS_ULTIMATELY_CONSOLIDATED_BY,
IS_DIRECTLY_CONSOLIDATED_BY, IS_SUBFUND_OF, IS_INTERNATIONAL_BRANCH_OF, IS_FEEDER_TO)
into the `EntityRelationships` table interleaved under `Entities`.

Configuration settings are automatically loaded from `.env` file (or system environment variables),
with command-line arguments taking highest priority.

Usage with `uv`:
    uv run --no-sync load_relationships.py --dry-run
    uv run --no-sync load_relationships.py --concurrency 16
"""

import argparse
import collections
import concurrent.futures
import datetime
import json
import logging
import os
import sys
import threading
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
    start_time = time.time()
    with open(rr_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("relations", [])
    logger.info(f"Loaded {len(records):,} relationship records from {rr_json_path} in {time.time() - start_time:.2f}s")

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

    logger.info(f"Successfully processed {len(relationship_rows):,} valid relationship rows in {time.time() - start_time:.2f}s")
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


def _write_single_batch(database, table_name: str, batch: List[Dict[str, Any]], use_batch_mutations: bool = True):
    """Write a single batch to Spanner using batch mutations or transaction."""
    columns = list(batch[0].keys())
    values = [[r[c] for c in columns] for r in batch]

    if use_batch_mutations:
        with database.batch() as b:
            b.insert_or_update(table=table_name, columns=columns, values=values)
    else:
        def write_txn(transaction):
            transaction.insert_or_update(table=table_name, columns=columns, values=values)
        database.run_in_transaction(write_txn)


def upload_to_spanner(
    instance_id: str,
    database_id: str,
    project_id: Optional[str],
    relationship_rows: List[Dict[str, Any]],
    batch_size: int = 1000,
    concurrency: int = 16,
    filter_existing: bool = True,
    use_batch_mutations: bool = True,
):
    """Write relationship rows into Cloud Spanner using concurrent ThreadPoolExecutor workers for maximum throughput."""
    from google.cloud import spanner

    logger.info(f"Connecting to Cloud Spanner instance='{instance_id}', database='{database_id}'...")
    spanner_client = spanner.Client(project=project_id, disable_builtin_metrics=True)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id, pool=spanner.BurstablePool(target_size=max(concurrency * 2, 32)))

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
    batches = [valid_rows[i : i + batch_size] for i in range(0, total_rows, batch_size)]

    method_str = "batch mutations" if use_batch_mutations else "transaction commits"
    logger.info(
        f"Uploading {total_rows:,} relationships using {concurrency} parallel worker threads "
        f"({total_batches:,} batches of {batch_size} rows via {method_str})..."
    )

    start_upload_time = time.time()
    completed_rows = 0
    completed_batches = 0
    lock = threading.Lock()

    def worker_task(batch_info: Tuple[int, List[Dict[str, Any]]]) -> int:
        batch_idx, batch_data = batch_info
        _write_single_batch(database, "EntityRelationships", batch_data, use_batch_mutations=use_batch_mutations)
        
        nonlocal completed_rows, completed_batches
        with lock:
            completed_rows += len(batch_data)
            completed_batches += 1
            elapsed = time.time() - start_upload_time
            rate = completed_rows / max(0.001, elapsed)

            if completed_batches % max(1, total_batches // 10) == 0 or completed_batches == total_batches:
                logger.info(
                    f"  [Progress] Committed batch {completed_batches:,} / {total_batches:,} "
                    f"({completed_rows:,} / {total_rows:,} rows loaded - {rate:,.0f} rows/sec)"
                )
        return len(batch_data)

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker_task, (idx, batch)) for idx, batch in enumerate(batches)]
        for future in concurrent.futures.as_completed(futures):
            future.result()

    total_elapsed = time.time() - start_upload_time
    avg_rate = total_rows / max(0.001, total_elapsed)
    logger.info(f"Successfully uploaded {total_rows:,} relationship edges into Cloud Spanner in {total_elapsed:.2f}s ({avg_rate:,.0f} rows/sec)!")


def main():
    default_instance = os.getenv("GOOGLE_SPANNER_INSTANCE") or os.getenv("SPANNER_INSTANCE_ID") or "spanner-instance"
    default_database = os.getenv("GOOGLE_SPANNER_DATABASE") or os.getenv("SPANNER_DATABASE_ID") or "lei-database"
    default_project = os.getenv("GOOGLE_PROJECT") or os.getenv("GCP_PROJECT_ID")
    default_rr_json = os.getenv("RR_JSON_PATH") or "data/20260804-0800-gleif-goldencopy-rr-golden-copy.json"
    default_batch_size = int(os.getenv("BATCH_SIZE") or "1000")
    default_concurrency = int(os.getenv("LOAD_CONCURRENCY") or os.getenv("CONCURRENCY") or "16")
    default_dry_run = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")

    parser = argparse.ArgumentParser(description="Load GLEIF Entity Relationship (RR) data into Cloud Spanner with parallel worker threads.")
    parser.add_argument("--rr-path", default=default_rr_json, help=f"Path to input RR JSON data file (default: '{default_rr_json}').")
    parser.add_argument("--instance-id", default=default_instance, help=f"Cloud Spanner Instance ID (default: '{default_instance}').")
    parser.add_argument("--database-id", default=default_database, help=f"Cloud Spanner Database ID (default: '{default_database}').")
    parser.add_argument("--project-id", default=default_project, help=f"Google Cloud Project ID (default: '{default_project}').")
    parser.add_argument("--batch-size", type=int, default=default_batch_size, help=f"Number of relationship records per Spanner commit batch (default: {default_batch_size}).")
    parser.add_argument("--concurrency", type=int, default=default_concurrency, help=f"Number of parallel worker threads for Spanner uploads (default: {default_concurrency}).")
    parser.add_argument(
        "--no-filter-existing",
        action="store_true",
        help="Skip filtering against existing LEIs in Spanner Entities table.",
    )
    parser.add_argument(
        "--use-transactions",
        action="store_true",
        help="Use read-write transactions instead of direct batch mutations.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=default_dry_run,
        help="Parse and process data without inserting into Spanner.",
    )

    args = parser.parse_args()

    logger.info(
        f"Settings loaded: instance_id='{args.instance_id}', database_id='{args.database_id}', "
        f"project_id='{args.project_id}', batch_size={args.batch_size}, concurrency={args.concurrency}"
    )

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
            concurrency=args.concurrency,
            filter_existing=not args.no_filter_existing,
            use_batch_mutations=not args.use_transactions,
        )


if __name__ == "__main__":
    main()
