"""
Script to load GLEIF Entity Relationship (RR) data into Google Cloud Spanner using true low-memory disk streaming and parallel worker threads.

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
import gc
import json
import logging
import os
import sys
import threading
import time
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

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


def safe_dict_get(obj: Any, *keys: str) -> Any:
    """Safely traverse nested dictionary keys without raising AttributeError on None or non-dict types."""
    curr = obj
    for k in keys:
        if not isinstance(curr, dict):
            return None
        curr = curr.get(k)
        if curr is None:
            return None
    return curr


def extract_text_field(obj: Any) -> Optional[str]:
    """Extract string value from LEI JSON dict wrapper {"$": "value"}."""
    if isinstance(obj, dict):
        return obj.get("$")
    if isinstance(obj, str):
        return obj
    return None


def stream_json_records(filepath: str) -> Generator[Dict[str, Any], None, None]:
    """Stream JSON array records one-by-one from disk to maintain true O(1) RAM footprint."""
    with open(filepath, "r", encoding="utf-8") as f:
        buf = []
        depth = 0
        in_record = False

        for line in f:
            stripped = line.strip()
            if not in_record:
                if stripped.startswith("{") and not stripped.startswith('{"records"') and not stripped.startswith('{"relations"'):
                    in_record = True
                    buf = [line]
                    depth = line.count("{") - line.count("}")
            else:
                buf.append(line)
                depth += line.count("{") - line.count("}")
                if depth == 0:
                    record_str = "".join(buf).rstrip(",\n ")
                    if record_str.endswith(","):
                        record_str = record_str[:-1]
                    try:
                        yield json.loads(record_str)
                    except Exception:
                        pass
                    buf = []
                    in_record = False


def truncate_str(val: Optional[str], max_len: int) -> Optional[str]:
    """Truncate string value defensively if it exceeds column max_len bounds."""
    return val[:max_len] if isinstance(val, str) else val


def fetch_existing_leis(database) -> Set[str]:
    """Fetch all valid LEIs from Entities table in Cloud Spanner to enforce Foreign Key integrity client-side."""
    logger.info("Fetching existing LEIs from Entities table in Cloud Spanner...")
    leis = set()
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("SELECT LEI FROM Entities")
        for row in results:
            leis.add(row[0])
    logger.info(f"Retrieved {len(leis):,} existing LEIs from Spanner.")
    if not leis:
        logger.warning(
            "WARNING: No existing LEIs found in Spanner's 'Entities' table! "
            "Ensure entity data is loaded into Spanner using 'load_spanner.py' before running 'load_relationships.py'."
        )
    return leis


def stream_rr_batches(
    json_path: str,
    batch_size: int = 1000,
    valid_leis: Optional[Set[str]] = None,
) -> Generator[List[Dict[str, Any]], None, None]:
    """Stream relationship JSON file in O(1) memory-efficient batch chunks directly from disk file handle."""
    logger.info(f"Streaming relationship records from {json_path}...")
    current_batch: List[Dict[str, Any]] = []
    seen_keys: Set[Tuple[str, str, str]] = set()
    skipped_fk_count = 0

    for r in stream_json_records(json_path):
        rec = safe_dict_get(r, "RelationshipRecord")
        if not isinstance(rec, dict):
            continue

        st_lei = extract_text_field(safe_dict_get(rec, "Relationship", "StartNode", "NodeID"))
        end_lei = extract_text_field(safe_dict_get(rec, "Relationship", "EndNode", "NodeID"))
        rtype = extract_text_field(safe_dict_get(rec, "Relationship", "RelationshipType"))

        if not st_lei or not end_lei or not rtype:
            continue

        st_lei = st_lei[:20]
        end_lei = end_lei[:20]
        rtype = rtype[:100]

        if valid_leis is not None:
            if st_lei not in valid_leis or end_lei not in valid_leis:
                skipped_fk_count += 1
                continue

        key = (st_lei, end_lei, rtype)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        row = {
            "LEI": st_lei,
            "EndLEI": end_lei,
            "RelationshipType": rtype,
            "RelationshipStatus": truncate_str(extract_text_field(safe_dict_get(rec, "Relationship", "RelationshipStatus")), 50),
            "InitialRegistrationDate": parse_timestamp(extract_text_field(safe_dict_get(rec, "Registration", "InitialRegistrationDate"))),
            "LastUpdateDate": parse_timestamp(extract_text_field(safe_dict_get(rec, "Registration", "LastUpdateDate"))),
            "RegistrationStatus": truncate_str(extract_text_field(safe_dict_get(rec, "Registration", "RegistrationStatus")), 50),
            "NextRenewalDate": parse_timestamp(extract_text_field(safe_dict_get(rec, "Registration", "NextRenewalDate"))),
            "ManagingLOU": truncate_str(extract_text_field(safe_dict_get(rec, "Registration", "ManagingLOU")), 100),
            "ValidationSources": truncate_str(extract_text_field(safe_dict_get(rec, "Registration", "ValidationSources")), 255),
        }
        current_batch.append(row)

        if len(current_batch) >= batch_size:
            yield current_batch
            current_batch = []

    if current_batch:
        yield current_batch

    if skipped_fk_count > 0:
        logger.info(f"Skipped {skipped_fk_count:,} relationship records referencing LEIs not present in the Entities table.")


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


def upload_relationships_streaming(
    instance_id: str,
    database_id: str,
    project_id: Optional[str],
    json_path: str,
    batch_size: int = 1000,
    concurrency: int = 16,
    use_batch_mutations: bool = True,
    skip_fk_check: bool = False,
    dry_run: bool = False,
):
    """Upload relationship records into Cloud Spanner in streaming parallel worker batches with minimal RAM footprint."""
    from google.cloud import spanner

    if dry_run:
        logger.info("Dry-run mode active. Streaming records and validating parsing...")
        total_valid = 0
        start_t = time.time()
        first_sample = None
        for batch in stream_rr_batches(json_path, batch_size=batch_size):
            total_valid += len(batch)
            if first_sample is None and batch:
                first_sample = batch[0]
        elapsed = time.time() - start_t
        if first_sample:
            logger.info(
                f"Sample Relationship Row: LEI={first_sample['LEI']} -> [{first_sample['RelationshipType']}] -> EndLEI={first_sample['EndLEI']} "
                f"(Status={first_sample['RelationshipStatus']}, RegStatus={first_sample['RegistrationStatus']})"
            )
        logger.info(f"Dry-run complete: Processed {total_valid:,} valid relationship rows in {elapsed:.2f}s.")
        return

    logger.info(f"Connecting to Cloud Spanner instance='{instance_id}', database='{database_id}'...")
    spanner_client = spanner.Client(project=project_id, disable_builtin_metrics=True)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id, pool=spanner.BurstyPool(target_size=max(concurrency * 2, 32)))

    valid_leis = None
    if not skip_fk_check:
        valid_leis = fetch_existing_leis(database)

    method_str = "batch mutations" if use_batch_mutations else "transaction commits"
    logger.info(f"Uploading relationships using {concurrency} parallel worker threads via {method_str}...")

    start_upload_time = time.time()
    completed_rows = 0
    completed_batches = 0
    lock = threading.Lock()

    def worker_task(batch_data: List[Dict[str, Any]]) -> int:
        _write_single_batch(database, "EntityRelationships", batch_data, use_batch_mutations=use_batch_mutations)
        nonlocal completed_rows, completed_batches
        with lock:
            completed_rows += len(batch_data)
            completed_batches += 1
            elapsed = time.time() - start_upload_time
            rate = completed_rows / max(0.001, elapsed)

            if completed_batches % 20 == 0:
                logger.info(
                    f"  [Progress] Committed batch {completed_batches:,} "
                    f"({completed_rows:,} rows loaded - {rate:,.0f} rows/sec)"
                )
        return len(batch_data)

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for batch in stream_rr_batches(json_path, batch_size=batch_size, valid_leis=valid_leis):
            futures.append(executor.submit(worker_task, batch))
            # Limit queue size in memory to prevent OOM
            if len(futures) >= concurrency * 4:
                done, pending = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
                for f in done:
                    f.result()
                futures = list(pending)

        for f in concurrent.futures.as_completed(futures):
            f.result()

    total_elapsed = time.time() - start_upload_time
    avg_rate = completed_rows / max(0.001, total_elapsed)
    logger.info(f"Successfully uploaded {completed_rows:,} relationship edges into Cloud Spanner in {total_elapsed:.2f}s ({avg_rate:,.0f} rows/sec)!")


def main():
    default_instance = os.getenv("GOOGLE_SPANNER_INSTANCE") or os.getenv("SPANNER_INSTANCE_ID") or "spanner-instance"
    default_database = os.getenv("GOOGLE_SPANNER_DATABASE") or os.getenv("SPANNER_DATABASE_ID") or "lei-database"
    default_project = os.getenv("GOOGLE_PROJECT") or os.getenv("GCP_PROJECT_ID")
    default_json = os.getenv("JSON_PATH") or os.getenv("RR_JSON_PATH") or "data/20260804-0800-gleif-goldencopy-rr-golden-copy.json"
    default_batch_size = int(os.getenv("BATCH_SIZE") or "1000")
    default_concurrency = int(os.getenv("LOAD_CONCURRENCY") or os.getenv("CONCURRENCY") or "16")
    default_dry_run = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")

    parser = argparse.ArgumentParser(description="Load GLEIF Entity Relationship (RR) data into Cloud Spanner with streaming low-memory worker threads.")
    parser.add_argument("--json-path", default=default_json, help=f"Path to input RR JSON data file (default: '{default_json}').")
    parser.add_argument("--instance-id", default=default_instance, help=f"Cloud Spanner Instance ID (default: '{default_instance}').")
    parser.add_argument("--database-id", default=default_database, help=f"Cloud Spanner Database ID (default: '{default_database}').")
    parser.add_argument("--project-id", default=default_project, help=f"Google Cloud Project ID (default: '{default_project}').")
    parser.add_argument("--batch-size", type=int, default=default_batch_size, help=f"Number of relationship records per Spanner commit batch (default: {default_batch_size}).")
    parser.add_argument("--concurrency", type=int, default=default_concurrency, help=f"Number of parallel worker threads for Spanner uploads (default: {default_concurrency}).")
    parser.add_argument(
        "--use-transactions",
        action="store_true",
        help="Use read-write transactions instead of direct batch mutations.",
    )
    parser.add_argument(
        "--skip-fk-check",
        action="store_true",
        help="Skip querying Spanner for existing LEIs and validating foreign keys prior to insertion.",
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

    upload_relationships_streaming(
        instance_id=args.instance_id,
        database_id=args.database_id,
        project_id=args.project_id,
        json_path=args.json_path,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        use_batch_mutations=not args.use_transactions,
        skip_fk_check=args.skip_fk_check,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
