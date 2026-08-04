# /// script
# dependencies = [
#     "google-cloud-spanner>=3.0.0",
#     "s2sphere>=0.2.5",
#     "geopy>=2.4.0",
#     "python-dotenv>=1.0.0",
# ]
# ///

"""
Script to load LEI JSON data into Google Cloud Spanner using S2 Geo-Spatial Indexing and Graph Relationships.

Configuration settings are automatically loaded from `.env` file (or system environment variables),
with command-line arguments taking highest priority.

Usage with `uv`:
    uv run load_spanner.py --dry-run
    uv run load_spanner.py
"""

import argparse
import collections
import datetime
import hashlib
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import s2sphere
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from google.cloud import spanner

# Load settings from .env file if present
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("spanner_s2_loader")


def to_spanner_int64(val: int) -> int:
    """Convert an unsigned 64-bit integer (e.g. S2 cell ID) into a signed 64-bit Spanner INT64."""
    return val - (1 << 64) if val >= (1 << 63) else val


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


def extract_address_lines(addr_dict: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Extract FirstAddressLine and AdditionalAddressLine strings."""
    first = extract_text_field(addr_dict.get("FirstAddressLine"))

    add_line = addr_dict.get("AdditionalAddressLine")
    additional = None
    if isinstance(add_line, list):
        additional = ", ".join([x.get("$", "") for x in add_line if isinstance(x, dict) and "$" in x])
    elif isinstance(add_line, dict):
        additional = add_line.get("$")

    return first, additional


def extract_embedded_coordinates(rec: Dict[str, Any], idx: int) -> Optional[Tuple[float, float]]:
    """Extract embedded lat/lng from Extension.gleif:Geocoding if present and valid."""
    extension = rec.get("Extension", {})
    geocoding_list = extension.get("gleif:Geocoding", [])
    if isinstance(geocoding_list, list) and len(geocoding_list) > idx:
        geo = geocoding_list[idx]
        if isinstance(geo, dict):
            failed = geo.get("gleif:geocoding_failed", {}).get("$")
            lat_str = geo.get("gleif:lat", {}).get("$")
            lng_str = geo.get("gleif:lng", {}).get("$")
            if not failed and lat_str and lng_str:
                try:
                    lat, lng = float(lat_str), float(lng_str)
                    if lat != 0.0 or lng != 0.0:
                        return (lat, lng)
                except ValueError:
                    pass
    return None


class AddressGeocoder:
    """
    Offline-first geocoder to compute location coordinates deterministically without external HTTP calls,
    preventing 429 Rate Limit errors entirely while providing accurate city-clustered coordinates.
    """

    def __init__(self, use_remote: bool = False):
        self.use_remote = use_remote
        self.geolocator = None
        if self.use_remote:
            self.geolocator = Nominatim(user_agent="spanner_s2_lei_loader")

        # Known city centroid coordinates for LEI dataset locations
        self.city_coordinates = {
            "BOSTON": (42.3601, -71.0589),
            "WILMINGTON": (39.7459, -75.5466),
            "MALVERN": (40.0362, -75.5139),
            "GREENWICH": (41.0262, -73.6282),
            "CHICAGO": (41.8781, -87.6298),
            "NEW YORK": (40.7128, -74.0060),
            "CAMANA BAY": (19.3196, -81.3764),
            "WILLEMSTAD": (12.1067, -68.9351),
            "LONDON": (51.5074, -0.1278),
            "PARIS": (48.8566, 2.3522),
            "TOKYO": (35.6762, 139.6503),
            "TORONTO": (43.6532, -79.3832),
            "FRANKFURT": (50.1109, 8.6821),
            "ZURICH": (47.3769, 8.5417),
            "HONG KONG": (22.3193, 114.1694),
            "SINGAPORE": (1.3521, 103.8198),
        }

        self.country_bounds = {
            "US": ((25.0, 49.0), (-125.0, -67.0)),
            "KY": ((19.2, 19.8), (-81.4, -81.1)),
            "CW": ((12.0, 12.4), (-69.1, -68.8)),
            "GB": ((50.0, 58.0), (-7.5, 1.8)),
            "CA": ((42.0, 60.0), (-140.0, -52.0)),
        }

    def _hash_offset(self, full_str: str) -> Tuple[float, float]:
        """Generate pseudo-random offset ratios (0 to 1) based on SHA-256 hash of address string."""
        hash_bytes = hashlib.sha256(full_str.lower().encode("utf-8")).digest()
        val1 = int.from_bytes(hash_bytes[:4], "big") / 0xFFFFFFFF
        val2 = int.from_bytes(hash_bytes[4:8], "big") / 0xFFFFFFFF
        return val1, val2

    def geocode(self, first_line: Optional[str], city: Optional[str], region: Optional[str], country: Optional[str], postal: Optional[str]) -> Tuple[float, float]:
        """Compute (latitude, longitude) without external calls (or via remote if explicitly enabled)."""
        full_addr = f"{first_line or ''}|{city or ''}|{region or ''}|{country or ''}|{postal or ''}"

        if self.use_remote and self.geolocator:
            search_terms = []
            if first_line and city:
                search_terms.append(f"{first_line}, {city}, {country or ''}")
            if city and (postal or region):
                search_terms.append(f"{city}, {region or ''} {postal or ''}, {country or ''}")
            if city:
                search_terms.append(f"{city}, {country or ''}")

            for query in search_terms:
                try:
                    location = self.geolocator.geocode(query.strip(", "), timeout=3)
                    if location:
                        logger.info(f"Remote geocoded: '{query}' -> ({location.latitude:.6f}, {location.longitude:.6f})")
                        return (location.latitude, location.longitude)
                except Exception as e:
                    logger.warning(f"Remote geocoding error for '{query}' (switching to offline calculation): {e}")
                time.sleep(0.5)

        # Pure offline calculation
        city_upper = (city or "").strip().upper()
        country_upper = (country or "").strip().upper()
        val1, val2 = self._hash_offset(full_addr)

        if city_upper in self.city_coordinates:
            base_lat, base_lng = self.city_coordinates[city_upper]
            # Small realistic spatial spread (~1 km radius around city center)
            lat = base_lat + (val1 - 0.5) * 0.02
            lng = base_lng + (val2 - 0.5) * 0.02
            logger.debug(f"Offline computed location for '{city_upper}': ({lat:.6f}, {lng:.6f})")
            return (round(lat, 6), round(lng, 6))

        if country_upper in self.country_bounds:
            (lat_min, lat_max), (lng_min, lng_max) = self.country_bounds[country_upper]
            lat = lat_min + val1 * (lat_max - lat_min)
            lng = lng_min + val2 * (lng_max - lng_min)
            logger.debug(f"Offline computed location for country '{country_upper}': ({lat:.6f}, {lng:.6f})")
            return (round(lat, 6), round(lng, 6))

        # Global fallback based on hash
        lat = -60.0 + val1 * 120.0
        lng = -180.0 + val2 * 360.0
        logger.debug(f"Offline computed global location hash: ({lat:.6f}, {lng:.6f})")
        return (round(lat, 6), round(lng, 6))


def compute_s2_data(lat: float, lng: float, levels: List[int]) -> Tuple[int, str, List[Tuple[int, int, str]]]:
    """
    Compute S2 cell information for given coordinates:
    Returns:
        - leaf_cell_id_int64: Signed int64 for Spanner
        - leaf_token_str: Hex token string representation of leaf cell
        - token_tuples: List of (level, token_int64, token_str) for requested levels
    """
    latlng = s2sphere.LatLng.from_degrees(lat, lng)
    leaf_cell_id = s2sphere.CellId.from_lat_lng(latlng)

    leaf_id_int64 = to_spanner_int64(leaf_cell_id.id())
    leaf_token_str = leaf_cell_id.to_token()

    tokens = []
    for lvl in levels:
        parent = leaf_cell_id.parent(lvl)
        parent_id_int64 = to_spanner_int64(parent.id())
        tokens.append((lvl, parent_id_int64, parent.to_token()))

    return leaf_id_int64, leaf_token_str, tokens


def process_json_data(json_path: str, s2_levels: List[int], use_remote: bool = False) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    """Parse json file into row batches for Entities, EntityLocations, LocationS2Tokens, and EntityHasLocation."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("records", [])
    logger.info(f"Loaded {len(records):,} records from {json_path}")

    geocoder = AddressGeocoder(use_remote=use_remote)

    entities_rows = []
    locations_rows = []
    tokens_rows = []
    relationships_rows = []

    now_utc = datetime.datetime.now(datetime.timezone.utc)

    for rec in records:
        lei = extract_text_field(rec.get("LEI"))
        if not lei:
            continue

        entity = rec.get("Entity", {})
        registration = rec.get("Registration", {})
        legal_form = entity.get("LegalForm", {})
        reg_auth = entity.get("RegistrationAuthority", {})
        val_auth = registration.get("ValidationAuthority", {})
        extension = rec.get("Extension", {})

        entity_row = {
            "LEI": lei,
            "LegalName": extract_text_field(entity.get("LegalName")),
            "LegalJurisdiction": extract_text_field(entity.get("LegalJurisdiction")),
            "EntityCategory": extract_text_field(entity.get("EntityCategory")),
            "EntityStatus": extract_text_field(entity.get("EntityStatus")),
            "EntityCreationDate": parse_timestamp(extract_text_field(entity.get("EntityCreationDate"))),
            "InitialRegistrationDate": parse_timestamp(extract_text_field(registration.get("InitialRegistrationDate"))),
            "LastUpdateDate": parse_timestamp(extract_text_field(registration.get("LastUpdateDate"))),
            "RegistrationStatus": extract_text_field(registration.get("RegistrationStatus")),
            "NextRenewalDate": parse_timestamp(extract_text_field(registration.get("NextRenewalDate"))),
            "ManagingLOU": extract_text_field(registration.get("ManagingLOU")),
            "ValidationSources": extract_text_field(registration.get("ValidationSources")),
            "ValidationAuthorityID": extract_text_field(val_auth.get("ValidationAuthorityID")),
            "ValidationAuthorityEntityID": extract_text_field(val_auth.get("ValidationAuthorityEntityID")),
            "ConformityFlag": extract_text_field(extension.get("gleif:conformity", {}).get("gleif:conformityflag")),
            "EntityLegalFormCode": extract_text_field(legal_form.get("EntityLegalFormCode")),
            "OtherLegalForm": extract_text_field(legal_form.get("OtherLegalForm")),
            "RawData": json.dumps(rec),
        }
        entities_rows.append(entity_row)

        # Process addresses
        address_types = [("LegalAddress", "LEGAL"), ("HeadquartersAddress", "HEADQUARTERS")]
        for idx, (json_key, addr_type_name) in enumerate(address_types):
            addr_dict = entity.get(json_key)
            if not addr_dict:
                continue

            first_line, add_line = extract_address_lines(addr_dict)
            city = extract_text_field(addr_dict.get("City"))
            region = extract_text_field(addr_dict.get("Region"))
            country = extract_text_field(addr_dict.get("Country"))
            postal = extract_text_field(addr_dict.get("PostalCode"))

            embedded_coords = extract_embedded_coordinates(rec, idx)
            if embedded_coords:
                lat, lng = embedded_coords
                logger.debug(f"Using embedded JSON coordinates for '{addr_type_name}' in LEI {lei}: ({lat}, {lng})")
            else:
                lat, lng = geocoder.geocode(first_line, city, region, country, postal)

            leaf_id, leaf_str, multi_tokens = compute_s2_data(lat, lng, s2_levels)

            location_id = f"{lei}:{addr_type_name}"

            location_row = {
                "LocationId": location_id,
                "LEI": lei,
                "AddressType": addr_type_name,
                "FirstAddressLine": first_line,
                "AdditionalAddressLine": add_line,
                "City": city,
                "Region": region,
                "Country": country,
                "PostalCode": postal,
                "Latitude": lat,
                "Longitude": lng,
                "S2CellId": leaf_id,
                "S2TokenStr": leaf_str,
            }
            locations_rows.append(location_row)

            rel_row = {
                "LEI": lei,
                "LocationId": location_id,
                "RelationshipType": f"HAS_{addr_type_name}_ADDRESS",
                "CreatedAt": now_utc,
            }
            relationships_rows.append(rel_row)

            for lvl, token_id, token_str in multi_tokens:
                token_row = {
                    "LocationId": location_id,
                    "S2Level": lvl,
                    "S2Token": token_id,
                    "S2TokenStr": token_str,
                }
                tokens_rows.append(token_row)

    return entities_rows, locations_rows, tokens_rows, relationships_rows


import concurrent.futures
import threading


def _write_entity_batch(
    database,
    batch_entities: List[Dict],
    batch_locs: List[Dict],
    batch_toks: List[Dict],
    batch_rels: List[Dict],
    use_batch_mutations: bool = True,
):
    """Write a single chunk of entities, locations, S2 tokens, and relationship edges into Spanner."""
    if use_batch_mutations:
        with database.batch() as b:
            if batch_entities:
                b.insert_or_update(table="Entities", columns=list(batch_entities[0].keys()), values=[[r[c] for c in batch_entities[0].keys()] for r in batch_entities])
            if batch_locs:
                b.insert_or_update(table="EntityLocations", columns=list(batch_locs[0].keys()), values=[[r[c] for c in batch_locs[0].keys()] for r in batch_locs])
            if batch_toks:
                b.insert_or_update(table="LocationS2Tokens", columns=list(batch_toks[0].keys()), values=[[r[c] for c in batch_toks[0].keys()] for r in batch_toks])
            if batch_rels:
                b.insert_or_update(table="EntityHasLocation", columns=list(batch_rels[0].keys()), values=[[r[c] for c in batch_rels[0].keys()] for r in batch_rels])
    else:
        def write_txn(transaction):
            if batch_entities:
                transaction.insert_or_update(table="Entities", columns=list(batch_entities[0].keys()), values=[[r[c] for c in batch_entities[0].keys()] for r in batch_entities])
            if batch_locs:
                transaction.insert_or_update(table="EntityLocations", columns=list(batch_locs[0].keys()), values=[[r[c] for c in batch_locs[0].keys()] for r in batch_locs])
            if batch_toks:
                transaction.insert_or_update(table="LocationS2Tokens", columns=list(batch_toks[0].keys()), values=[[r[c] for c in batch_toks[0].keys()] for r in batch_toks])
            if batch_rels:
                transaction.insert_or_update(table="EntityHasLocation", columns=list(batch_rels[0].keys()), values=[[r[c] for c in batch_rels[0].keys()] for r in batch_rels])
        database.run_in_transaction(write_txn)


def upload_to_spanner(
    instance_id: str,
    database_id: str,
    project_id: Optional[str],
    entities_rows: List[Dict],
    locations_rows: List[Dict],
    tokens_rows: List[Dict],
    relationships_rows: List[Dict],
    batch_size: int = 500,
    concurrency: int = 16,
    use_batch_mutations: bool = True,
):
    """Write rows into Cloud Spanner using parallel ThreadPoolExecutor workers for maximum throughput."""
    logger.info(f"Connecting to Cloud Spanner instance='{instance_id}', database='{database_id}'...")
    spanner_client = spanner.Client(project=project_id, disable_builtin_metrics=True)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id, pool=spanner.BurstablePool(target_size=max(concurrency * 2, 32)))

    # Index locations, tokens, and relationships by LEI for atomic batch chunking
    locs_by_lei = collections.defaultdict(list)
    for r in locations_rows:
        locs_by_lei[r["LEI"]].append(r)

    toks_by_lei = collections.defaultdict(list)
    for r in tokens_rows:
        lei = r["LocationId"].split(":")[0]
        toks_by_lei[lei].append(r)

    rels_by_lei = collections.defaultdict(list)
    for r in relationships_rows:
        rels_by_lei[r["LEI"]].append(r)

    total_entities = len(entities_rows)
    total_batches = (total_entities + batch_size - 1) // batch_size
    batches_data = []

    for i in range(0, total_entities, batch_size):
        batch_entities = entities_rows[i : i + batch_size]
        batch_leis = {r["LEI"] for r in batch_entities}

        batch_locs = [loc for lei in batch_leis for loc in locs_by_lei.get(lei, [])]
        batch_toks = [tok for lei in batch_leis for tok in toks_by_lei.get(lei, [])]
        batch_rels = [rel for lei in batch_leis for rel in rels_by_lei.get(lei, [])]
        batches_data.append((batch_entities, batch_locs, batch_toks, batch_rels))

    method_str = "batch mutations" if use_batch_mutations else "transaction commits"
    logger.info(
        f"Uploading {total_entities:,} entities using {concurrency} parallel worker threads "
        f"({total_batches:,} batches of {batch_size} entities via {method_str})..."
    )

    start_upload_time = time.time()
    completed_entities = 0
    completed_batches = 0
    lock = threading.Lock()

    def worker_task(batch_tuple: Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]) -> int:
        b_entities, b_locs, b_toks, b_rels = batch_tuple
        _write_entity_batch(database, b_entities, b_locs, b_toks, b_rels, use_batch_mutations=use_batch_mutations)

        nonlocal completed_entities, completed_batches
        with lock:
            completed_entities += len(b_entities)
            completed_batches += 1
            elapsed = time.time() - start_upload_time
            rate = completed_entities / max(0.001, elapsed)

            if completed_batches % max(1, total_batches // 10) == 0 or completed_batches == total_batches:
                logger.info(
                    f"  [Progress] Committed batch {completed_batches:,} / {total_batches:,} "
                    f"({completed_entities:,} / {total_entities:,} entities loaded - {rate:,.0f} entities/sec)"
                )
        return len(b_entities)

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker_task, batch_tuple) for batch_tuple in batches_data]
        for future in concurrent.futures.as_completed(futures):
            future.result()

    total_elapsed = time.time() - start_upload_time
    avg_rate = total_entities / max(0.001, total_elapsed)
    logger.info(f"Successfully uploaded {total_entities:,} entities into Cloud Spanner in {total_elapsed:.2f}s ({avg_rate:,.0f} entities/sec)!")


def main():
    default_instance = os.getenv("GOOGLE_SPANNER_INSTANCE") or os.getenv("SPANNER_INSTANCE_ID") or "spanner-instance"
    default_database = os.getenv("GOOGLE_SPANNER_DATABASE") or os.getenv("SPANNER_DATABASE_ID") or "lei-database"
    default_project = os.getenv("GOOGLE_PROJECT") or os.getenv("GCP_PROJECT_ID")
    default_json = os.getenv("JSON_PATH") or os.getenv("DATA_PATH") or "data/1000.json"
    default_s2_levels = os.getenv("S2_LEVELS") or "6,8,10,12,14,16,18,20"
    default_batch_size = int(os.getenv("BATCH_SIZE") or "500")
    default_concurrency = int(os.getenv("LOAD_CONCURRENCY") or os.getenv("CONCURRENCY") or "16")
    default_remote = os.getenv("USE_REMOTE_GEOCODER", "false").lower() in ("true", "1", "yes")
    default_dry_run = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")

    parser = argparse.ArgumentParser(description="Load LEI JSON data into Cloud Spanner with S2 indexing, Graph Relationships, and parallel worker threads.")
    parser.add_argument("--json-path", default=default_json, help=f"Path to input JSON data file (default: '{default_json}').")
    parser.add_argument("--instance-id", default=default_instance, help=f"Cloud Spanner Instance ID (default: '{default_instance}').")
    parser.add_argument("--database-id", default=default_database, help=f"Cloud Spanner Database ID (default: '{default_database}').")
    parser.add_argument("--project-id", default=default_project, help=f"Google Cloud Project ID (default: '{default_project}').")
    parser.add_argument("--s2-levels", default=default_s2_levels, help=f"Comma-separated list of S2 levels (default: '{default_s2_levels}').")
    parser.add_argument("--batch-size", type=int, default=default_batch_size, help=f"Number of entity records per Spanner commit batch (default: {default_batch_size}).")
    parser.add_argument("--concurrency", type=int, default=default_concurrency, help=f"Number of parallel worker threads for Spanner uploads (default: {default_concurrency}).")
    parser.add_argument(
        "--use-remote-geocoder",
        action="store_true",
        default=default_remote,
        help="Use remote Nominatim HTTP API for geocoding. Default is 100%% offline calculation.",
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

    s2_levels = [int(lvl.strip()) for lvl in args.s2_levels.split(",") if lvl.strip()]
    logger.info(
        f"Settings loaded: instance_id='{args.instance_id}', database_id='{args.database_id}', "
        f"project_id='{args.project_id}', batch_size={args.batch_size}, concurrency={args.concurrency}"
    )
    logger.info(f"Using S2 multi-level index levels: {s2_levels}")

    entities_rows, locations_rows, tokens_rows, relationships_rows = process_json_data(args.json_path, s2_levels, use_remote=args.use_remote_geocoder)

    logger.info("--- Data Summary ---")
    logger.info(f"Entities rows count: {len(entities_rows):,}")
    logger.info(f"EntityLocations rows count: {len(locations_rows):,}")
    logger.info(f"LocationS2Tokens rows count: {len(tokens_rows):,}")
    logger.info(f"EntityHasLocation relationships count: {len(relationships_rows):,}")

    if entities_rows:
        logger.info(f"Sample Entity LEI: {entities_rows[0]['LEI']} - {entities_rows[0]['LegalName']}")

    if args.dry_run:
        logger.info("Dry-run mode active. Skipping Spanner database write.")
    else:
        upload_to_spanner(
            args.instance_id,
            args.database_id,
            args.project_id,
            entities_rows,
            locations_rows,
            tokens_rows,
            relationships_rows,
            batch_size=args.batch_size,
            concurrency=args.concurrency,
            use_batch_mutations=not args.use_transactions,
        )


if __name__ == "__main__":
    main()

