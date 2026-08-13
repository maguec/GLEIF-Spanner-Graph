"""
Sample Spanner Graph Query Script: Find Nearest Companies using S2 Spatial Indexing.

This script demonstrates querying the `LEIGraph` property graph in Google Cloud Spanner
combined with multi-level S2 cell token indexes to find the closest entities to given
geographic coordinates (default: 39.7459, -75.5466 for Wilmington, DE).

Usage with `uv`:
    # Print generated SQL / GQL without executing:
    uv run query_spanner.py --print-sql

    # Execute against Cloud Spanner:
    uv run query_spanner.py

    # Query with custom customer coordinates, S2 level, and limit:
    uv run query_spanner.py --lat 39.7459 --lng -75.5466 --s2-level 8 --limit 10
"""

import argparse
import math
import os
import sys
from typing import Dict, List, Optional, Tuple


# ----------------------------------------------------------------------
# Environment Configuration (.env reader)
# ----------------------------------------------------------------------

def load_env_file(env_path: str = ".env") -> Dict[str, str]:
    """
    Parse a .env file and populate os.environ for any missing variables.
    Uses `python-dotenv` if installed, with a self-contained fallback parser.
    """
    loaded = {}
    # First try python-dotenv if available
    try:
        from dotenv import load_dotenv
        load_dotenv(env_path, override=False)
    except ImportError:
        pass

    # Direct fallback parser to ensure .env is read even without python-dotenv
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip()
                        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                            v = v[1:-1]
                        if k:
                            loaded[k] = v
                            if k not in os.environ:
                                os.environ[k] = v
        except Exception:
            pass

    return loaded


# Load default .env at module import
load_env_file(".env")


# ----------------------------------------------------------------------
# S2 Cell & Geometry Utilities
# ----------------------------------------------------------------------

def to_spanner_int64(val: int) -> int:
    """Convert an unsigned 64-bit integer into signed 64-bit Spanner INT64."""
    return val - (1 << 64) if val >= (1 << 63) else val


def compute_s2_token_and_id(lat: float, lng: float, level: int) -> Tuple[int, str]:
    """
    Computes the S2 token string and signed INT64 cell ID at the specified level.
    Uses `s2sphere` if installed, with a self-contained pure-Python fallback.
    """
    try:
        import s2sphere
        latlng = s2sphere.LatLng.from_degrees(lat, lng)
        leaf_cell = s2sphere.CellId.from_lat_lng(latlng)
        parent_cell = leaf_cell.parent(level)
        return to_spanner_int64(parent_cell.id()), parent_cell.to_token()
    except ImportError:
        # Pure-Python S2 cell implementation fallback
        return _pure_python_s2_cell(lat, lng, level)


def _pure_python_s2_cell(lat_deg: float, lng_deg: float, level: int) -> Tuple[int, str]:
    """Pure-Python implementation of S2 LatLng to parent CellId."""
    lat_rad = math.radians(lat_deg)
    lng_rad = math.radians(lng_deg)
    cos_lat = math.cos(lat_rad)
    x = cos_lat * math.cos(lng_rad)
    y = cos_lat * math.sin(lng_rad)
    z = math.sin(lat_rad)

    # Face determination
    face = 0
    fx, fy, fz = abs(x), abs(y), abs(z)
    if fy > fx:
        face = 1
        fx = fy
    if fz > fx:
        face = 2
    if [x, y, z][face] < 0:
        face += 3

    # Face projection to UV
    if face == 0:
        u, v = y / x, z / x
    elif face == 1:
        u, v = -x / y, z / y
    elif face == 2:
        u, v = -x / z, -y / z
    elif face == 3:
        u, v = z / x, y / x
    elif face == 4:
        u, v = z / y, -x / y
    else:
        u, v = -y / z, -x / z

    # UV to ST (quadratic projection)
    s = 0.5 * math.sqrt(1.0 + 3.0 * u) if u >= 0 else 1.0 - 0.5 * math.sqrt(1.0 - 3.0 * u)
    t = 0.5 * math.sqrt(1.0 + 3.0 * v) if v >= 0 else 1.0 - 0.5 * math.sqrt(1.0 - 3.0 * v)

    # ST to IJ (level 30 discrete coordinates)
    max_size = 1 << 30
    i = max(0, min(max_size - 1, int(math.floor(s * max_size))))
    j = max(0, min(max_size - 1, int(math.floor(t * max_size))))

    # Hilbert curve transformation tables
    lookup_pos = [
        [0, 1, 3, 2],
        [0, 2, 3, 1],
        [3, 2, 0, 1],
        [3, 1, 0, 2],
    ]
    lookup_ij = [
        [0, 1, 3, 2],
        [0, 3, 1, 2],
        [2, 3, 1, 0],
        [2, 1, 3, 0],
    ]

    cell_id = face << 60
    bits = face & 1
    for k in range(29, -1, -1):
        mask = 1 << k
        bits_i = 1 if (i & mask) else 0
        bits_j = 1 if (j & mask) else 0
        quad = (bits_i << 1) | bits_j
        pos = lookup_pos[bits][quad]
        bits = lookup_ij[bits][pos]
        cell_id |= (pos << (2 * k + 1))
    cell_id |= 1  # Leaf cell marker

    # Get parent at target level
    lsb = 1 << (2 * (30 - level))
    parent_id = (cell_id & -lsb) | (lsb >> 1)
    token_str = f"{parent_id:016x}".rstrip("0")
    return to_spanner_int64(parent_id), token_str


# ----------------------------------------------------------------------
# SQL / Graph Query Builders
# ----------------------------------------------------------------------

GRAPH_TABLE_QUERY = """
SELECT
    g.LEI,
    g.LegalName,
    g.EntityStatus,
    g.RelationshipType,
    g.AddressType,
    g.FirstAddressLine,
    g.City,
    g.Region,
    g.Country,
    g.PostalCode,
    g.Latitude,
    g.Longitude,
    -- Geodesic Haversine distance in kilometers (Earth radius: ~6,371.0088 km, deg-to-rad: 0.017453292519943295)
    ROUND(
        6371.0088 * 2 * ASIN(
            SQRT(
                POW(SIN((g.Latitude - @target_lat) * 0.017453292519943295 / 2), 2) +
                COS(@target_lat * 0.017453292519943295) * COS(g.Latitude * 0.017453292519943295) *
                POW(SIN((g.Longitude - @target_lng) * 0.017453292519943295 / 2), 2)
            )
        ), 3
    ) AS distance_km
FROM GRAPH_TABLE(
    LEIGraph
    MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location)
    WHERE l.LocationId IN (
        SELECT LocationId FROM LocationS2Tokens@{FORCE_INDEX=IndexLocationS2TokensByToken}
        WHERE S2Level = @s2_level AND S2Token = @s2_token
    )
    COLUMNS (
        e.LEI,
        e.LegalName,
        e.EntityStatus,
        r.RelationshipType,
        l.LocationId,
        l.AddressType,
        l.FirstAddressLine,
        l.City,
        l.Region,
        l.Country,
        l.PostalCode,
        l.Latitude,
        l.Longitude
    )
) AS g
ORDER BY distance_km ASC
LIMIT @limit;
""".strip()

PURE_GQL_QUERY = """
GRAPH LEIGraph
MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location)
WHERE l.S2TokenStr LIKE @s2_prefix
LET distance_km = ROUND(
    6371.0088 * 2 * ASIN(
        SQRT(
            POW(SIN((l.Latitude - @target_lat) * 0.017453292519943295 / 2), 2) +
            COS(@target_lat * 0.017453292519943295) * COS(l.Latitude * 0.017453292519943295) *
            POW(SIN((l.Longitude - @target_lng) * 0.017453292519943295 / 2), 2)
        )
    ), 3
)
RETURN
    e.LEI,
    e.LegalName,
    e.EntityStatus,
    r.RelationshipType,
    l.AddressType,
    l.FirstAddressLine,
    l.City,
    l.Region,
    l.Country,
    l.Latitude,
    l.Longitude,
    distance_km
ORDER BY distance_km ASC
LIMIT @limit;
""".strip()


# ----------------------------------------------------------------------
# Main Query Execution Logic
# ----------------------------------------------------------------------

def print_table(rows: List[Dict], headers: List[str]):
    """Format and print rows in a clean ASCII table."""
    if not rows:
        print("No matching entities found in this spatial cell.")
        return

    # Calculate column widths
    widths = {h: len(h) for h in headers}
    for row in rows:
        for h in headers:
            val_str = str(row.get(h, "") or "")
            widths[h] = max(widths[h], min(len(val_str), 40))

    # Print header
    header_line = " | ".join(f"{h:<{widths[h]}}" for h in headers)
    sep_line = "-+-".join("-" * widths[h] for h in headers)
    print(header_line)
    print(sep_line)

    # Print data
    for row in rows:
        row_line = " | ".join(
            f"{str(row.get(h, '') or '')[:40]:<{widths[h]}}" for h in headers
        )
        print(row_line)


def run_query(
    instance_id: str,
    database_id: str,
    project_id: Optional[str],
    query_sql: str,
    params: Dict,
    param_types: Dict,
):
    """Execute query against Cloud Spanner and display results."""
    from google.cloud import spanner

    print(f"Connecting to Cloud Spanner instance='{instance_id}', database='{database_id}'...")
    spanner_client = spanner.Client(project=project_id, disable_builtin_metrics=True)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            query_sql,
            params=params,
            param_types=param_types,
        )

        rows = []
        for r in results:
            row_dict = {}
            for col, val in zip(results.fields, r):
                row_dict[col.name] = val
            if "distance_km" in row_dict and row_dict["distance_km"] is not None:
                row_dict["distance_mi"] = round(row_dict["distance_km"] * 0.621371, 3)
            rows.append(row_dict)

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Query Spanner Property Graph (LEIGraph) with S2 spatial indexing for closest companies to customer coordinates."
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env configuration file (default: .env).")
    
    # Pre-parse --env-file to load environment before building other defaults
    temp_args, _ = parser.parse_known_args()
    if temp_args.env_file and os.path.exists(temp_args.env_file):
        load_env_file(temp_args.env_file)

    # Defaults loaded from .env / environment variables
    default_instance = os.getenv("GOOGLE_SPANNER_INSTANCE") or os.getenv("SPANNER_INSTANCE_ID") or "spanner-instance"
    default_database = os.getenv("GOOGLE_SPANNER_DATABASE") or os.getenv("SPANNER_DATABASE_ID") or "lei-database"
    default_project = os.getenv("GOOGLE_PROJECT") or os.getenv("GCP_PROJECT_ID")
    default_lat = float(os.getenv("CUSTOMER_LAT") or os.getenv("QUERY_LAT") or os.getenv("TARGET_LAT") or os.getenv("LATITUDE") or "39.7459")
    default_lng = float(os.getenv("CUSTOMER_LNG") or os.getenv("QUERY_LNG") or os.getenv("TARGET_LNG") or os.getenv("LONGITUDE") or "-75.5466")
    default_s2_level = int(os.getenv("QUERY_S2_LEVEL") or os.getenv("S2_LEVEL") or "8")
    default_limit = int(os.getenv("QUERY_LIMIT") or os.getenv("LIMIT") or "10")
    default_mode = os.getenv("QUERY_MODE") or "graph_table"

    parser.add_argument(
        "--lat",
        type=float,
        default=default_lat,
        help=f"Customer latitude coordinate (default from .env or 39.7459: {default_lat}).",
    )
    parser.add_argument(
        "--lng",
        type=float,
        default=default_lng,
        help=f"Customer longitude coordinate (default from .env or -75.5466: {default_lng}).",
    )
    parser.add_argument(
        "--s2-level",
        type=int,
        default=default_s2_level,
        help=f"S2 level for spatial candidate filtering (default from .env: {default_s2_level} [~50km radius]; use 10 for ~12km, 6 for ~200km).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=default_limit,
        help=f"Number of closest companies to return (default from .env: {default_limit}).",
    )
    parser.add_argument(
        "--mode",
        choices=["graph_table", "gql"],
        default=default_mode,
        help=f"Query mode: 'graph_table' (indexed join with LocationS2Tokens) or 'gql' (pure GQL) (default from .env: {default_mode}).",
    )
    parser.add_argument("--instance-id", default=default_instance, help=f"Cloud Spanner Instance ID (default from .env: '{default_instance}').")
    parser.add_argument("--database-id", default=default_database, help=f"Cloud Spanner Database ID (default from .env: '{default_database}').")
    parser.add_argument("--project-id", default=default_project, help=f"Google Cloud Project ID (default from .env: '{default_project}').")
    parser.add_argument("--print-sql", action="store_true", help="Print the generated SQL/GQL query and exit without connecting.")

    args = parser.parse_args()

    target_lat = args.lat
    target_lng = args.lng

    # Compute S2 cell ID and token for customer coordinates
    s2_token_id, s2_token_str = compute_s2_token_and_id(target_lat, target_lng, args.s2_level)

    env_status = f"Loaded from {args.env_file}" if os.path.exists(args.env_file) else "No .env found (using system env/defaults)"
    print("=" * 80)
    print(" Spanner Graph S2 Nearest Companies Query")
    print("=" * 80)
    print(f"Config Source       : {env_status}")
    print(f"Customer Coordinates: Lat={target_lat:.6f}, Lng={target_lng:.6f}")
    print(f"S2 Level            : {args.s2_level}")
    print(f"S2 Token Str        : '{s2_token_str}'")
    print(f"S2 Token INT64      : {s2_token_id}")
    print(f"Result Limit        : {args.limit}")
    print(f"Query Mode          : {args.mode}")
    print("=" * 80)

    if args.mode == "graph_table":
        query_sql = GRAPH_TABLE_QUERY
        params = {
            "target_lat": target_lat,
            "target_lng": target_lng,
            "s2_level": args.s2_level,
            "s2_token": s2_token_id,
            "limit": args.limit,
        }
    else:
        query_sql = PURE_GQL_QUERY
        params = {
            "target_lat": target_lat,
            "target_lng": target_lng,
            "s2_prefix": f"{s2_token_str}%",
            "limit": args.limit,
        }

    if args.print_sql:
        print("\n--- Generated Spanner Graph Query ---")
        print(query_sql)
        print("\n--- Query Parameters ---")
        for k, v in params.items():
            print(f"  @{k} = {v} ({type(v).__name__})")
        return

    # Check for google-cloud-spanner dependency before attempting connection
    try:
        from google.cloud import spanner
    except ImportError:
        print("\n[ERROR] 'google-cloud-spanner' is not installed in the current environment.")
        print("Please install dependencies or run with uv:")
        print("    uv run query_spanner.py\n")
        print("To preview the generated SQL instead, run:")
        print("    uv run query_spanner.py --print-sql")
        sys.exit(1)

    # Param types mapping for Spanner Client
    if args.mode == "graph_table":
        param_types = {
            "target_lat": spanner.param_types.FLOAT64,
            "target_lng": spanner.param_types.FLOAT64,
            "s2_level": spanner.param_types.INT64,
            "s2_token": spanner.param_types.INT64,
            "limit": spanner.param_types.INT64,
        }
    else:
        param_types = {
            "target_lat": spanner.param_types.FLOAT64,
            "target_lng": spanner.param_types.FLOAT64,
            "s2_prefix": spanner.param_types.STRING,
            "limit": spanner.param_types.INT64,
        }

    try:
        rows = run_query(
            instance_id=args.instance_id,
            database_id=args.database_id,
            project_id=args.project_id,
            query_sql=query_sql,
            params=params,
            param_types=param_types,
        )

        print(f"\nFound {len(rows)} closest companies to coordinates ({target_lat:.4f}, {target_lng:.4f}):\n")
        display_headers = [
            "LEI",
            "LegalName",
            "AddressType",
            "City",
            "Region",
            "Country",
            "distance_km",
            "distance_mi",
        ]
        print_table(rows, display_headers)

    except Exception as e:
        print(f"\n[ERROR] Query execution failed: {e}")
        print("\nTip: You can preview the exact query by passing `--print-sql`:")
        print("    uv run query_spanner.py --print-sql")
        sys.exit(1)


if __name__ == "__main__":
    main()
