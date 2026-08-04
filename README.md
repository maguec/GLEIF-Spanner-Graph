# Cloud Spanner S2 Geo-Spatial & Graph Database Data Loader

This repository provides a complete solution for loading Legal Entity Identifier (LEI) data from [`data/1000.json`](file://data/1000.json) into Google Cloud Spanner using **S2 Geometry multi-level spatial indexing** and **Spanner Property Graph (GQL)**.

The spatial indexing follows the flexible multi-level S2 approach described in:
[*Geo-Spatial Indexing on Google Cloud Spanner with S2*](https://medium.com/google-cloud/geo-spatial-indexing-on-google-cloud-spanner-with-s2-81a013d772c4).

---

## Workspace Files

- [`slides.html`](file://docs/slides.html): Quick background on using S2
- [`DDL.sql`](file://DDL.sql): Single SQL schema file defining all relational tables, S2 token tables, secondary indexes, and the `LEIGraph` Property Graph schema.
- [`load_spanner.py`](file://load_spanner.py): Python loader script managed by `uv`. Parses LEI records, geocodes addresses, generates S2 leaf cell IDs and multi-level tokens, populates node & edge tables, and batches uploads into Spanner.
- [`load_relationships.py`](file://load_relationships.py): Python loader script managed by `uv`. Parses GLEIF Relationship Records (RR) and populates inter-entity relationship edges (e.g. fund managers, parent consolidation, sub-funds) interleaved in `Entities`.
- [`SampleQueries.md`](file://SampleQueries.md): 6 ready-to-run queries for Cloud Spanner Studio covering Entity Resolution, S2 spatial indexing, and Property Graph traversal.
- [`query_spanner.py`](file://query_spanner.py): Sample Python script to query `LEIGraph` using S2 cell tokens and compute closest companies to customer coordinates (`--lat` and `--lng`).
- [`Makefile`](file://Makefile): Automation workflow for creating Spanner instances, initializing databases with DDL, updating schemas, loading data, loading relationships, and running graph queries.
- [`pyproject.toml`](file://pyproject.toml): Project dependencies for `uv`.
- [`.env.example`](file://.env.example): Environment variable template.

---

## Schema Architecture: Spanner Property Graph + S2 Indexing

### 1. Node Tables
- **`Entities`**: Represents legal entity nodes.
  - Primary Key: `LEI`
  - Attributes: LegalName, LegalJurisdiction, EntityCategory, EntityStatus, Dates, etc.
- **`EntityLocations`**: Represents geographic location nodes.
  - Primary Key: `LocationId` (`"{LEI}:{AddressType}"`)
  - Attributes: Address fields, City, Region, Country, Latitude, Longitude, `S2CellId`, `S2TokenStr`.

### 2. Edge / Relationship Tables
- **`EntityHasLocation`**: Relationship table linking Entities to Locations.
  - Primary Key: `(LEI, LocationId)`
  - Foreign Keys: `LEI` -> `Entities(LEI)`, `LocationId` -> `EntityLocations(LocationId)`
  - Attributes: `RelationshipType` (`'HAS_LEGAL_ADDRESS'`, `'HAS_HEADQUARTERS_ADDRESS'`), `CreatedAt`.
- **`EntityRelationships`**: Interleaved relationship table linking Entities to Entities (e.g., Parent/Subsidiary, Fund Managers).
  - Primary Key: `(StartLEI, EndLEI, RelationshipType)`
  - Interleaved in parent `Entities(LEI)` ON DELETE CASCADE.
  - Foreign Keys: `StartLEI` -> `Entities(LEI)`, `EndLEI` -> `Entities(LEI)`.
  - Secondary Index: `IndexEntityRelationshipsByEndLEI ON EntityRelationships(EndLEI, StartLEI)` for fast reverse graph lookups.

### 3. Multi-Level S2 Token Table
- **`LocationS2Tokens`**: Interleaved child table under `EntityLocations` for flexible multi-level S2 cell indexing.
  - Primary Key: `(LocationId, S2Level, S2Token)`
  - Interleaved in `EntityLocations` ON DELETE CASCADE.
  - Stores cell tokens across hierarchy levels (e.g., levels 6, 8, 10, 12, 14, 16, 18, 20) for variable-radius spatial range queries.

### 4. Property Graph Definition
```sql
CREATE PROPERTY GRAPH LEIGraph
  NODE TABLES (
    Entities KEY (LEI) LABEL Entity ...,
    EntityLocations KEY (LocationId) LABEL Location ...
  )
  EDGE TABLES (
    EntityHasLocation
      KEY (LEI, LocationId)
      SOURCE KEY (LEI) REFERENCES Entities (LEI)
      DESTINATION KEY (LocationId) REFERENCES EntityLocations (LocationId)
      LABEL HAS_LOCATION ...,
    EntityRelationships
      KEY (StartLEI, EndLEI, RelationshipType)
      SOURCE KEY (StartLEI) REFERENCES Entities (LEI)
      DESTINATION KEY (EndLEI) REFERENCES Entities (LEI)
      LABEL IS_RELATED_TO ...
  );
```

---

## Management via Makefile

Set up your `.env` file first:
```bash
cp .env.example .env
```

### Key Targets:
- `make dbcreate`: Creates the Spanner database and applies [`DDL.sql`](file://DDL.sql) schema.
- `make dbschema`: Updates the database DDL from [`DDL.sql`](file://DDL.sql).
- `make dbload-dryrun`: Runs [`load_spanner.py`](file://load_spanner.py) in dry-run mode to verify entity parsing, geocoding, and S2 generation.
- `make dbload`: Uploads entity graph nodes, locations, and S2 tokens into Cloud Spanner.
- `make dbload-rr-dryrun`: Runs [`load_relationships.py`](file://load_relationships.py) in dry-run mode to verify RR relationship records parsing.
- `make dbload-rr`: Uploads entity relationship edges (`EntityRelationships`) into Cloud Spanner.
- `make dbquery`: Executes the S2 Graph query against Spanner to find the 10 closest companies to Wilmington, Delaware.
- `make dbquery-sql`: Previews the parameterized Spanner Graph SQL/GQL query without connecting.
- `make instancecreate`: Creates a Cloud Spanner instance.

---

## Running with `uv`

All scripts are executed via `uv`:

```bash
# Preview query and S2 token parameters:
uv run query_spanner.py --print-sql

# Execute query against Cloud Spanner:
uv run query_spanner.py

# Query closest entities with custom coordinates, S2 level, and limit:
uv run query_spanner.py --lat 39.7459 --lng -75.5466 --s2-level 8 --limit 10
```
