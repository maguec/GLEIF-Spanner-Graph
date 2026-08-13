# Cloud Spanner S2 Geo-Spatial & Graph Database Data Loader

This repository provides a complete solution for loading Legal Entity Identifier (LEI) data from [GLEIF.org](https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/) into Google Cloud Spanner using **S2 Geometry multi-level spatial indexing** and **Spanner Property Graph (GQL)**.

The spatial indexing follows the flexible multi-level S2 approach described in:
[*Geo-Spatial Indexing on Google Cloud Spanner with S2*](https://medium.com/google-cloud/geo-spatial-indexing-on-google-cloud-spanner-with-s2-81a013d772c4).

---

## Prerequisites

- Google Cloud Project with Spanner enabled
- [Google Cloud SDK installed](https://cloud.google.com/sdk?e=ahttps://cloud.google.com/sdk?e=a) and authenticated `gcloud auth application-default login`
- [uv installed](https://docs.astral.sh/uv/getting-started/installation/)
- python installed
- LEI data downloaded from GLEIF.org - see [README.md](file://data/README.md)

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

## Management via Makefile

Set up your `.env` file first:
```bash
cp .env.example .env
```

### See options to create database
```bash
make
```
---

## Loading data

```bash
# load the data
uv run load_spanner.py --file data/<GOLDENCOPY-LEI-FILENAME>
uv run load_relationships.py --file data/<RELATIONSHIP-RECORDS-FILENAME>
```

## Run the queries to populate the EntityGraphAnalytics tables

### Run the PageRank Algorithm
```sql
EXPORT DATA OPTIONS (
  format = "CLOUD_SPANNER",
  table = "EntityGraphAnalytics",
  write_mode = 'upsert_ignore_all'
) AS
GRAPH LEIGraph
CALL PageRank(
    node_labels => ['Entity'],
    edge_labels => ['IS_RELATED_TO'],
    damping_factor => 0.85,
    max_iterations => 20
) YIELD node, score
RETURN 
    node.LEI AS LEI,
    score AS PageRankScore;
```

### Run the Modularity Community Detection Algorithm
```sql
EXPORT DATA OPTIONS (
  format = "CLOUD_SPANNER",
  table = "EntityGraphAnalytics",
  write_mode = 'upsert_ignore_all'
) AS
GRAPH LEIGraph
CALL ModularityClustering(
    node_labels => ['Entity'],
    edge_labels => ['IS_RELATED_TO'],
    resolution => 1.0
) YIELD node, cluster
RETURN 
    node.LEI AS LEI,
    cluster AS CommunityId;
```

### Run the Jaccard Community Detection Algorithm
```sql
EXPORT DATA OPTIONS (
  format = "CLOUD_SPANNER",
  table = "EntityGraphAnalytics",
  write_mode = 'upsert_ignore_all'
) AS
GRAPH LEIGraph
CALL CorrelationClustering(
    node_labels => ['Entity'],
    edge_labels => ['IS_RELATED_TO'],
    resolution => 0.5
) YIELD node, cluster
RETURN 
    node.LEI AS LEI,
    cluster AS JaccardCommunityId;
```

## Running the application

```bash
uv run app.py
```

Connect to the [local webserver](http://127.0.0.1:5001)

## Querying data

### Sample queries to run in Spanner Studio

[Sample Queries.md](file://SampleQueries.md)

### Sample scripts

```bash
# Preview query and S2 token parameters:
uv run query_spanner.py --print-sql

# Execute query against Cloud Spanner:
uv run query_spanner.py

# Query closest entities with custom coordinates, S2 level, and limit:
uv run query_spanner.py --lat 39.7459 --lng -75.5466 --s2-level 8 --limit 10
```
