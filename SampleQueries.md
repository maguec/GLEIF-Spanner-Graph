# Cloud Spanner Graph & S2 Spatial Search: Sample Queries

This document contains **5 ready-to-run queries** designed for **Google Cloud Spanner Studio** (GCP Console) demonstrating Entity Resolution (ER), S2 spatial indexing, full-text search, and Property Graph traversal against the `LEIGraph` schema in [`DDL.sql`](file://DDL.sql).

> **Note**: All queries use explicit literals (no parameter markers `@param`), so they can be copied and executed directly in **Spanner Studio** without configuring query parameters.

---

## Quick Reference: Wilmington, DE Coordinates & S2 Tokens

* **Latitude / Longitude**: `39.7459`, `-75.5466`
* **S2 Level 8 Token (Signed INT64)**: `-8518857762334048256` (Hex string: `'89c6f'`, covers ~50 km radius)
* **S2 Level 10 Token (Signed INT64)**: `-8520147489473429504` (Hex string: `'89c25b'`, covers ~12 km radius)
* **Degree-to-Radian Factor**: `0.017453292519943295` ($\pi / 180$)

---

## Query 1: S2 Spatial Proximity Search (Closest 10 Companies to Wilmington, DE)

### Business Scenario
Find the 10 closest legal entities to a given customer coordinate (Wilmington, DE) without performing a full-table scan of 100K+ entities.

### How It Works
1. Hits `IndexLocationS2TokensByToken` on `LocationS2Tokens` to perform an instant $O(\log N)$ point seek for the ~50 km Level 8 S2 cell (`-8518857762334048256`), pruning 100K+ records down to ~50 candidates.
2. Traverses `LEIGraph` via `GRAPH_TABLE` to join matched location nodes with company entity records.
3. Computes exact geodesic Haversine distance in kilometers on only the pruned candidates and returns the Top 10.

```sql
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
    -- Geodesic Haversine distance in km (Earth radius: ~6,371.0088 km)
    ROUND(
        6371.0088 * 2 * ASIN(
            SQRT(
                POW(SIN((g.Latitude - 39.7459) * 0.017453292519943295 / 2), 2) +
                COS(39.7459 * 0.017453292519943295) * COS(g.Latitude * 0.017453292519943295) *
                POW(SIN((g.Longitude - (-75.5466)) * 0.017453292519943295 / 2), 2)
            )
        ), 3
    ) AS distance_km
FROM LocationS2Tokens AS t
JOIN GRAPH_TABLE(
    LEIGraph
    MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location)
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
        l.Longitude,
        l.S2CellId,
        l.S2TokenStr
    )
) AS g ON t.LocationId = g.LocationId
WHERE t.S2Level = 8
  AND t.S2Token = -8518857762334048256
ORDER BY distance_km ASC
LIMIT 10;
```

---

## Query 2: Multi-Signal Entity Matching (Fuzzy Name + S2 Spatial Blocking)

### Business Scenario
An incoming raw record mentions *"fund"* in the legal name near Wilmington, DE. You need to resolve this record against master LEI entities using combined text and geospatial signals.

### How It Works
Combines Spanner's full-text search index (`SEARCH_SUBSTRING(e.name_SubString, 'fund')`) with the S2 Level 8 spatial index seek (`LocationS2Tokens`), resolving candidates in milliseconds.

```sql
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
    ROUND(
        6371.0088 * 2 * ASIN(
            SQRT(
                POW(SIN((g.Latitude - 39.7459) * 0.017453292519943295 / 2), 2) +
                COS(39.7459 * 0.017453292519943295) * COS(g.Latitude * 0.017453292519943295) *
                POW(SIN((g.Longitude - (-75.5466)) * 0.017453292519943295 / 2), 2)
            )
        ), 3
    ) AS distance_km
FROM LocationS2Tokens AS t
JOIN GRAPH_TABLE(
    LEIGraph
    MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location)
    WHERE SEARCH_SUBSTRING(e.name_SubString, 'fund')
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
        l.Latitude,
        l.Longitude
    )
) AS g ON t.LocationId = g.LocationId
WHERE t.S2Level = 8
  AND t.S2Token = -8518857762334048256
ORDER BY distance_km ASC
LIMIT 10;
```

---

## Query 3: Co-Location & Registered Agent Cluster Detection (Shell Network Discovery)

### Business Scenario
Anti-Money Laundering (AML) / KYC analysts need to detect networks of distinct corporate entities that share the exact same registered agent or physical address in Wilmington, DE.

### How It Works
Executes a pure GQL Property Graph query pattern: `(e1:Entity)-[:HAS_LOCATION]->(l:Location)<-[:HAS_LOCATION]-(e2:Entity)`. Filters on `e1.LEI < e2.LEI` to avoid symmetric self-joins and return distinct co-located corporate pairs.

```sql
GRAPH LEIGraph
MATCH (e1:Entity)-[:HAS_LOCATION]->(l:Location)<-[:HAS_LOCATION]-(e2:Entity)
WHERE e1.LEI < e2.LEI
  AND (l.City = 'WILMINGTON' OR l.S2TokenStr LIKE '89c6f%')
RETURN
    l.LocationId,
    l.FirstAddressLine,
    l.City,
    l.Region,
    l.PostalCode,
    e1.LEI AS entity_1_lei,
    e1.LegalName AS entity_1_name,
    e1.EntityStatus AS entity_1_status,
    e2.LEI AS entity_2_lei,
    e2.LegalName AS entity_2_name,
    e2.EntityStatus AS entity_2_status
LIMIT 20;
```

---

## Query 4: Spatial Disambiguation of Common / Ambiguous Corporate Names

### Business Scenario
Multiple distinct corporations share generic keywords (e.g. *"Capital"*). A counterparty transaction occurred at coordinates `(39.7459, -75.5466)`. You need to rank candidate entities by spatial distance to disambiguate the true legal entity.

### How It Works
Uses Spanner Full-Text Search to retrieve entities matching keyword `'Capital'`, computes true distance from the target site, and ranks closest matches to the top.

```sql
SELECT
    g.LEI,
    g.LegalName,
    g.EntityCategory,
    g.EntityStatus,
    g.FirstAddressLine,
    g.City,
    g.Region,
    g.Country,
    ROUND(
        6371.0088 * 2 * ASIN(
            SQRT(
                POW(SIN((g.Latitude - 39.7459) * 0.017453292519943295 / 2), 2) +
                COS(39.7459 * 0.017453292519943295) * COS(g.Latitude * 0.017453292519943295) *
                POW(SIN((g.Longitude - (-75.5466)) * 0.017453292519943295 / 2), 2)
            )
        ), 3
    ) AS distance_km
FROM GRAPH_TABLE(
    LEIGraph
    MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location)
    WHERE SEARCH_SUBSTRING(e.name_SubString, 'Capital')
    COLUMNS (
        e.LEI,
        e.LegalName,
        e.EntityCategory,
        e.EntityStatus,
        l.LocationId,
        l.FirstAddressLine,
        l.City,
        l.Region,
        l.Country,
        l.Latitude,
        l.Longitude
    )
) AS g
ORDER BY distance_km ASC
LIMIT 10;
```

---

## Query 5: 360° Entity Profile (Legal Address vs Operating Headquarters Graph Traversal)

### Business Scenario
Entity resolution requires a complete 360° profile connecting an entity's formal **Legal Address** (e.g., Delaware or Cayman registered address) with its operating **Headquarters Address** (e.g., New York or London).

### How It Works
Traverses both `HAS_LEGAL_ADDRESS` and `HAS_HEADQUARTERS_ADDRESS` edges connected to entity nodes in `LEIGraph`, retrieving complete multi-jurisdiction addresses and S2 tokens in a single query.

```sql
SELECT
    g.LEI,
    g.LegalName,
    g.EntityCategory,
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
    g.S2TokenStr
FROM GRAPH_TABLE(
    LEIGraph
    MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location)
    WHERE l.City = 'WILMINGTON' AND l.Region = 'DE'
    COLUMNS (
        e.LEI,
        e.LegalName,
        e.EntityCategory,
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
        l.Longitude,
        l.S2TokenStr
    )
) AS g
ORDER BY g.LEI, g.AddressType
LIMIT 20;
```
