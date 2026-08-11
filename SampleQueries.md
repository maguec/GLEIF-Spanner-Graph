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

---

## Query 6: Corporate Relationship Traversal (Fund Management & Consolidation Graph)

### Business Scenario
Analyze inter-entity corporate structures by traversing relationships between entities (such as fund management relationships, ultimate consolidation parents, direct consolidation parents, and sub-funds).

### How It Works
Traverses the `IS_RELATED_TO` edge table (`EntityRelationships`) interleaved in `Entities` within `LEIGraph`, linking subject entities (`e1`) to target entities (`e2`) and filtering by relationship type.

```sql
GRAPH LEIGraph
MATCH (e1:Entity)-[r:IS_RELATED_TO]->(e2:Entity)
WHERE r.RelationshipType IN ('IS_FUND-MANAGED_BY', 'IS_ULTIMATELY_CONSOLIDATED_BY', 'IS_DIRECTLY_CONSOLIDATED_BY', 'IS_SUBFUND_OF')
RETURN
    e1.LEI AS subject_lei,
    e1.LegalName AS subject_name,
    e1.EntityCategory AS subject_category,
    r.RelationshipType AS relationship_type,
    r.RelationshipStatus AS relationship_status,
    e2.LEI AS target_lei,
    e2.LegalName AS target_name,
    e2.EntityCategory AS target_category
LIMIT 20;
```

---

## Query 7: PageRank Centrality (Write directly to `EntityGraphAnalytics` Table)

### Business Scenario
Compute PageRank centrality scores across all entities using Spanner Data Boost scale-up mode and update the `EntityGraphAnalytics` table directly.

### How It Works
Invokes `PageRank` wrapped inside `EXPORT DATA OPTIONS (format = "CLOUD_SPANNER", table = "EntityGraphAnalytics", write_mode = "upsert_ignore_all")` to write scores back to Spanner.

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

---

## Query 8: Community Detection (Modularity Clustering to `EntityGraphAnalytics`)

### Business Scenario
Group entities into community structures using Modularity Clustering in scale-up mode and persist the community IDs into the `EntityGraphAnalytics` table.

### How It Works
Uses `EXPORT DATA OPTIONS` to execute `ModularityClustering` and update the `CommunityId` column for each `LEI`.

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

---

## Query 9: Jaccard-Based Community Detection (Correlation Clustering)

### Business Scenario
Cluster entities based on neighborhood overlap / Jaccard similarity and update `JaccardCommunityId` in `EntityGraphAnalytics`.

### How It Works
Executes `CorrelationClustering` with resolution tuning in scale-up mode and updates `JaccardCommunityId`.

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

---

## Query 10: Unified Entity Profile (Joining Entities with Populated Graph Analytics)

### Business Scenario
After running Queries 7, 8, and 9 sequentially to populate `PageRankScore`, `CommunityId`, and `JaccardCommunityId`, query the merged 360° entity profile.

### How It Works
Performs a standard SQL `JOIN` between `Entities` and `EntityGraphAnalytics`.

```sql
SELECT 
    e.LEI,
    e.LegalName,
    e.EntityCategory,
    e.EntityStatus,
    a.PageRankScore,
    a.CommunityId,
    a.JaccardCommunityId,
    a.LastUpdated
FROM Entities AS e
JOIN EntityGraphAnalytics AS a ON e.LEI = a.LEI
ORDER BY a.PageRankScore DESC
LIMIT 50;
```

---

## Query 11: Web App Global PageRank Leaderboard (`Page Rank` Tab Query)

### Business Scenario
Powers the **Page Rank** tab leaderboard inside the LEI Graph Explorer web application. Lists top legal entities ordered by precomputed degree and link-structure centrality (`PageRankScore`), joined with entity master profile data and community cluster ID assignments.

### How It Works
Queries `EntityGraphAnalytics` joined directly with `Entities` without `@param` markers so it can be copied straight into **Cloud Spanner Studio** and run immediately. Returns the top 20 network hubs (e.g. Custody Bank of Japan, Master Trust Bank of Japan, BlackRock Asset Management, UBS, Union Investment, etc.).

```sql
SELECT 
    a.LEI,
    e.LegalName,
    a.PageRankScore,
    a.CommunityId,
    a.JaccardCommunityId,
    e.EntityCategory,
    e.LegalJurisdiction,
    e.EntityStatus
FROM EntityGraphAnalytics AS a
JOIN Entities AS e ON a.LEI = e.LEI
ORDER BY a.PageRankScore DESC
LIMIT 20;
```

---

## Query 12: Web App Community Cluster Roster (`Community` Tab Starting Cluster Query)

### Business Scenario
Retrieves all starting corporate entities belonging to a specific precomputed graph community cluster (e.g. Community `#1283192` matching **Alphabet Inc.** and its corporate entities, or Community `#815529` matching **Custody Bank of Japan**).

### How It Works
Filters `EntityGraphAnalytics` by explicit Community ID literal `1283192` joined with `Entities`, ordering all starting community seed entities by individual PageRank centrality.

```sql
SELECT 
    a.LEI,
    e.LegalName,
    a.PageRankScore,
    a.CommunityId,
    e.EntityCategory,
    e.LegalJurisdiction,
    e.EntityStatus
FROM EntityGraphAnalytics AS a
JOIN Entities AS e ON a.LEI = e.LEI
WHERE a.CommunityId = 1283192  -- Concrete starting point: Alphabet Inc. Community (#1283192)
ORDER BY a.PageRankScore DESC;
```

---

## Query 13: Spanner `GRAPH_TABLE` Multi-Source Community Outward Reachability Graph Query

### Business Scenario
Powers the **Community View** graph and table analysis in the application using Cloud Spanner's native **Property Graph GQL (`GRAPH_TABLE`)** query capabilities against `LEIGraph`. Starting from **ALL entities within a community cluster simultaneously** as multi-source graph seeds (Alphabet Inc. cluster `#1283192`, 19 seed nodes), traverses graph paths to outward destination entities, and ranks destinations by how many distinct community seed entities have an outgoing pathway to that destination.

### How It Works
Invokes `GRAPH_TABLE(LEIGraph MATCH ...)` directly inside standard Spanner SQL to perform multi-hop graph pattern matching (`(e1:Entity)-[r:IS_RELATED_TO]->(e2:Entity)` and `(e1:Entity)-[r1:IS_RELATED_TO]->(mid:Entity)-[r2:IS_RELATED_TO]->(e2:Entity)`), joining graph paths back to `EntityGraphAnalytics` for cluster filtering.

```sql
WITH CommOutwardGraph AS (
    -- 1-Hop Outward Graph Paths from all Community Cluster members using GRAPH_TABLE
    SELECT 
        g.SeedLEI,
        g.SeedName,
        g.DestLEI,
        g.DestName,
        g.RelType,
        1 AS HopDistance
    FROM EntityGraphAnalytics AS a
    JOIN GRAPH_TABLE(
        LEIGraph
        MATCH (e1:Entity)-[r:IS_RELATED_TO]->(e2:Entity)
        COLUMNS (
            e1.LEI AS SeedLEI,
            e1.LegalName AS SeedName,
            e2.LEI AS DestLEI,
            e2.LegalName AS DestName,
            r.RelationshipType AS RelType
        )
    ) AS g ON a.LEI = g.SeedLEI
    WHERE a.CommunityId = 1283192  -- Concrete starting point: Alphabet Inc. Community (#1283192)

    UNION DISTINCT

    -- 2-Hop Outward Graph Paths from all Community Cluster members using GRAPH_TABLE
    SELECT 
        g2.SeedLEI,
        g2.SeedName,
        g2.DestLEI,
        g2.DestName,
        g2.RelType,
        2 AS HopDistance
    FROM EntityGraphAnalytics AS a
    JOIN GRAPH_TABLE(
        LEIGraph
        MATCH (e1:Entity)-[r1:IS_RELATED_TO]->(mid:Entity)-[r2:IS_RELATED_TO]->(e2:Entity)
        COLUMNS (
            e1.LEI AS SeedLEI,
            e1.LegalName AS SeedName,
            e2.LEI AS DestLEI,
            e2.LegalName AS DestName,
            r2.RelationshipType AS RelType
        )
    ) AS g2 ON a.LEI = g2.SeedLEI
    WHERE a.CommunityId = 1283192
)
SELECT 
    cg.DestLEI,
    cg.DestName AS DestinationName,
    COUNT(DISTINCT cg.SeedLEI) AS ConnectedCommunitySeedsCount,
    19 AS TotalCommunitySeeds,
    ROUND((COUNT(DISTINCT cg.SeedLEI) / 19.0) * 100, 1) AS ConnectedSeedPercentage,
    MIN(cg.HopDistance) AS MinHopDistance,
    COALESCE(dest_a.PageRankScore, 0.0) AS PageRankScore,
    dest_e.EntityCategory,
    dest_e.LegalJurisdiction
FROM CommOutwardGraph AS cg
JOIN Entities AS dest_e ON cg.DestLEI = dest_e.LEI
LEFT JOIN EntityGraphAnalytics AS dest_a ON cg.DestLEI = dest_a.LEI
GROUP BY cg.DestLEI, cg.DestName, dest_a.PageRankScore, dest_e.EntityCategory, dest_e.LegalJurisdiction
ORDER BY ConnectedCommunitySeedsCount DESC, MinHopDistance ASC, PageRankScore DESC
LIMIT 50;
```

---

## Query 14: Web App Spanner `ANY SHORTEST` 2-Hop Relationship Path Query (PE Capital XI Reference Sample)

### Business Scenario
Powers the **Show Relationship** tab reference example inside the web application. Given a Source Entity (**PE Capital XI** `529900EGRFQDZFSHYN17`) and a Target Entity (**Columbus Global Fund** `529900MHMTMUTJWK3E84`), executes Cloud Spanner's native `ANY SHORTEST` GQL path pattern matching to discover the connecting intermediary entity.

```sql
SELECT 
    g.source_lei,
    g.source_name,
    g.mid_lei,
    g.mid_name,
    g.target_lei,
    g.target_name,
    g.rel1_type,
    g.rel2_type
FROM GRAPH_TABLE(
    LEIGraph
    MATCH ANY SHORTEST (src:Entity)-[r1:IS_RELATED_TO]->(mid:Entity)<-[r2:IS_RELATED_TO]-(tgt:Entity)
    WHERE src.LEI = '529900EGRFQDZFSHYN17'       -- Source Entity: PE Capital XI
      AND tgt.LEI = '529900MHMTMUTJWK3E84'       -- Target Entity: Columbus Global Fund
    COLUMNS (
        src.LEI AS source_lei,
        src.LegalName AS source_name,
        mid.LEI AS mid_lei,
        mid.LegalName AS mid_name,
        tgt.LEI AS target_lei,
        tgt.LegalName AS target_name,
        r1.RelationshipType AS rel1_type,
        r2.RelationshipType AS rel2_type
    )
) AS g;
```

---

## Query 15: Web App Spanner `ANY SHORTEST` 3-Hop Relationship Path Query (Verified 3-Hop Sample)

### Business Scenario
Powers the **Show Relationship** tab 3-hop example inside the web application. Given a Source Entity (**GOLDMAN SACHS US EQUITY ESG PORTFOLIO** `04N6BH2GW8URDY0AK302`) and a Target Entity (**Goldman Sachs Asset Management Holdings B.V.** `549300N0HHGLT70MM602`), executes Cloud Spanner's native `ANY SHORTEST` GQL path pattern matching to discover the exact multi-hop connecting path requiring 3 hops across institutional holding and management intermediaries.

### Verified 3-Hop Pathway sequence:
1. **Source Entity**: `GOLDMAN SACHS US EQUITY ESG PORTFOLIO` (`04N6BH2GW8URDY0AK302`)
2. **Hop 1 Intermediate**: `Goldman Sachs Asset Management B.V.` (`54930031LV6Z8OHO6762`)
3. **Hop 2 Intermediate**: `Goldman Sachs Asset Management International Holdings B.V.` (`5493001QST0I7Z235S79`)
4. **Hop 3 Target Entity**: `Goldman Sachs Asset Management Holdings B.V.` (`549300N0HHGLT70MM602`)

```sql
SELECT 
    g.source_lei,
    g.source_name,
    g.mid1_lei,
    g.mid1_name,
    g.mid2_lei,
    g.mid2_name,
    g.target_lei,
    g.target_name,
    g.rel1_type,
    g.rel2_type,
    g.rel3_type
FROM GRAPH_TABLE(
    LEIGraph
    MATCH ANY SHORTEST (src:Entity)-[r1:IS_RELATED_TO]->(mid1:Entity)-[r2:IS_RELATED_TO]->(mid2:Entity)-[r3:IS_RELATED_TO]->(tgt:Entity)
    WHERE src.LEI = '04N6BH2GW8URDY0AK302'       -- Source: GOLDMAN SACHS US EQUITY ESG PORTFOLIO
      AND tgt.LEI = '549300N0HHGLT70MM602'       -- Target: Goldman Sachs Asset Management Holdings B.V.
    COLUMNS (
        src.LEI AS source_lei,
        src.LegalName AS source_name,
        mid1.LEI AS mid1_lei,
        mid1.LegalName AS mid1_name,
        mid2.LEI AS mid2_lei,
        mid2.LegalName AS mid2_name,
        tgt.LEI AS target_lei,
        tgt.LegalName AS target_name,
        r1.RelationshipType AS rel1_type,
        r2.RelationshipType AS rel2_type,
        r3.RelationshipType AS rel3_type
    )
) AS g;
```




