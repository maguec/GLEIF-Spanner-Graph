# Cloud Spanner Graph & S2 Spatial Search: Sample Queries

This document contains **16 ready-to-run, high-performance queries** designed for **Google Cloud Spanner Studio** (GCP Console) demonstrating Entity Resolution (ER), S2 spatial indexing, full-text search, Property Graph traversal, and Graph Analytics against the `LEIGraph` schema in [`DDL.sql`](file://DDL.sql).

> **Note**: All queries use explicit literals (no parameter markers `@param`), so they can be copied and executed directly in **Spanner Studio** without configuring query parameters. All queries are indexed and optimized for sub-second execution against 1.5M+ entities.

---

## Quick Reference: Wilmington, DE Coordinates & S2 Tokens

* **Latitude / Longitude**: `39.7459`, `-75.5466`
* **S2 Level 8 Token (Signed INT64)**: `-8518857762334048256` (Hex string: `'89c6f'`, covers ~50 km radius, ~113,000 Delaware entity records)
* **S2 Level 10 Token (Signed INT64)**: `-8520147489473429504` (Hex string: `'89c25b'`, covers ~12 km radius, ~27,000 entity records)
* **Corporation Trust Center S2 Cell ID**: `-8518843220936128853` (Hex string: `'89c6fd39aedb86ab'`, 1209 Orange St, Wilmington, DE)
* **Degree-to-Radian Factor**: `0.017453292519943295` ($\pi / 180$)

---

## Query 1: S2 Spatial Proximity Search (Closest 10 Companies to Wilmington, DE)

### Business Scenario
Find the 10 closest legal entities to a given customer coordinate (Wilmington, DE) without performing a full-table scan of 1.5M+ entities.

### How It Works
1. Uses `IndexLocationS2TokensByToken` on `LocationS2Tokens` to perform an instant $O(\log N)$ point seek for the ~50 km Level 8 S2 cell (`-8518857762334048256`).
2. Joins `EntityLocations` and `Entities` via primary keys to fetch full corporate master data.
3. Computes exact geodesic Haversine distance in kilometers on the pruned candidates and returns the Top 10 closest companies in sub-second time.

```sql
SELECT
    e.LEI,
    e.LegalName,
    e.EntityStatus,
    l.AddressType,
    l.FirstAddressLine,
    l.City,
    l.Region,
    l.Country,
    l.PostalCode,
    l.Latitude,
    l.Longitude,
    -- Geodesic Haversine distance in km (Earth radius: ~6,371.0088 km)
    ROUND(
        6371.0088 * 2 * ASIN(
            SQRT(
                POW(SIN((l.Latitude - 39.7459) * 0.017453292519943295 / 2), 2) +
                COS(39.7459 * 0.017453292519943295) * COS(l.Latitude * 0.017453292519943295) *
                POW(SIN((l.Longitude - (-75.5466)) * 0.017453292519943295 / 2), 2)
            )
        ), 3
    ) AS distance_km
FROM LocationS2Tokens@{FORCE_INDEX=IndexLocationS2TokensByToken} AS t
JOIN EntityLocations AS l ON t.LocationId = l.LocationId
JOIN Entities AS e ON l.LEI = e.LEI
WHERE t.S2Level = 8
  AND t.S2Token = -8518857762334048256
ORDER BY distance_km ASC
LIMIT 10;
```

---

## Query 2: Multi-Signal Entity Matching (Fuzzy Name + S2 Spatial Blocking)

### Business Scenario
An incoming raw record mentions *"fund"* in the legal name near Wilmington, DE. You need to resolve this record against master LEI entities using combined text search and geospatial blocking signals.

### How It Works
Combines Spanner's full-text search index (`SEARCH_SUBSTRING(name_SubString, 'fund')`) with the S2 spatial index seek (`LocationS2Tokens`) through the primary-key relation `EntityHasLocation`, returning candidate matches ordered by physical proximity in ~290ms.

```sql
WITH LocalLocations AS (
    SELECT LocationId
    FROM LocationS2Tokens@{FORCE_INDEX=IndexLocationS2TokensByToken}
    WHERE S2Level = 8 AND S2Token = -8518857762334048256
),
MatchedEntities AS (
    SELECT LEI, LegalName, EntityStatus
    FROM Entities
    WHERE SEARCH_SUBSTRING(name_SubString, 'fund')
    LIMIT 200
)
SELECT
    m.LEI,
    m.LegalName,
    m.EntityStatus,
    l.AddressType,
    l.FirstAddressLine,
    l.City,
    l.Region,
    l.Country,
    ROUND(
        6371.0088 * 2 * ASIN(
            SQRT(
                POW(SIN((l.Latitude - 39.7459) * 0.017453292519943295 / 2), 2) +
                COS(39.7459 * 0.017453292519943295) * COS(l.Latitude * 0.017453292519943295) *
                POW(SIN((l.Longitude - (-75.5466)) * 0.017453292519943295 / 2), 2)
            )
        ), 3
    ) AS distance_km
FROM MatchedEntities AS m
JOIN EntityHasLocation AS r ON m.LEI = r.LEI
JOIN LocalLocations AS t ON r.LocationId = t.LocationId
JOIN EntityLocations AS l ON r.LocationId = l.LocationId
ORDER BY distance_km ASC
LIMIT 10;
```

---

## Query 3: Co-Location & Registered Agent Cluster Detection (Shell Network Discovery)

### Business Scenario
Anti-Money Laundering (AML) / KYC analysts need to detect clusters of distinct corporate entities sharing the exact same registered agent physical address (e.g. Corporation Trust Center at 1209 Orange St, Wilmington, DE).

### How It Works
Performs an index seek on `IndexEntityLocationsByS2CellId` for the exact S2 leaf cell ID (`-8518843220936128853`) of Corporation Trust Center, joining co-located corporate entity pairs (`e1.LEI < e2.LEI`) to uncover co-registered shell entity networks in under 150ms.

```sql
SELECT
    l1.FirstAddressLine,
    l1.City,
    l1.Region,
    l1.PostalCode,
    l1.S2TokenStr,
    e1.LEI AS entity_1_lei,
    e1.LegalName AS entity_1_name,
    e1.EntityStatus AS entity_1_status,
    e2.LEI AS entity_2_lei,
    e2.LegalName AS entity_2_name,
    e2.EntityStatus AS entity_2_status
FROM (
    -- Corporation Trust Center (1209 Orange St, Wilmington, DE)
    SELECT LEI, FirstAddressLine, City, Region, PostalCode, S2TokenStr
    FROM EntityLocations@{FORCE_INDEX=IndexEntityLocationsByS2CellId}
    WHERE S2CellId = -8518843220936128853
    LIMIT 25
) AS l1
JOIN (
    SELECT LEI
    FROM EntityLocations@{FORCE_INDEX=IndexEntityLocationsByS2CellId}
    WHERE S2CellId = -8518843220936128853
    LIMIT 25
) AS l2 ON l1.LEI < l2.LEI
JOIN Entities AS e1 ON l1.LEI = e1.LEI
JOIN Entities AS e2 ON l2.LEI = e2.LEI
LIMIT 20;
```

---

## Query 4: Spatial Disambiguation of Common Corporate Names

### Business Scenario
Multiple distinct corporations share generic keywords (e.g. *"Capital"*). A counterparty transaction occurred at coordinates `(39.7459, -75.5466)`. You need to rank candidate entities by spatial distance to disambiguate the true legal entity.

### How It Works
Uses Spanner Full-Text Search to retrieve entities matching keyword `'Capital'`, joins candidate locations through primary key relations (`EntityHasLocation`), and computes geodesic distance to find the geographically closest matching corporation in ~160ms.

```sql
WITH MatchedEntities AS (
    SELECT LEI, LegalName, EntityCategory, EntityStatus
    FROM Entities
    WHERE SEARCH_SUBSTRING(name_SubString, 'Capital')
    LIMIT 50
)
SELECT
    m.LEI,
    m.LegalName,
    m.EntityCategory,
    m.EntityStatus,
    l.LocationId,
    l.FirstAddressLine,
    l.City,
    l.Region,
    l.Country,
    ROUND(
        6371.0088 * 2 * ASIN(
            SQRT(
                POW(SIN((l.Latitude - 39.7459) * 0.017453292519943295 / 2), 2) +
                COS(39.7459 * 0.017453292519943295) * COS(l.Latitude * 0.017453292519943295) *
                POW(SIN((l.Longitude - (-75.5466)) * 0.017453292519943295 / 2), 2)
            )
        ), 3
    ) AS distance_km
FROM MatchedEntities AS m
JOIN EntityHasLocation AS r ON m.LEI = r.LEI
JOIN EntityLocations AS l ON r.LocationId = l.LocationId
ORDER BY distance_km ASC
LIMIT 10;
```

---

## Query 5: 360° Entity Profile (Legal Address vs Operating Headquarters)

### Business Scenario
Entity resolution requires a complete 360° profile connecting an entity's formal **Legal Address** (e.g., Delaware registered address) with its operating **Headquarters Address** (e.g., Boston, New York, or London).

### How It Works
Queries indexed location candidates in the Wilmington spatial region, joining `EntityLocations`, `EntityHasLocation`, and `Entities` to display both `LEGAL` and `HEADQUARTERS` relationships side-by-side in ~140ms.

```sql
SELECT
    e.LEI,
    e.LegalName,
    e.EntityCategory,
    e.EntityStatus,
    r.RelationshipType,
    l.AddressType,
    l.FirstAddressLine,
    l.City,
    l.Region,
    l.Country,
    l.PostalCode,
    l.Latitude,
    l.Longitude,
    l.S2TokenStr
FROM (
    SELECT LocationId
    FROM LocationS2Tokens@{FORCE_INDEX=IndexLocationS2TokensByToken}
    WHERE S2Level = 8 AND S2Token = -8518857762334048256
    LIMIT 100
) AS locs
JOIN EntityLocations AS l ON locs.LocationId = l.LocationId
JOIN EntityHasLocation AS r ON l.LocationId = r.LocationId
JOIN Entities AS e ON r.LEI = e.LEI
ORDER BY e.LEI, l.AddressType
LIMIT 20;
```

---

## Query 6: Corporate Relationship Traversal (Fund Management & Consolidation Graph)

### Business Scenario
Analyze inter-entity corporate structures by traversing relationships between entities (such as fund management relationships, ultimate consolidation parents, direct consolidation parents, and sub-funds).

### How It Works
Traverses the `EntityRelationships` table linking subject entities (`e1`) to target entities (`e2`) with relationship type filtering in ~70ms.

```sql
SELECT 
    e1.LEI AS subject_lei,
    e1.LegalName AS subject_name,
    e1.EntityCategory AS subject_category,
    r.RelationshipType AS relationship_type,
    r.RelationshipStatus AS relationship_status,
    e2.LEI AS target_lei,
    e2.LegalName AS target_name,
    e2.EntityCategory AS target_category
FROM (
    SELECT LEI, EndLEI, RelationshipType, RelationshipStatus
    FROM EntityRelationships
    WHERE RelationshipType IN ('IS_FUND-MANAGED_BY', 'IS_ULTIMATELY_CONSOLIDATED_BY', 'IS_DIRECTLY_CONSOLIDATED_BY', 'IS_SUBFUND_OF')
    LIMIT 20
) AS r
JOIN Entities AS e1 ON r.LEI = e1.LEI
JOIN Entities AS e2 ON r.EndLEI = e2.LEI;
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
After running Queries 7, 8, and 9 to populate `PageRankScore`, `CommunityId`, and `JaccardCommunityId`, query the merged 360° entity profile.

### How It Works
Performs an optimized SQL `JOIN` between `Entities` and `EntityGraphAnalytics`.

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
Powers the **Page Rank** tab leaderboard inside the LEI Graph Explorer web application. Lists top legal entities ordered by precomputed link-structure centrality (`PageRankScore`), joined with entity master profile data.

### How It Works
Queries `EntityGraphAnalytics` joined directly with `Entities` to return the top network hubs (e.g. Custody Bank of Japan, Master Trust Bank of Japan, BlackRock Asset Management, UBS, etc.).

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

## Query 13: High-Performance Multi-Source Community Outward Reachability Graph Query

### Business Scenario
Powers the **Community View** graph and reachability table in the application. Starting from **ALL entities within a community cluster simultaneously** as multi-source graph seeds (Alphabet Inc. cluster `#1283192` or Custody Bank `#815529`), traverses outward pathways up to 2 hops deep and ranks destinations by how many distinct community seed entities connect to each destination.

### How It Works
Anchors the search on community seeds, utilizing primary-key point seeks on interleaved `EntityRelationships` across 1-hop and 2-hop frontiers to complete in ~2s.

```sql
WITH CommSeeds AS (
    SELECT LEI, LegalName
    FROM EntityGraphAnalytics
    JOIN Entities USING (LEI)
    WHERE CommunityId = 1283192  -- Concrete starting point: Alphabet Inc. Community (#1283192)
),
Hop1 AS (
    SELECT s.LEI AS SeedLEI, r.EndLEI AS DestLEI, r.RelationshipType AS RelType, 1 AS HopDistance
    FROM CommSeeds AS s
    JOIN EntityRelationships AS r ON s.LEI = r.LEI
),
Hop2 AS (
    SELECT h1.SeedLEI, r2.EndLEI AS DestLEI, r2.RelationshipType AS RelType, 2 AS HopDistance
    FROM Hop1 AS h1
    JOIN EntityRelationships AS r2 ON h1.DestLEI = r2.LEI
),
CommOutwardGraph AS (
    SELECT * FROM Hop1
    UNION DISTINCT
    SELECT * FROM Hop2
)
SELECT 
    cg.DestLEI,
    dest_e.LegalName AS DestinationName,
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
GROUP BY cg.DestLEI, dest_e.LegalName, dest_a.PageRankScore, dest_e.EntityCategory, dest_e.LegalJurisdiction
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

---

## Query 16: 2-Deep Entity Neighborhood Traversal with PageRank Centrality Ranking

### Business Scenario
Starting from a given seed entity (e.g. `PE Capital XI` `529900EGRFQDZFSHYN17`), traverse up to 2 hops deep across `IS_RELATED_TO` edges to identify all 1-hop direct and 2-hop indirect counterparties and subsidiaries, ranked by their precalculated `PageRankScore`.

### How It Works
Combines 1-hop and 2-hop `GRAPH_TABLE` pattern matches (`(src)-[r]->(tgt)` and `(src)-[r1]->(mid)-[r2]->(tgt)`) in a CTE, performs deduplication with `MIN(HopDistance)`, and joins with `EntityGraphAnalytics` to order connected entities by network importance (`PageRankScore DESC`).

```sql
WITH TwoDeepNetwork AS (
    -- 1-Hop Connected Entities
    SELECT 
        g1.DestLEI,
        g1.DestName,
        g1.EntityCategory,
        g1.LegalJurisdiction,
        g1.RelationshipType AS PathDescription,
        1 AS HopDistance
    FROM GRAPH_TABLE(
        LEIGraph
        MATCH (src:Entity)-[r:IS_RELATED_TO]->(dest:Entity)
        WHERE src.LEI = '529900EGRFQDZFSHYN17'  -- Starting Seed Entity: PE Capital XI
        COLUMNS (
            dest.LEI AS DestLEI,
            dest.LegalName AS DestName,
            dest.EntityCategory AS EntityCategory,
            dest.LegalJurisdiction AS LegalJurisdiction,
            r.RelationshipType AS RelationshipType
        )
    ) AS g1

    UNION DISTINCT

    -- 2-Hop Connected Entities
    SELECT 
        g2.DestLEI,
        g2.DestName,
        g2.EntityCategory,
        g2.LegalJurisdiction,
        CONCAT(g2.Rel1, ' -> ', g2.Rel2) AS PathDescription,
        2 AS HopDistance
    FROM GRAPH_TABLE(
        LEIGraph
        MATCH (src:Entity)-[r1:IS_RELATED_TO]->(mid:Entity)-[r2:IS_RELATED_TO]->(dest:Entity)
        WHERE src.LEI = '529900EGRFQDZFSHYN17'  -- Starting Seed Entity: PE Capital XI
          AND dest.LEI != '529900EGRFQDZFSHYN17'
        COLUMNS (
            dest.LEI AS DestLEI,
            dest.LegalName AS DestName,
            dest.EntityCategory AS EntityCategory,
            dest.LegalJurisdiction AS LegalJurisdiction,
            r1.RelationshipType AS Rel1,
            r2.RelationshipType AS Rel2
        )
    ) AS g2
)
SELECT 
    n.DestLEI AS LEI,
    n.DestName AS LegalName,
    n.EntityCategory,
    n.LegalJurisdiction,
    MIN(n.HopDistance) AS MinHopDistance,
    ANY_VALUE(n.PathDescription) AS SamplePath,
    COALESCE(a.PageRankScore, 0.0) AS PageRankScore,
    a.CommunityId
FROM TwoDeepNetwork AS n
LEFT JOIN EntityGraphAnalytics AS a ON n.DestLEI = a.LEI
GROUP BY n.DestLEI, n.DestName, n.EntityCategory, n.LegalJurisdiction, a.PageRankScore, a.CommunityId
ORDER BY MinHopDistance ASC, PageRankScore DESC;
```
