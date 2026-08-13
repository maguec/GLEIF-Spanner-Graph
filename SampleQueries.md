# Cloud Spanner Graph & S2 Spatial Search: Sample Queries

This document contains **16 ready-to-run, high-performance Property Graph (GQL) queries** designed for **Google Cloud Spanner Studio** (GCP Console) demonstrating Entity Resolution (ER), S2 spatial indexing, full-text search, Property Graph traversal, and Graph Analytics against the `LEIGraph` schema in [`DDL.sql`](file://DDL.sql).

> **Note**: All queries use explicit literals (no parameter markers `@param`), so they can be copied and executed directly in **Spanner Studio** without configuring query parameters. All queries are written as pure **Property Graph GQL** (`GRAPH LEIGraph`) or **Spanner Graph SQL** (`GRAPH_TABLE`), indexed and optimized for sub-second execution against 1.5M+ entities.

---

## Quick Reference: Wilmington, DE Coordinates & S2 Tokens

* **Latitude / Longitude**: `39.7459`, `-75.5466`
* **S2 Level 8 Token (Signed INT64)**: `-8518857762334048256` (Hex string: `'89c6f'`, covers ~50 km radius, ~113,000 Delaware entity records)
* **S2 Level 10 Token (Signed INT64)**: `-8520147489473429504` (Hex string: `'89c25b'`, covers ~12 km radius, ~27,000 entity records)
* **Corporation Trust Center S2 Cell ID**: `-8518843220936128853` (Hex string: `'89c6fd39aedb86ab'`, 1209 Orange St, Wilmington, DE)
* **Degree-to-Radian Factor**: `0.017453292519943295` ($\pi / 180$)

---

## Query 1: S2 Spatial Proximity Graph Search (Closest 10 Companies to Wilmington, DE)

### Business Scenario
Find the 10 closest legal entities to a given customer coordinate (Wilmington, DE) by traversing the `LEIGraph` Property Graph without performing a full-table scan of 1.5M+ entities.

### How It Works
Traverses the `(e:Entity)-[r:HAS_LOCATION]->(l:Location)` property graph pattern using `GRAPH_TABLE`, anchored by an indexed S2 spatial subquery (`LocationS2Tokens`) on the ~50 km Level 8 cell (`-8518857762334048256`), and computes geodesic Haversine distance in ~1.9s.

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
FROM GRAPH_TABLE(
    LEIGraph
    MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location)
    WHERE l.LocationId IN (
        SELECT LocationId FROM LocationS2Tokens@{FORCE_INDEX=IndexLocationS2TokensByToken}
        WHERE S2Level = 8 AND S2Token = -8518857762334048256
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
LIMIT 10;
```

---

## Query 2: Multi-Signal Entity Matching (Fuzzy Name + S2 Spatial Blocking)

### Business Scenario
An incoming raw record mentions *"fund"* in the legal name near Wilmington, DE. You need to resolve this record against master LEI entities using combined text search and geospatial blocking signals over the graph.

### How It Works
Combines Spanner's full-text search index (`SEARCH_SUBSTRING(name_SubString, 'fund')`) with the S2 spatial index seek (`LocationS2Tokens`) directly inside `GRAPH_TABLE(LEIGraph MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location))`, returning candidate matches ordered by physical proximity in ~330ms.

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
FROM GRAPH_TABLE(
    LEIGraph
    MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location)
    WHERE e.LEI IN (
        SELECT LEI FROM Entities
        WHERE SEARCH_SUBSTRING(name_SubString, 'fund')
        LIMIT 200
    )
    AND l.LocationId IN (
        SELECT LocationId FROM LocationS2Tokens@{FORCE_INDEX=IndexLocationS2TokensByToken}
        WHERE S2Level = 8 AND S2Token = -8518857762334048256
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
        l.Latitude,
        l.Longitude
    )
) AS g
ORDER BY distance_km ASC
LIMIT 10;
```

---

## Query 3: Co-Location & Registered Agent Cluster Detection (Shell Network Discovery)

### Business Scenario
Anti-Money Laundering (AML) / KYC analysts need to detect clusters of distinct corporate entities sharing the exact same registered agent physical address (e.g. Corporation Trust Center at 1209 Orange St, Wilmington, DE).

### How It Works
Traverses the property graph pattern `MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location)` anchored by the exact registered agent S2 leaf cell ID (`-8518843220936128853`), joining co-located corporate pairs (`e1.LEI < e2.LEI`) in ~180ms.

```sql
WITH CTC_Entities AS (
    SELECT
        g.LocationId,
        g.FirstAddressLine,
        g.City,
        g.Region,
        g.PostalCode,
        g.S2TokenStr,
        g.LEI,
        g.LegalName,
        g.EntityStatus
    FROM GRAPH_TABLE(
        LEIGraph
        MATCH (e:Entity)-[r:HAS_LOCATION]->(l:Location)
        WHERE l.S2CellId = -8518843220936128853
        COLUMNS (
            l.LocationId,
            l.FirstAddressLine,
            l.City,
            l.Region,
            l.PostalCode,
            l.S2TokenStr,
            e.LEI,
            e.LegalName,
            e.EntityStatus
        )
    ) AS g
    LIMIT 25
)
SELECT
    e1.FirstAddressLine,
    e1.City,
    e1.Region,
    e1.PostalCode,
    e1.S2TokenStr,
    e1.LEI AS entity_1_lei,
    e1.LegalName AS entity_1_name,
    e1.EntityStatus AS entity_1_status,
    e2.LEI AS entity_2_lei,
    e2.LegalName AS entity_2_name,
    e2.EntityStatus AS entity_2_status
FROM CTC_Entities AS e1
JOIN CTC_Entities AS e2 ON e1.LEI < e2.LEI
LIMIT 20;
```

---

## Query 4: Spatial Disambiguation of Common Corporate Names

### Business Scenario
Multiple distinct corporations share generic keywords (e.g. *"Capital"*). A counterparty transaction occurred at coordinates `(39.7459, -75.5466)`. You need to rank candidate entities by spatial distance to disambiguate the true legal entity.

### How It Works
Combines Spanner Full-Text Search for keyword `'Capital'` with `GRAPH_TABLE` Property Graph traversal `(e:Entity)-[r:HAS_LOCATION]->(l:Location)`, computing geodesic distance to find the geographically closest matching corporation in ~160ms.

```sql
SELECT
    g.LEI,
    g.LegalName,
    g.EntityCategory,
    g.EntityStatus,
    g.LocationId,
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
    WHERE e.LEI IN (
        SELECT LEI FROM Entities
        WHERE SEARCH_SUBSTRING(name_SubString, 'Capital')
        LIMIT 50
    )
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
Entity resolution requires a complete 360° profile connecting an entity's formal **Legal Address** (e.g., Delaware registered address) with its operating **Headquarters Address** (e.g., Boston, New York, or London).

### How It Works
Traverses the `(e:Entity)-[r:HAS_LOCATION]->(l:Location)` graph pattern for candidate locations in the Wilmington spatial region, displaying both `LEGAL` and `HEADQUARTERS` relationships side-by-side in ~170ms.

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
    WHERE l.LocationId IN (
        SELECT LocationId
        FROM LocationS2Tokens@{FORCE_INDEX=IndexLocationS2TokensByToken}
        WHERE S2Level = 8 AND S2Token = -8518857762334048256
        LIMIT 100
    )
    COLUMNS (
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
    )
) AS g
ORDER BY g.LEI, g.AddressType
LIMIT 20;
```

---

## Query 6: Corporate Relationship Traversal (Fund Management & Consolidation Graph)

### Business Scenario
Analyze inter-entity corporate structures by traversing relationships between entities (such as fund management relationships, ultimate consolidation parents, direct consolidation parents, and sub-funds) using Cloud Spanner Property Graph (GQL).

### How It Works
Executes pure GQL graph pattern matching `MATCH (e1:Entity)-[r:IS_RELATED_TO]->(e2:Entity)` against `LEIGraph` with relationship type filtering, returning linked legal entities in ~90ms.

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

## Query 10: Unified Entity Profile (Joining Graph Nodes with Analytics)

### Business Scenario
After running Queries 7, 8, and 9 to populate `PageRankScore`, `CommunityId`, and `JaccardCommunityId`, query the merged 360° entity profile.

### How It Works
Uses `IndexEntityGraphAnalyticsByPageRank` to perform an instant index seek on the highest-ranking nodes in `EntityGraphAnalytics`, joining `GRAPH_TABLE(LEIGraph MATCH (e:Entity))` to fetch entity attributes in ~115ms.

```sql
SELECT 
    g.LEI,
    g.LegalName,
    g.EntityCategory,
    g.EntityStatus,
    a.PageRankScore,
    a.CommunityId,
    a.JaccardCommunityId,
    a.LastUpdated
FROM EntityGraphAnalytics@{FORCE_INDEX=IndexEntityGraphAnalyticsByPageRank} AS a
JOIN GRAPH_TABLE(
    LEIGraph
    MATCH (e:Entity)
    COLUMNS (
        e.LEI,
        e.LegalName,
        e.EntityCategory,
        e.EntityStatus
    )
) AS g ON a.LEI = g.LEI
ORDER BY a.PageRankScore DESC
LIMIT 50;
```

---

## Query 11: Web App Global PageRank Leaderboard (`Page Rank` Tab Query)

### Business Scenario
Powers the **Page Rank** tab leaderboard inside the LEI Graph Explorer web application. Lists top legal entities ordered by precomputed link-structure centrality (`PageRankScore`), joined with entity master graph nodes.

### How It Works
Performs an index seek on `IndexEntityGraphAnalyticsByPageRank` joined directly with `GRAPH_TABLE(LEIGraph MATCH (e:Entity))` to return top network hubs (e.g. Custody Bank of Japan, Master Trust Bank of Japan, BlackRock Asset Management, UBS, etc.) in ~85ms.

```sql
SELECT 
    a.LEI,
    g.LegalName,
    a.PageRankScore,
    a.CommunityId,
    a.JaccardCommunityId,
    g.EntityCategory,
    g.LegalJurisdiction,
    g.EntityStatus
FROM EntityGraphAnalytics@{FORCE_INDEX=IndexEntityGraphAnalyticsByPageRank} AS a
JOIN GRAPH_TABLE(
    LEIGraph
    MATCH (e:Entity)
    COLUMNS (
        e.LEI,
        e.LegalName,
        e.EntityCategory,
        e.LegalJurisdiction,
        e.EntityStatus
    )
) AS g ON a.LEI = g.LEI
ORDER BY a.PageRankScore DESC
LIMIT 20;
```

---

## Query 12: Web App Community Cluster Roster (`Community` Tab Starting Cluster Query)

### Business Scenario
Retrieves all starting corporate entities belonging to a specific precomputed graph community cluster (e.g. Community `#1283192` matching **Alphabet Inc.** and its corporate entities, or Community `#815529` matching **Custody Bank of Japan**).

### How It Works
Uses `IndexEntityGraphAnalyticsByCommunity` to perform an instant point seek for Community ID literal `1283192`, joined with `GRAPH_TABLE(LEIGraph MATCH (e:Entity))` to return community seed entities ordered by PageRank centrality in ~80ms.

```sql
SELECT 
    a.LEI,
    g.LegalName,
    a.PageRankScore,
    a.CommunityId,
    g.EntityCategory,
    g.LegalJurisdiction,
    g.EntityStatus
FROM EntityGraphAnalytics@{FORCE_INDEX=IndexEntityGraphAnalyticsByCommunity} AS a
JOIN GRAPH_TABLE(
    LEIGraph
    MATCH (e:Entity)
    COLUMNS (
        e.LEI,
        e.LegalName,
        e.EntityCategory,
        e.LegalJurisdiction,
        e.EntityStatus
    )
) AS g ON a.LEI = g.LEI
WHERE a.CommunityId = 1283192  -- Concrete starting point: Alphabet Inc. Community (#1283192)
ORDER BY a.PageRankScore DESC;
```

---

## Query 13: Spanner `GRAPH_TABLE` Multi-Source Community Outward Reachability Graph Query

### Business Scenario
Powers the **Community View** graph and reachability analysis in the application using Cloud Spanner's native **Property Graph GQL (`GRAPH_TABLE`)** query capabilities against `LEIGraph`. Starting from **ALL entities within a community cluster simultaneously** as multi-source graph seeds (Alphabet Inc. cluster `#1283192` or Custody Bank `#815529`), traverses graph paths up to 2 hops deep and ranks destinations by how many distinct community seed entities connect to each destination.

### How It Works
Anchors community seeds instantly via `IndexEntityGraphAnalyticsByCommunity`, performing indexed 1-hop and 2-hop `GRAPH_TABLE` edge pattern traversals in ~235ms.

```sql
WITH CommSeeds AS (
    SELECT a.LEI, g.LegalName
    FROM EntityGraphAnalytics@{FORCE_INDEX=IndexEntityGraphAnalyticsByCommunity} AS a
    JOIN GRAPH_TABLE(
        LEIGraph
        MATCH (e:Entity)
        COLUMNS (e.LEI, e.LegalName)
    ) AS g ON a.LEI = g.LEI
    WHERE a.CommunityId = 1283192  -- Concrete starting point: Alphabet Inc. Community (#1283192)
),
Hop1 AS (
    SELECT 
        g1.SeedLEI,
        g1.DestLEI,
        g1.DestName,
        g1.EntityCategory,
        g1.LegalJurisdiction,
        1 AS HopDistance
    FROM CommSeeds AS s
    JOIN GRAPH_TABLE(
        LEIGraph
        MATCH (e1:Entity)-[r:IS_RELATED_TO]->(e2:Entity)
        COLUMNS (
            e1.LEI AS SeedLEI,
            e2.LEI AS DestLEI,
            e2.LegalName AS DestName,
            e2.EntityCategory AS EntityCategory,
            e2.LegalJurisdiction AS LegalJurisdiction
        )
    ) AS g1 ON s.LEI = g1.SeedLEI
),
Hop2 AS (
    SELECT 
        h1.SeedLEI,
        g2.DestLEI,
        g2.DestName,
        g2.EntityCategory,
        g2.LegalJurisdiction,
        2 AS HopDistance
    FROM Hop1 AS h1
    JOIN GRAPH_TABLE(
        LEIGraph
        MATCH (mid:Entity)-[r:IS_RELATED_TO]->(e2:Entity)
        COLUMNS (
            mid.LEI AS MidLEI,
            e2.LEI AS DestLEI,
            e2.LegalName AS DestName,
            e2.EntityCategory AS EntityCategory,
            e2.LegalJurisdiction AS LegalJurisdiction
        )
    ) AS g2 ON h1.DestLEI = g2.MidLEI
),
CommOutwardGraph AS (
    SELECT * FROM Hop1
    UNION DISTINCT
    SELECT * FROM Hop2
)
SELECT 
    cg.DestLEI,
    cg.DestName AS DestinationName,
    COUNT(DISTINCT cg.SeedLEI) AS ConnectedCommunitySeedsCount,
    19 AS TotalCommunitySeeds,
    ROUND((COUNT(DISTINCT cg.SeedLEI) / 19.0) * 100, 1) AS ConnectedSeedPercentage,
    MIN(cg.HopDistance) AS MinHopDistance,
    COALESCE(dest_a.PageRankScore, 0.0) AS PageRankScore,
    cg.EntityCategory,
    cg.LegalJurisdiction
FROM CommOutwardGraph AS cg
LEFT JOIN EntityGraphAnalytics AS dest_a ON cg.DestLEI = dest_a.LEI
GROUP BY cg.DestLEI, cg.DestName, dest_a.PageRankScore, cg.EntityCategory, cg.LegalJurisdiction
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
