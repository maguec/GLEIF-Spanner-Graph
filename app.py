#!/usr/bin/env python3
"""
Integrated Spanner Type-Ahead & 3-Hop D3.js Graph Analysis App
Combines Spanner full-text/substring search type-ahead with dynamic 3-hop GQL graph query traversal
rendered in an interactive D3.js force-directed visualization.
"""

import os
import json

# Set gRPC C-core environment suppressions before importing spanner client to stop fork poll list spew
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "1")
os.environ.setdefault("GRPC_VERBOSITY", "ERROR")
os.environ.setdefault("GOOGLE_CLOUD_SPANNER_DISABLE_BUILTIN_METRICS", "true")

from flask import Flask, render_template, request, jsonify, redirect
from google.cloud import spanner

# Load environment variables from .env if present
if os.path.exists(".env"):
    with open(".env", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

GOOGLE_PROJECT = os.getenv("GOOGLE_PROJECT", "mague-tf")
GOOGLE_SPANNER_INSTANCE = os.getenv("GOOGLE_SPANNER_INSTANCE", "shared-demos")
GOOGLE_SPANNER_DATABASE = os.getenv("GOOGLE_SPANNER_DATABASE", "spglobal")

# Initialize Flask App
app = Flask(__name__)

# Post-fork per-process lazy Spanner database client getter
_db_cache = {}


def get_database():
    pid = os.getpid()
    if pid not in _db_cache:
        spanner_client = spanner.Client(project=GOOGLE_PROJECT, disable_builtin_metrics=True)
        instance = spanner_client.instance(GOOGLE_SPANNER_INSTANCE)
        _db_cache[pid] = instance.database(GOOGLE_SPANNER_DATABASE)
    return _db_cache[pid]


def parse_spanner_json(raw_json):
    """Safely convert Cloud Spanner JsonObject or string into python object."""
    if raw_json is None:
        return None
    try:
        if isinstance(raw_json, str):
            return json.loads(raw_json)
        elif hasattr(raw_json, "serialize"):
            return json.loads(raw_json.serialize())
        elif isinstance(raw_json, (list, dict)):
            return raw_json
        else:
            return json.loads(json.dumps(raw_json, default=str))
    except Exception as e:
        print(f"Error parsing JSON object: {e}")
        return None


def search_entities_typeahead(term: str, limit: int = 15):
    """
    Search entities by legal name using Spanner SEARCH_SUBSTRING / SEARCH_NGRAMS or prefix matching.
    """
    clean_term = term.strip().lower()
    if not clean_term:
        return []

    # First try fast SEARCH_SUBSTRING on name_SubString
    query = """
    SELECT LEI, LegalName, EntityCategory, LegalJurisdiction, EntityStatus
    FROM Entities
    WHERE SEARCH_SUBSTRING(name_SubString, @term)
    LIMIT @limit
    """
    params = {"term": clean_term, "limit": limit}
    param_types = {
        "term": spanner.param_types.STRING,
        "limit": spanner.param_types.INT64,
    }

    results_list = []
    with get_database().snapshot() as snapshot:
        rows = snapshot.execute_sql(query, params=params, param_types=param_types)
        for row in rows:
            results_list.append({
                "id": row[0],
                "value": row[1] or row[0],
                "label": f"{row[1]} ({row[0]})" if row[1] else row[0],
                "lei": row[0],
                "name": row[1],
                "category": row[2] or "UNKNOWN",
                "jurisdiction": row[3] or "UNKNOWN",
                "status": row[4] or "UNKNOWN",
            })

    # If SEARCH_SUBSTRING returned empty, fallback to SEARCH_NGRAMS ranking
    if not results_list:
        query_ngram = """
        SELECT LEI, LegalName, EntityCategory, LegalJurisdiction, EntityStatus
        FROM Entities
        WHERE SEARCH_NGRAMS(name_Tokens, @term)
        ORDER BY SCORE_NGRAMS(name_Tokens, @term) DESC
        LIMIT @limit
        """
        with get_database().snapshot() as snapshot:
            rows = snapshot.execute_sql(query_ngram, params=params, param_types=param_types)
            for row in rows:
                results_list.append({
                    "id": row[0],
                    "value": row[1] or row[0],
                    "label": f"{row[1]} ({row[0]})" if row[1] else row[0],
                    "lei": row[0],
                    "name": row[1],
                    "category": row[2] or "UNKNOWN",
                    "jurisdiction": row[3] or "UNKNOWN",
                    "status": row[4] or "UNKNOWN",
                })

    return results_list


def query_3hop_graph(seed_lei: str, max_hops: int = 2, limit: int = 150):
    """
    Ultra-fast cycle-free BFS multi-hop graph query against LEIGraph/EntityRelationships.
    Splits edge lookups into clean primary key and secondary index seeks without JOIN overhead,
    then fetches all discovered node metadata in a single batched primary key seek (<0.4s).
    """
    max_hops = max(1, min(int(max_hops), 3))

    visited = {seed_lei}
    links = []
    seen_links = set()
    node_hops = {seed_lei: 0}

    frontier = [seed_lei]

    q_out = """
    SELECT LEI, EndLEI, RelationshipType, RelationshipStatus
    FROM EntityRelationships
    WHERE LEI IN UNNEST(@frontiers)
    LIMIT @limit
    """
    q_in = """
    SELECT LEI, EndLEI, RelationshipType, RelationshipStatus
    FROM EntityRelationships
    WHERE EndLEI IN UNNEST(@frontiers)
    LIMIT @limit
    """

    with get_database().snapshot(multi_use=True) as snapshot:
        for current_hop in range(1, max_hops + 1):
            if not frontier or len(node_hops) >= limit:
                break

            params = {"frontiers": frontier, "limit": limit}
            ptypes = {
                "frontiers": spanner.param_types.Array(spanner.param_types.STRING),
                "limit": spanner.param_types.INT64,
            }

            rows_out = list(snapshot.execute_sql(q_out, params=params, param_types=ptypes))
            rows_in = list(snapshot.execute_sql(q_in, params=params, param_types=ptypes))

            next_frontier = set()
            for s_id, t_id, rel_type, rel_st in rows_out + rows_in:
                if s_id in visited and t_id in visited:
                    continue

                link_key = (s_id, t_id, rel_type)
                if link_key not in seen_links:
                    seen_links.add(link_key)
                    links.append({
                        "source": s_id,
                        "target": t_id,
                        "type": rel_type or "IS_RELATED_TO",
                        "status": rel_st or "ACTIVE",
                    })

                if s_id not in node_hops:
                    node_hops[s_id] = current_hop
                    visited.add(s_id)
                    next_frontier.add(s_id)
                if t_id not in node_hops:
                    node_hops[t_id] = current_hop
                    visited.add(t_id)
                    next_frontier.add(t_id)

            frontier = list(next_frontier)

        # Batched metadata point-seek for all discovered entities (pre-filled with fallbacks)
        all_leis = list(node_hops.keys())
        nodes = {}
        for lei_id in all_leis:
            nodes[lei_id] = {
                "id": lei_id,
                "lei": lei_id,
                "name": lei_id,
                "category": "ENTITY",
                "jurisdiction": "N/A",
                "status": "ACTIVE",
                "regStatus": "N/A",
                "hop": node_hops.get(lei_id, 0),
                "isSeed": (lei_id == seed_lei),
            }

        if all_leis:
            q_nodes = """
            SELECT LEI, LegalName, EntityCategory, LegalJurisdiction, EntityStatus, RegistrationStatus
            FROM Entities
            WHERE LEI IN UNNEST(@leis)
            """
            node_rows = snapshot.execute_sql(
                q_nodes,
                params={"leis": all_leis},
                param_types={"leis": spanner.param_types.Array(spanner.param_types.STRING)},
            )
            for row in node_rows:
                lei_id = row[0]
                if lei_id in nodes:
                    nodes[lei_id].update({
                        "name": row[1] or lei_id,
                        "category": row[2] or "ENTITY",
                        "jurisdiction": row[3] or "N/A",
                        "status": row[4] or "ACTIVE",
                        "regStatus": row[5] or "N/A",
                    })

    return {
        "seed": nodes.get(seed_lei),
        "nodes": list(nodes.values()),
        "links": links,
        "stats": {
            "nodeCount": len(nodes),
            "linkCount": len(links),
            "maxHopsReached": max([n.get("hop", 0) for n in nodes.values()]) if nodes else 0,
        },
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/autocomplete")
def autocomplete():
    term = request.args.get("term", "").strip()
    if not term:
        return jsonify([])
    suggestions = search_entities_typeahead(term, limit=15)
    return jsonify(suggestions)


@app.route("/api/graph")
@app.route("/api/graph/3hop")
def graph_api():
    lei = request.args.get("lei", "").strip()
    # Default to 2 hops for optimal performance (3 hops can be query-intensive)
    hops = int(request.args.get("hops", 2))
    limit = int(request.args.get("limit", 150))
    if not lei:
        return jsonify({"error": "LEI parameter is required"}), 400

    graph_data = query_3hop_graph(lei, max_hops=hops, limit=limit)
    return jsonify(graph_data)


def query_community_graph(seed_lei: str, max_hops: int = 1, limit: int = 150):
    """
    Looks up LEI in EntityGraphAnalytics to find its precomputed CommunityId and PageRankScore,
    initializes EVERY member of that community as starting nodes, and performs a 1, 2, or 3 hop
    graph expansion outward across EntityRelationships.
    """
    max_hops = max(1, min(int(max_hops), 3))
    seed_lei = seed_lei.strip()

    with get_database().snapshot(multi_use=True) as snapshot:
        # 1. Inspect EntityGraphAnalytics for seed entity
        q_analytics = """
        SELECT a.LEI, a.PageRankScore, a.CommunityId, a.JaccardCommunityId,
               e.LegalName, e.EntityCategory, e.LegalJurisdiction, e.EntityStatus, e.RegistrationStatus
        FROM Entities e
        LEFT JOIN EntityGraphAnalytics a ON e.LEI = a.LEI
        WHERE e.LEI = @lei
        LIMIT 1
        """
        seed_rows = list(snapshot.execute_sql(
            q_analytics,
            params={"lei": seed_lei},
            param_types={"lei": spanner.param_types.STRING}
        ))

        if not seed_rows:
            return {
                "error": f"Entity {seed_lei} not found",
                "nodes": [],
                "links": [],
                "stats": {"nodeCount": 0, "linkCount": 0}
            }

        r = seed_rows[0]
        pr_score = r[1] or 0.0
        comm_id = r[2] if r[2] is not None else -1
        jaccard_id = r[3] if r[3] is not None else -1

        seed_node = {
            "id": seed_lei,
            "lei": seed_lei,
            "name": r[4] or seed_lei,
            "category": r[5] or "ENTITY",
            "jurisdiction": r[6] or "N/A",
            "status": r[7] or "ACTIVE",
            "regStatus": r[8] or "N/A",
            "hop": 0,
            "isSeed": True,
            "pageRankScore": pr_score,
            "communityId": comm_id,
        }

        # 2. Get overlapping members sharing the same CommunityId in EntityGraphAnalytics
        community_members = []
        member_leis = set()
        member_leis.add(seed_lei)
        nodes_dict = {}

        # Place seed entity in starting nodes
        nodes_dict[seed_lei] = seed_node

        if comm_id != -1:
            q_comm = """
            SELECT a.LEI, a.PageRankScore, e.LegalName, e.EntityCategory, e.LegalJurisdiction, e.EntityStatus, e.RegistrationStatus
            FROM EntityGraphAnalytics a
            JOIN Entities e ON a.LEI = e.LEI
            WHERE a.CommunityId = @comm_id
            ORDER BY a.PageRankScore DESC
            LIMIT @limit
            """
            comm_rows = list(snapshot.execute_sql(
                q_comm,
                params={"comm_id": comm_id, "limit": limit},
                param_types={
                    "comm_id": spanner.param_types.INT64,
                    "limit": spanner.param_types.INT64
                }
            ))
            for crow in comm_rows:
                c_lei = crow[0]
                member_leis.add(c_lei)
                community_members.append({
                    "lei": c_lei,
                    "name": crow[2] or c_lei,
                    "pageRankScore": crow[1] or 0.0,
                    "category": crow[3] or "ENTITY",
                    "jurisdiction": crow[4] or "N/A"
                })

                # EVERY member of the community is a starting node in the Community view!
                if c_lei not in nodes_dict:
                    nodes_dict[c_lei] = {
                        "id": c_lei,
                        "lei": c_lei,
                        "name": crow[2] or c_lei,
                        "category": crow[3] or "ENTITY",
                        "jurisdiction": crow[4] or "N/A",
                        "status": crow[5] or "ACTIVE",
                        "regStatus": crow[6] or "ISSUED",
                        "hop": 0,
                        "isSeed": (c_lei == seed_lei),
                        "isCommunityMember": True,
                        "pageRankScore": crow[1] or 0.0,
                        "communityId": comm_id
                    }
                else:
                    nodes_dict[c_lei]["isCommunityMember"] = True
                    nodes_dict[c_lei]["pageRankScore"] = crow[1] or 0.0
                    nodes_dict[c_lei]["communityId"] = comm_id

        # 3. BFS expansion up to 1, 2, or 3 hops outward starting from ALL community members!
        visited = set(nodes_dict.keys())
        frontier = list(visited)
        links = []
        seen_links = set()
        missing_leis = set()

        q_out = """
        SELECT LEI, EndLEI, RelationshipType, RelationshipStatus
        FROM EntityRelationships
        WHERE LEI IN UNNEST(@frontiers)
        LIMIT @limit
        """
        q_in = """
        SELECT LEI, EndLEI, RelationshipType, RelationshipStatus
        FROM EntityRelationships
        WHERE EndLEI IN UNNEST(@frontiers)
        LIMIT @limit
        """

        for current_hop in range(1, max_hops + 1):
            if not frontier or len(nodes_dict) >= limit:
                break

            params = {"frontiers": frontier, "limit": limit}
            ptypes = {
                "frontiers": spanner.param_types.Array(spanner.param_types.STRING),
                "limit": spanner.param_types.INT64
            }

            rows_out = list(snapshot.execute_sql(q_out, params=params, param_types=ptypes))
            rows_in = list(snapshot.execute_sql(q_in, params=params, param_types=ptypes))

            next_frontier = set()
            for src, tgt, rel_type, rel_st in rows_out + rows_in:
                rel_type = rel_type or "IS_RELATED_TO"
                rel_st = rel_st or "ACTIVE"
                pair_key = (src, tgt, rel_type)

                if pair_key not in seen_links:
                    seen_links.add(pair_key)
                    links.append({
                        "source": src,
                        "target": tgt,
                        "type": rel_type,
                        "status": rel_st
                    })

                for nid in (src, tgt):
                    if nid not in visited and len(nodes_dict) < limit:
                        visited.add(nid)
                        next_frontier.add(nid)
                        missing_leis.add(nid)
                        nodes_dict[nid] = {
                            "id": nid,
                            "lei": nid,
                            "name": nid,
                            "category": "ENTITY",
                            "jurisdiction": "N/A",
                            "status": "ACTIVE",
                            "regStatus": "ISSUED",
                            "hop": current_hop,
                            "isSeed": False,
                            "isCommunityMember": (nid in member_leis)
                        }

            frontier = list(next_frontier)

        # 4. Fetch entity metadata for external non-community neighbors discovered during expansion
        if missing_leis:
            q_meta = """
            SELECT LEI, LegalName, EntityCategory, LegalJurisdiction, EntityStatus, RegistrationStatus
            FROM Entities
            WHERE LEI IN UNNEST(@leis)
            """
            meta_rows = snapshot.execute_sql(
                q_meta,
                params={"leis": list(missing_leis)},
                param_types={"leis": spanner.param_types.Array(spanner.param_types.STRING)}
            )
            for mrow in meta_rows:
                m_id = mrow[0]
                if m_id in nodes_dict:
                    nodes_dict[m_id]["name"] = mrow[1] or m_id
                    nodes_dict[m_id]["category"] = mrow[2] or "ENTITY"
                    nodes_dict[m_id]["jurisdiction"] = mrow[3] or "N/A"
                    nodes_dict[m_id]["status"] = mrow[4] or "ACTIVE"
                    nodes_dict[m_id]["regStatus"] = mrow[5] or "ISSUED"

        return {
            "seed": seed_node,
            "community": {
                "communityId": comm_id,
                "jaccardCommunityId": jaccard_id,
                "pageRankScore": pr_score,
                "totalMembers": len(community_members),
                "members": community_members
            },
            "nodes": list(nodes_dict.values()),
            "links": links,
            "stats": {
                "nodeCount": len(nodes_dict),
                "linkCount": len(links),
                "maxHopsReached": max_hops,
                "communityMemberCount": len(community_members)
            }
        }


def query_pagerank_leaderboard(limit: int = 50, search: str = ""):
    """
    Queries EntityGraphAnalytics joined with Entities ordered by precomputed PageRankScore DESC.
    """
    limit = max(5, min(int(limit), 100))
    search = search.strip()

    with get_database().snapshot(multi_use=True) as snapshot:
        # Get overall community analytics summary stats
        q_stats = """
        SELECT COUNT(a.LEI) as total_ranked,
               COUNT(DISTINCT a.CommunityId) as total_communities,
               MAX(a.PageRankScore) as max_pagerank
        FROM EntityGraphAnalytics a
        """
        stats_row = list(snapshot.execute_sql(q_stats))[0]

        if search:
            q_leaderboard = """
            SELECT a.LEI, a.PageRankScore, a.CommunityId, a.JaccardCommunityId,
                   e.LegalName, e.EntityCategory, e.LegalJurisdiction, e.EntityStatus
            FROM EntityGraphAnalytics a
            JOIN Entities e ON a.LEI = e.LEI
            WHERE UPPER(e.LegalName) LIKE @term OR UPPER(a.LEI) LIKE @term
            ORDER BY a.PageRankScore DESC
            LIMIT @limit
            """
            params = {"term": f"%{search.upper()}%", "limit": limit}
            ptypes = {"term": spanner.param_types.STRING, "limit": spanner.param_types.INT64}
        else:
            q_leaderboard = """
            SELECT a.LEI, a.PageRankScore, a.CommunityId, a.JaccardCommunityId,
                   e.LegalName, e.EntityCategory, e.LegalJurisdiction, e.EntityStatus
            FROM EntityGraphAnalytics a
            JOIN Entities e ON a.LEI = e.LEI
            ORDER BY a.PageRankScore DESC
            LIMIT @limit
            """
            params = {"limit": limit}
            ptypes = {"limit": spanner.param_types.INT64}

        rows = snapshot.execute_sql(q_leaderboard, params=params, param_types=ptypes)
        leaderboard = []
        for idx, row in enumerate(rows, 1):
            leaderboard.append({
                "rank": idx,
                "lei": row[0],
                "pageRankScore": row[1] or 0.0,
                "communityId": row[2] if row[2] is not None else -1,
                "jaccardCommunityId": row[3] if row[3] is not None else -1,
                "name": row[4] or row[0],
                "category": row[5] or "ENTITY",
                "jurisdiction": row[6] or "N/A",
                "status": row[7] or "ACTIVE",
            })

        return {
            "summary": {
                "totalRankedEntities": stats_row[0] or 0,
                "totalCommunities": stats_row[1] or 0,
                "maxPageRankScore": stats_row[2] or 0.0
            },
            "leaderboard": leaderboard
        }


@app.route("/api/community")
def community_api():
    lei = request.args.get("lei", "").strip()
    hops = int(request.args.get("hops", 1))  # Default to 1 hop per user specification
    limit = int(request.args.get("limit", 150))
    if not lei:
        return jsonify({"error": "LEI parameter is required"}), 400

    community_data = query_community_graph(lei, max_hops=hops, limit=limit)
    return jsonify(community_data)


@app.route("/api/pagerank")
def pagerank_api():
    limit = int(request.args.get("limit", 50))
    search = request.args.get("search", "").strip()
    pagerank_data = query_pagerank_leaderboard(limit=limit, search=search)
    return jsonify(pagerank_data)


@app.route("/v1/health")
def health_check():
    try:
        with get_database().snapshot() as snapshot:
            results = snapshot.execute_sql("SELECT 1")
            list(results)
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True, use_reloader=False)
