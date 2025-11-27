set client_min_messages = notice;

\set max_distance 70000
\set separation 5000
\set mast_height 28
\set frequency 868e6

-- Collect every tower or placeable cell so we know which H3s can participate in routing later.
drop table if exists mesh_route_candidate_cells;
create table mesh_route_candidate_cells as
select
    s.h3,
    s.centroid_geog
from mesh_surface_h3_r8 s
where s.has_tower
   or s.can_place_tower;

-- mesh_route_bridge consumes these nodes, so keep a compact primary key for joins.
alter table mesh_route_candidate_cells
    add primary key (h3);

-- Spatial index accelerates the neighbor pairing step below.
create index if not exists mesh_route_candidate_cells_geog_idx
    on mesh_route_candidate_cells
    using gist (centroid_geog);

-- Precompute how close every candidate sits to an existing blocked visibility edge so reruns can clear the worst blind spots first.
drop table if exists mesh_route_candidate_invisible_dist;
create table mesh_route_candidate_invisible_dist as
select
    c.h3,
    coalesce(
        (
            select ST_Distance(c.centroid_geog, e.geom::geography)
            from mesh_visibility_edges e
            where not e.is_visible
              and e.is_between_clusters
            order by c.centroid_geog::geometry <-> e.geom
            limit 1
        ),
        :max_distance
    ) as distance_m
from mesh_route_candidate_cells c;

alter table mesh_route_candidate_invisible_dist
    add primary key (h3);

create index if not exists mesh_route_candidate_invisible_dist_distance_brin
    on mesh_route_candidate_invisible_dist
    using brin (distance_m);

-- Generate all LOS-eligible candidate pairs (>=5 km, <=70 km) that might need cache entries.
drop table if exists mesh_route_pair_candidates;
create table mesh_route_pair_candidates as
select
    c1.h3 as src_h3,
    c2.h3 as dst_h3,
    ST_Distance(c1.centroid_geog, c2.centroid_geog) as distance_m
from mesh_route_candidate_cells c1
join mesh_route_candidate_cells c2
  on c2.h3 > c1.h3
 and ST_DWithin(c1.centroid_geog, c2.centroid_geog, :max_distance)
and not ST_DWithin(c1.centroid_geog, c2.centroid_geog, :separation);

-- Later cache lookups join on src/dst, so add a btree index.
create index if not exists mesh_route_pair_candidates_src_dst_idx
    on mesh_route_pair_candidates (src_h3, dst_h3);

-- Identify which candidate pairs still lack LOS/cache metrics and persist a priority distance to the nearest blocked visibility edge.
drop table if exists mesh_route_missing_pairs;
create table mesh_route_missing_pairs as
select
    pr.src_h3,
    pr.dst_h3,
    src_priority.distance_m + dst_priority.distance_m as priority
from mesh_route_pair_candidates pr
left join mesh_los_cache mlc
    on mlc.src_h3 = pr.src_h3
   and mlc.dst_h3 = pr.dst_h3
   and mlc.mast_height_src = :mast_height
   and mlc.mast_height_dst = :mast_height
   and mlc.frequency_hz = :frequency
join mesh_route_candidate_invisible_dist src_priority on src_priority.h3 = pr.src_h3
join mesh_route_candidate_invisible_dist dst_priority on dst_priority.h3 = pr.dst_h3
where mlc.src_h3 is null;

-- Report cache coverage so the operator knows whether this stage still has work to do.
with stats as (
    select
        (select count(*) from mesh_route_missing_pairs) as missing_pairs,
        (select count(*) from mesh_route_pair_candidates) as total_pairs,
        (select min(priority) from mesh_route_missing_pairs) as min_priority_m
)
select
    missing_pairs,
    total_pairs,
    case
        when total_pairs = 0 then 100::numeric
        else round(((total_pairs - missing_pairs)::numeric / total_pairs) * 100, 2)
    end as completion_pct
    ,
    min_priority_m
from stats;


create index on mesh_route_missing_pairs using brin (src_h3, dst_h3);
create index on mesh_route_missing_pairs using brin (priority);


----------------------------------
---- LOOP here

-- Compute clearance/path loss for every missing pair so mesh_route_bridge can reuse the cache later.
drop table if exists mesh_route_missing_metrics;
create table mesh_route_missing_metrics as
with prioritized_pairs as (
    -- Take the closest million missing pairs so repeated runs burn down the most urgent blind spots first.
    select
        mp.src_h3,
        mp.dst_h3,
        mp.priority
    from mesh_route_missing_pairs mp
    order by mp.priority, mp.src_h3, mp.dst_h3
    limit 20000000
)
select
    mp.src_h3,
    mp.dst_h3,
    metrics.clearance,
    metrics.path_loss_db,
    metrics.distance_m,
    metrics.d1_m,
    metrics.d2_m
from prioritized_pairs mp
cross join lateral h3_visibility_clearance_compute(
    mp.src_h3,
    mp.dst_h3,
    :mast_height,
    :mast_height,
    :frequency
) as metrics(clearance, path_loss_db, distance_m, d1_m, d2_m);

-- Persist the new LOS metrics so repeated runs of mesh_route_bridge stay fast.
insert into mesh_los_cache (
    src_h3,
    dst_h3,
    mast_height_src,
    mast_height_dst,
    frequency_hz,
    distance_m,
    clearance,
    d1_m,
    d2_m,
    path_loss_db,
    computed_at
)
select
    rmm.src_h3,
    rmm.dst_h3,
    :mast_height,
    :mast_height,
    :frequency,
    rmm.distance_m,
    rmm.clearance,
    rmm.d1_m,
    rmm.d2_m,
    rmm.path_loss_db,
    now()
from mesh_route_missing_metrics rmm
on conflict on constraint mesh_los_cache_pkey do update
    set clearance = excluded.clearance,
        path_loss_db = excluded.path_loss_db,
        d1_m = excluded.d1_m,
        d2_m = excluded.d2_m,
        distance_m = excluded.distance_m,
        computed_at = now();

delete from mesh_route_missing_pairs mp using mesh_route_missing_metrics mm where mm.src_h3 = mp.src_h3 and mm.dst_h3 = mp.dst_h3;

-- Report cache coverage so the operator knows whether this stage still has work to do.
with stats as (
    select
        (select count(*) from mesh_route_missing_pairs) as missing_pairs,
        (select count(*) from mesh_route_pair_candidates) as total_pairs,
        (select min(priority) from mesh_route_missing_pairs) as min_priority_m
)
select
    missing_pairs,
    total_pairs,
    case
        when total_pairs = 0 then 100::numeric
        else round(((total_pairs - missing_pairs)::numeric / total_pairs) * 100, 2)
    end as completion_pct
    ,
    min_priority_m
from stats;



------- END LOOP
        
        
-- Cleanup staging tables now that their contents are in mesh_los_cache.
drop table if exists mesh_route_missing_metrics;
drop table if exists mesh_route_missing_pairs;
drop table if exists mesh_route_candidate_invisible_dist;


-- Assign deterministic numeric ids to each candidate node; pgRouting expects integer node ids.
drop table if exists mesh_route_nodes;
create table mesh_route_nodes as
select
    row_number() over (order by h3) as node_id,
    h3
from mesh_route_candidate_cells;

alter table mesh_route_nodes
    add primary key (node_id);

create unique index if not exists mesh_route_nodes_h3_idx
    on mesh_route_nodes (h3);

-- Build the pgRouting edge list with directional costs so mesh_route_bridge can run shortest paths.
drop table if exists mesh_route_edges;
create table mesh_route_edges as
select
    row_number() over () as edge_id,
    src.node_id as source,
    dst.node_id as target,
    mlc.path_loss_db as cost,
    mlc.path_loss_db as reverse_cost
from mesh_los_cache mlc
join mesh_route_nodes src on src.h3 = mlc.src_h3
join mesh_route_nodes dst on dst.h3 = mlc.dst_h3
where mlc.mast_height_src = :mast_height
  and mlc.mast_height_dst = :mast_height
  and mlc.frequency_hz = :frequency
  and mlc.clearance > 0
  and mlc.distance_m <= :max_distance
  and mlc.distance_m >= :separation;

alter table mesh_route_edges
    add primary key (edge_id);

create index if not exists mesh_route_edges_source_idx on mesh_route_edges (source);
create index if not exists mesh_route_edges_target_idx on mesh_route_edges (target);

-- Drop intermediate inputs so only the persistent nodes/edges remain for later use.
drop index if exists mesh_route_candidate_cells_geog_idx;
drop table if exists mesh_route_candidate_cells;
drop table if exists mesh_route_pair_candidates;

vacuum analyze mesh_los_cache;
