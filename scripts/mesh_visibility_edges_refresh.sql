set client_min_messages = warning;

truncate mesh_visibility_edges;

drop table if exists tmp_visibility_missing_elevation;
-- Temporary table capturing towers lacking GEBCO elevation coverage
create temporary table tmp_visibility_missing_elevation (
    tower_id integer primary key,
    h3 h3index not null
) on commit preserve rows;

insert into tmp_visibility_missing_elevation (tower_id, h3)
select
    t.tower_id,
    t.h3
from mesh_towers t
left join gebco_elevation_h3_r8 ge
  on ge.h3 = t.h3
where ge.h3 is null;

with eligible_towers as (
    -- Filter towers that have elevation samples so LOS math stays reliable.
    select t.*
    from mesh_towers t
    where not exists (
        select 1
        from tmp_visibility_missing_elevation missing
        where missing.h3 = t.h3
    )
),
tower_clusters as (
    -- Label towers by cluster id so we can flag inter-cluster LOS edges.
    select *
    from mesh_tower_clusters()
),
edge_pairs as (
    -- Build all tower-to-tower distances plus LOS flag and straight-line geometry.
    select
        t1.tower_id as source_id,
        t2.tower_id as target_id,
        t1.h3 as source_h3,
        t2.h3 as target_h3,
        ST_Distance(t1.centroid_geog, t2.centroid_geog) as distance_m,
        h3_los_between_cells(t1.h3, t2.h3) as is_visible,
        (src.cluster_id <> dst.cluster_id) as is_between_clusters,
        ST_MakeLine(t1.centroid_geog::geometry, t2.centroid_geog::geometry) as geom
    from eligible_towers t1
    join eligible_towers t2
      on t1.tower_id < t2.tower_id
    join tower_clusters src on src.tower_id = t1.tower_id
    join tower_clusters dst on dst.tower_id = t2.tower_id
)
insert into mesh_visibility_edges (
    source_id,
    target_id,
    source_h3,
    target_h3,
    distance_m,
    is_visible,
    is_between_clusters,
    geom
)
select
    source_id,
    target_id,
    source_h3,
    target_h3,
    distance_m,
    is_visible,
    is_between_clusters,
    geom
from edge_pairs;

with tower_clusters as (
    -- Cache cluster ids for every tower to detect when an edge bridges disconnected components.
    select *
    from mesh_tower_clusters()
),
invisible_cluster_edges as (
    -- Generate pgRouting fallback geometries for invisible edges that join separate clusters.
    select
        e.source_id,
        e.target_id,
        mesh_visibility_invisible_route_geom(e.source_h3, e.target_h3) as routed_geom
    from mesh_visibility_edges e
    join tower_clusters src on src.tower_id = e.source_id
    join tower_clusters dst on dst.tower_id = e.target_id
    where not e.is_visible
      and src.cluster_id <> dst.cluster_id
)
-- Apply the routed geometries wherever a path was successfully generated.
update mesh_visibility_edges e
set geom = ice.routed_geom
from invisible_cluster_edges ice
where e.source_id = ice.source_id
  and e.target_id = ice.target_id
  and ice.routed_geom is not null;

do
$$
declare
    missing_list text;
    missing_count integer;
begin
    select
        string_agg(h3::text, ', ' order by h3),
        count(*)
    into missing_list, missing_count
    from tmp_visibility_missing_elevation;

    if missing_count > 0 then
        raise warning 'Skipped % tower(s) without GEBCO elevation samples: %',
            missing_count,
            missing_list;
    end if;
end;
$$;

drop table if exists tmp_visibility_missing_elevation;
