set client_min_messages = warning;

-- Rebuild mesh_visibility_edges along with visibility diagnostics and hop counts.
create or replace procedure mesh_visibility_edges_refresh()
    language plpgsql
as
$$
declare
    missing_list text;
    missing_count integer;
begin
    truncate mesh_visibility_edges;

if to_regclass('tmp_visibility_missing_elevation') is not null then
    execute 'drop table tmp_visibility_missing_elevation';
end if;
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

if to_regclass('tmp_visibility_cluster_edges') is not null then
    execute 'drop table tmp_visibility_cluster_edges';
end if;
    -- Temporary graph holding LOS-adjacent tower pairs (<=70 km) so pgRouting can recover hop counts.
    create temporary table tmp_visibility_cluster_edges (
        edge_id bigserial primary key,
        source_id integer not null,
        target_id integer not null,
        cost double precision not null default 1
    )
    on commit preserve rows;

    insert into tmp_visibility_cluster_edges (source_id, target_id)
    select
        e.source_id,
        e.target_id
    from mesh_visibility_edges e
    where e.is_visible
      and e.distance_m <= 70000;

    with vertex_ids as (
        -- Limit pgRouting sources/targets to towers that participate in any LOS edge.
        select vid
        from (
            select source_id as vid from tmp_visibility_cluster_edges
            union
            select target_id as vid from tmp_visibility_cluster_edges
        ) v
    ),
    cluster_hops as (
        -- Measure minimum hop count between every reachable tower pair inside a cluster.
        select
            least(result.start_vid, result.end_vid) as source_id,
            greatest(result.start_vid, result.end_vid) as target_id,
            result.agg_cost::integer as hops
        from pgr_dijkstra(
            'select edge_id as id, source_id as source, target_id as target, cost, cost as reverse_cost from tmp_visibility_cluster_edges',
            coalesce(array(select vid::bigint from vertex_ids order by vid), array[]::bigint[]),
            coalesce(array(select vid::bigint from vertex_ids order by vid), array[]::bigint[]),
            false
        ) as result
        where result.start_vid < result.end_vid
          and result.agg_cost < 'Infinity'::double precision
          and result.node = result.end_vid
    )
    update mesh_visibility_edges e
    set cluster_hops = ch.hops
    from cluster_hops ch
    where e.source_id = ch.source_id
      and e.target_id = ch.target_id;

if to_regclass('tmp_visibility_cluster_edges') is not null then
    execute 'drop table tmp_visibility_cluster_edges';
end if;

    with tower_clusters as (
        -- Cache cluster ids for every tower to detect when an edge bridges disconnected components.
        select *
        from mesh_tower_clusters()
    ),
    edges_requiring_routes as (
        -- Generate pgRouting fallback geometries for inter-cluster invisible edges and any intra-cluster pair that spans 8+ hops.
        select
            e.source_id,
            e.target_id,
            mesh_visibility_invisible_route_geom(e.source_h3, e.target_h3) as routed_geom
        from mesh_visibility_edges e
        join tower_clusters src on src.tower_id = e.source_id
        join tower_clusters dst on dst.tower_id = e.target_id
        where (not e.is_visible and src.cluster_id <> dst.cluster_id)
           or (e.cluster_hops is not null and e.cluster_hops >= 8)
    )
    update mesh_visibility_edges e
    set geom = err.routed_geom
    from edges_requiring_routes err
    where e.source_id = err.source_id
      and e.target_id = err.target_id
      and err.routed_geom is not null;

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

if to_regclass('tmp_visibility_missing_elevation') is not null then
    execute 'drop table tmp_visibility_missing_elevation';
end if;
end;
$$;
