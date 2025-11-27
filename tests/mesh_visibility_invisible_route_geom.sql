set client_min_messages = warning;

begin;

-- Shadow mesh_surface_h3_r8 with a lightweight temporary version for deterministic routing tests plus routing hints.
drop table if exists pg_temp.mesh_surface_h3_r8;
create temporary table mesh_surface_h3_r8 (
    h3 h3index primary key,
    ele double precision,
    centroid_geog geography(Point, 4326),
    population numeric,
    has_road boolean,
    is_in_boundaries boolean,
    is_in_unfit_area boolean default false
) on commit drop;

-- Shadow the precomputed routing graph tables so tests can rebuild them per scenario.
drop table if exists pg_temp.mesh_route_graph_edges;
drop table if exists pg_temp.mesh_route_graph_nodes;
drop table if exists pg_temp.mesh_route_graph_cache;
create temporary table mesh_route_graph_nodes (
    node_id integer generated always as identity primary key,
    h3 h3index not null unique,
    geom geometry not null,
    geog public.geography not null
) on commit drop;

create temporary table mesh_route_graph_edges (
    edge_id bigint generated always as identity primary key,
    source_node_id integer not null,
    target_node_id integer not null,
    cost double precision not null
) on commit drop;

create temporary table mesh_route_graph_cache (
    source_h3 h3index not null,
    target_h3 h3index not null,
    geom geometry(LineString, 4326) not null,
    created_at timestamptz default now(),
    primary key (source_h3, target_h3)
) on commit drop;

do
$$
declare
    src_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    dst_h3 h3index;
    blocked_h3 h3index;
    detour_h3 h3index;
    dst_candidate record;
    path_geom geometry;
    vertex_count integer;
    uses_detour boolean;
    hits_blocked boolean;
    cached_geom geometry;
begin
    -- Reset stub surface table so only this scenario's cells participate.
    truncate mesh_surface_h3_r8;

    -- Search for a destination that shares an alternate neighbor with the source.
    for dst_candidate in
        select h3
        from h3_grid_disk(src_h3, 2) as candidate(h3)
    loop
        exit when dst_h3 is not null;

        if dst_candidate.h3 = src_h3 then
            continue;
        end if;

        -- Skip neighbors directly adjacent to the source since they do not need detours.
        if exists (
            select 1
            from h3_grid_disk(src_h3, 1) as immediate(h3)
            where immediate.h3 = dst_candidate.h3
        ) then
            continue;
        end if;

        select h3
        into blocked_h3
        from h3_grid_path_cells(src_h3, dst_candidate.h3) with ordinality as path(h3, ordinality)
        where path.ordinality = 2
        limit 1;

        if blocked_h3 is null then
            continue;
        end if;

        if blocked_h3 = dst_candidate.h3 then
            continue;
        end if;

        select sn.h3
        into detour_h3
        from h3_grid_disk(src_h3, 1) as sn(h3)
        join h3_grid_disk(dst_candidate.h3, 1) as dn(h3)
            on dn.h3 = sn.h3
        where sn.h3 not in (src_h3, dst_candidate.h3, blocked_h3)
        limit 1;

        if detour_h3 is not null then
            dst_h3 := dst_candidate.h3;
        end if;
    end loop;

    if dst_h3 is null or detour_h3 is null then
        raise exception 'Failed to find detour-ready destination for source %', src_h3::text;
    end if;

    insert into mesh_surface_h3_r8 (
        h3,
        ele,
        centroid_geog,
        population,
        has_road,
        is_in_boundaries,
        is_in_unfit_area
    )
    select *
    from (
        values
            (src_h3, 0::double precision, src_h3::geography, 10::numeric, true, true, false),
            (dst_h3, 0::double precision, dst_h3::geography, 10::numeric, true, true, false),
            (blocked_h3, 0::double precision, blocked_h3::geography, 10::numeric, true, false, false),
            (detour_h3, 0::double precision, detour_h3::geography, 10::numeric, true, true, false)
    ) as payload(h3, ele, centroid_geog, population, has_road, is_in_boundaries, is_in_unfit_area);

    -- Rebuild the cached routing graph for this synthetic setup.
    truncate mesh_route_graph_cache;
    truncate mesh_route_graph_edges;
    truncate mesh_route_graph_nodes restart identity;

    insert into mesh_route_graph_nodes (h3, geom, geog)
    select
        h3,
        centroid_geog::geometry,
        centroid_geog
    from mesh_surface_h3_r8
    where is_in_boundaries
      and not is_in_unfit_area;

    insert into mesh_route_graph_edges (source_node_id, target_node_id, cost)
    select
        src.node_id,
        dst.node_id,
        1
        + case when coalesce(target_surface.population, 0) = 0 then 1 else 0 end
        + case when not coalesce(target_surface.has_road, false) then 1 else 0 end
        + case
            when source_surface.ele is not null
             and target_surface.ele is not null
             and source_surface.ele > target_surface.ele
            then 1
            else 0
          end
    from mesh_route_graph_nodes src
    join lateral (
        select h3_grid_disk(src.h3, 1) as neighbor
    ) n on true
    join mesh_route_graph_nodes dst on dst.h3 = n.neighbor
    join mesh_surface_h3_r8 target_surface on target_surface.h3 = dst.h3
    join mesh_surface_h3_r8 source_surface on source_surface.h3 = src.h3
    where dst.h3 <> src.h3;

    select mesh_visibility_invisible_route_geom(src_h3, dst_h3)
    into path_geom;

    if path_geom is null then
        raise exception 'mesh_visibility_invisible_route_geom(%, %) returned NULL path for synthetic detour', src_h3::text, dst_h3::text;
    end if;

    if not exists (
        select 1
        from mesh_route_graph_cache
        where source_h3 = (case when src_h3::text <= dst_h3::text then src_h3 else dst_h3 end)
          and target_h3 = (case when src_h3::text <= dst_h3::text then dst_h3 else src_h3 end)
    ) then
        raise exception 'Expected cache entry for %, % after first routing call', src_h3::text, dst_h3::text;
    end if;

    -- Force the function to rely on the cache by removing edges and rerunning the request.
    truncate mesh_route_graph_edges;

    select mesh_visibility_invisible_route_geom(src_h3, dst_h3)
    into cached_geom;

    if cached_geom is null then
        raise exception 'Routing cache failed to serve %, % after edges disappeared', src_h3::text, dst_h3::text;
    end if;

    if not ST_Equals(path_geom, cached_geom) then
        raise exception 'Cached geometry for %, % differs from original path', src_h3::text, dst_h3::text;
    end if;

    with dumped as (
        -- Convert each vertex back into an H3 cell to inspect which grid cells participate.
        select
            h3_latlng_to_cell((dp.geom)::geography, 8) as h3
        from ST_DumpPoints(path_geom) dp
    )
    select
        count(*) as vertex_total,
        bool_or(h3 = detour_h3) as hits_detour,
        bool_or(h3 = blocked_h3) as hits_blocked
    into vertex_count, uses_detour, hits_blocked
    from dumped;

    if vertex_count < 3 then
        raise exception 'Expected detoured path with >=3 vertices between % and %, got %', src_h3::text, dst_h3::text, vertex_count;
    end if;

    if not uses_detour then
        raise exception 'Routed path for % -> % should include detour cell % to prove adjacency routing works',
            src_h3::text,
            dst_h3::text,
            detour_h3::text;
    end if;

    if hits_blocked then
        raise exception 'Routed path for % -> % should avoid non-admin cell % but it was included',
            src_h3::text,
            dst_h3::text,
            blocked_h3::text;
    end if;
end;
$$;

do
$$
declare
    src_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    dst_h3 h3index;
    preferred_h3 h3index;
    penalized_h3 h3index;
    dst_candidate record;
    shared_neighbors h3index[];
    path_geom geometry;
    vertex_count integer;
    uses_preferred boolean;
    hits_penalized boolean;
begin
    -- Reset stub surface table for the population/road penalty scenario.
    truncate mesh_surface_h3_r8;

    -- Search for a destination with at least two shared neighbors so routing can choose between them.
    for dst_candidate in
        select h3
        from h3_grid_disk(src_h3, 2) as candidate(h3)
    loop
        if dst_candidate.h3 = src_h3 then
            continue;
        end if;

        -- Avoid direct neighbors since we need alternate shared cells to compare penalties.
        if exists (
            select 1
            from h3_grid_disk(src_h3, 1) as immediate(h3)
            where immediate.h3 = dst_candidate.h3
        ) then
            continue;
        end if;

        select array_agg(common.h3 order by common.h3)
        into shared_neighbors
        from (
            select sn.h3
            from h3_grid_disk(src_h3, 1) as sn(h3)
            join h3_grid_disk(dst_candidate.h3, 1) as dn(h3)
                on dn.h3 = sn.h3
            where sn.h3 not in (src_h3, dst_candidate.h3)
        ) as common;

        if shared_neighbors is not null and cardinality(shared_neighbors) >= 2 then
            dst_h3 := dst_candidate.h3;
            preferred_h3 := shared_neighbors[1];
            penalized_h3 := shared_neighbors[2];
            exit;
        end if;
    end loop;

    if dst_h3 is null or preferred_h3 is null or penalized_h3 is null then
        raise exception 'Failed to find detour pair with dual shared neighbors for source %', src_h3::text;
    end if;

    insert into mesh_surface_h3_r8 (
        h3,
        ele,
        centroid_geog,
        population,
        has_road,
        is_in_boundaries,
        is_in_unfit_area
    )
    select *
    from (
        values
            (src_h3, 0::double precision, src_h3::geography, 10::numeric, true, true, false),
            (dst_h3, 0::double precision, dst_h3::geography, 10::numeric, true, true, false),
            (preferred_h3, 0::double precision, preferred_h3::geography, 50::numeric, true, true, false),
            (penalized_h3, 0::double precision, penalized_h3::geography, 0::numeric, false, true, false)
    ) as payload(h3, ele, centroid_geog, population, has_road, is_in_boundaries, is_in_unfit_area);

    -- Rebuild the cached routing graph for the penalty scenario.
    truncate mesh_route_graph_cache;
    truncate mesh_route_graph_edges;
    truncate mesh_route_graph_nodes restart identity;

    insert into mesh_route_graph_nodes (h3, geom, geog)
    select
        h3,
        centroid_geog::geometry,
        centroid_geog
    from mesh_surface_h3_r8
    where is_in_boundaries
      and not is_in_unfit_area;

    insert into mesh_route_graph_edges (source_node_id, target_node_id, cost)
    select
        src.node_id,
        dst.node_id,
        1
        + case when coalesce(target_surface.population, 0) = 0 then 1 else 0 end
        + case when not coalesce(target_surface.has_road, false) then 1 else 0 end
        + case
            when source_surface.ele is not null
             and target_surface.ele is not null
             and source_surface.ele > target_surface.ele
            then 1
            else 0
          end
    from mesh_route_graph_nodes src
    join lateral (
        select h3_grid_disk(src.h3, 1) as neighbor
    ) n on true
    join mesh_route_graph_nodes dst on dst.h3 = n.neighbor
    join mesh_surface_h3_r8 target_surface on target_surface.h3 = dst.h3
    join mesh_surface_h3_r8 source_surface on source_surface.h3 = src.h3
    where dst.h3 <> src.h3;

    select mesh_visibility_invisible_route_geom(src_h3, dst_h3)
    into path_geom;

    if path_geom is null then
        raise exception 'mesh_visibility_invisible_route_geom(%, %) returned NULL when testing population/road penalties', src_h3::text, dst_h3::text;
    end if;

    with dumped as (
        -- Convert each vertex back into an H3 cell for adjacency inspection.
        select
            h3_latlng_to_cell((dp.geom)::geography, 8) as h3
        from ST_DumpPoints(path_geom) dp
    )
    select
        count(*) as vertex_total,
        bool_or(h3 = preferred_h3) as includes_preferred,
        bool_or(h3 = penalized_h3) as includes_penalized
    into vertex_count, uses_preferred, hits_penalized
    from dumped;

    if vertex_count < 3 then
        raise exception 'Expected at least 3 vertices for penalty routing test between % and %, got %', src_h3::text, dst_h3::text, vertex_count;
    end if;

    if not uses_preferred then
        raise exception 'Routing between % and % should prioritize populated road cell %, but it was skipped',
            src_h3::text,
            dst_h3::text,
            preferred_h3::text;
    end if;

    if hits_penalized then
        raise exception 'Routing between % and % should avoid unpopulated roadless cell %, yet it appeared in the path',
            src_h3::text,
            dst_h3::text,
            penalized_h3::text;
    end if;
end;
$$;

do
$$
declare
    src_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    dst_h3 h3index;
    preferred_h3 h3index;
    penalized_h3 h3index;
    dst_candidate record;
    shared_neighbors h3index[];
    path_geom geometry;
    vertex_count integer;
    uses_preferred boolean;
    hits_penalized boolean;
begin
    -- Reset stub surface table for the downhill penalty scenario.
    truncate mesh_surface_h3_r8;

    -- Search for a destination with at least two shared neighbors so routing can choose between them.
    for dst_candidate in
        select h3
        from h3_grid_disk(src_h3, 2) as candidate(h3)
    loop
        if dst_candidate.h3 = src_h3 then
            continue;
        end if;

        -- Avoid direct neighbors since we need alternate shared cells to compare penalties.
        if exists (
            select 1
            from h3_grid_disk(src_h3, 1) as immediate(h3)
            where immediate.h3 = dst_candidate.h3
        ) then
            continue;
        end if;

        select array_agg(common.h3 order by common.h3)
        into shared_neighbors
        from (
            select sn.h3
            from h3_grid_disk(src_h3, 1) as sn(h3)
            join h3_grid_disk(dst_candidate.h3, 1) as dn(h3)
                on dn.h3 = sn.h3
            where sn.h3 not in (src_h3, dst_candidate.h3)
        ) as common;

        if shared_neighbors is not null and cardinality(shared_neighbors) >= 2 then
            dst_h3 := dst_candidate.h3;
            preferred_h3 := shared_neighbors[1];
            penalized_h3 := shared_neighbors[2];
            exit;
        end if;
    end loop;

    if dst_h3 is null or preferred_h3 is null or penalized_h3 is null then
        raise exception 'Failed to find detour pair with dual shared neighbors for downhill penalty test (source %)', src_h3::text;
    end if;

    insert into mesh_surface_h3_r8 (
        h3,
        ele,
        centroid_geog,
        population,
        has_road,
        is_in_boundaries,
        is_in_unfit_area
    )
    select *
    from (
        values
            (src_h3, 10::double precision, src_h3::geography, 10::numeric, true, true, false),
            (dst_h3, 30::double precision, dst_h3::geography, 10::numeric, true, true, false),
            (preferred_h3, 20::double precision, preferred_h3::geography, 10::numeric, true, true, false),
            (penalized_h3, 0::double precision, penalized_h3::geography, 10::numeric, true, true, false)
    ) as payload(h3, ele, centroid_geog, population, has_road, is_in_boundaries, is_in_unfit_area);

    -- Rebuild the cached routing graph for the downhill penalty scenario.
    truncate mesh_route_graph_cache;
    truncate mesh_route_graph_edges;
    truncate mesh_route_graph_nodes restart identity;

    insert into mesh_route_graph_nodes (h3, geom, geog)
    select
        h3,
        centroid_geog::geometry,
        centroid_geog
    from mesh_surface_h3_r8
    where is_in_boundaries
      and not is_in_unfit_area;

    insert into mesh_route_graph_edges (source_node_id, target_node_id, cost)
    select
        src.node_id,
        dst.node_id,
        1
        + case when coalesce(target_surface.population, 0) = 0 then 1 else 0 end
        + case when not coalesce(target_surface.has_road, false) then 1 else 0 end
        + case
            when source_surface.ele is not null
             and target_surface.ele is not null
             and source_surface.ele > target_surface.ele
            then 1
            else 0
          end
    from mesh_route_graph_nodes src
    join lateral (
        select h3_grid_disk(src.h3, 1) as neighbor
    ) n on true
    join mesh_route_graph_nodes dst on dst.h3 = n.neighbor
    join mesh_surface_h3_r8 target_surface on target_surface.h3 = dst.h3
    join mesh_surface_h3_r8 source_surface on source_surface.h3 = src.h3
    where dst.h3 <> src.h3;

    select mesh_visibility_invisible_route_geom(src_h3, dst_h3)
    into path_geom;

    if path_geom is null then
        raise exception 'mesh_visibility_invisible_route_geom(%, %) returned NULL when testing downhill penalties', src_h3::text, dst_h3::text;
    end if;

    with dumped as (
        -- Convert each vertex back into an H3 cell for adjacency inspection.
        select
            h3_latlng_to_cell((dp.geom)::geography, 8) as h3
        from ST_DumpPoints(path_geom) dp
    )
    select
        count(*) as vertex_total,
        bool_or(h3 = preferred_h3) as includes_preferred,
        bool_or(h3 = penalized_h3) as includes_penalized
    into vertex_count, uses_preferred, hits_penalized
    from dumped;

    if vertex_count < 3 then
        raise exception 'Expected at least 3 vertices for downhill routing test between % and %, got %', src_h3::text, dst_h3::text, vertex_count;
    end if;

    if not uses_preferred then
        raise exception 'Routing between % and % should favor the higher-elevation neighbor %, but it was skipped',
            src_h3::text,
            dst_h3::text,
            preferred_h3::text;
    end if;

    if hits_penalized then
        raise exception 'Routing between % and % should avoid downhill neighbor % after the elevation penalty, yet it appeared',
            src_h3::text,
            dst_h3::text,
            penalized_h3::text;
    end if;
end;
$$;

rollback;
