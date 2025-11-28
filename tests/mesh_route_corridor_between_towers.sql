set client_min_messages = warning;

begin;

-- Shadow the routing tables so this test can define a deterministic corridor.
drop table if exists pg_temp.mesh_route_nodes;
create temporary table mesh_route_nodes (
    node_id integer primary key,
    h3 h3index not null unique
) on commit drop;

drop table if exists pg_temp.mesh_route_edges;
create temporary table mesh_route_edges (
    edge_id integer primary key,
    source integer not null,
    target integer not null,
    cost double precision not null,
    reverse_cost double precision not null
) on commit drop;

drop table if exists pg_temp.mesh_surface_h3_r8;
create temporary table mesh_surface_h3_r8 (
    h3 h3index primary key,
    centroid_geog geography(Point, 4326)
) on commit drop;

drop table if exists pg_temp.mesh_towers;
create temporary table mesh_towers (
    tower_id integer primary key,
    h3 h3index not null unique,
    centroid_geog geography(Point, 4326)
) on commit drop;

do
$$
declare
    src_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    mid_h3 h3index;
    dst_h3 h3index;
    hop_count integer;
begin
    -- Pick an intermediate neighbor for the corridor and a destination adjacent to that neighbor.
    select neighbor
    into mid_h3
    from h3_grid_disk(src_h3, 1) as n(neighbor)
    where neighbor <> src_h3
    limit 1;

    if mid_h3 is null then
        raise exception 'mesh_route_corridor_between_towers test failed to find a neighbor for %', src_h3::text;
    end if;

    select neighbor
    into dst_h3
    from h3_grid_disk(mid_h3, 1) as n(neighbor)
    where neighbor not in (src_h3, mid_h3)
    limit 1;

    if dst_h3 is null then
        raise exception 'mesh_route_corridor_between_towers test failed to find a destination from %', mid_h3::text;
    end if;

    insert into mesh_route_nodes (node_id, h3)
    values
        (1, src_h3),
        (2, mid_h3),
        (3, dst_h3);

    insert into mesh_route_edges (edge_id, source, target, cost, reverse_cost)
    values
        (1, 1, 2, 1, 1),
        (2, 2, 3, 1, 1);

    insert into mesh_surface_h3_r8 (h3, centroid_geog)
    values
        (src_h3, h3_cell_to_geometry(src_h3)::geography),
        (mid_h3, h3_cell_to_geometry(mid_h3)::geography),
        (dst_h3, h3_cell_to_geometry(dst_h3)::geography);

    insert into mesh_towers (tower_id, h3, centroid_geog)
    values
        (1, src_h3, h3_cell_to_geometry(src_h3)::geography),
        (2, dst_h3, h3_cell_to_geometry(dst_h3)::geography);

    select count(*)
    into hop_count
    from mesh_route_corridor_between_towers(src_h3, dst_h3);

    if hop_count <> 1 then
        raise exception 'Expected exactly one intermediate cell between % and %, got %',
            src_h3::text,
            dst_h3::text,
            hop_count;
    end if;

    -- Passing a blocked node array should keep pgRouting from returning those nodes at all.
    select count(*)
    into hop_count
    from mesh_route_corridor_between_towers(src_h3, dst_h3, array[2]);

    if hop_count <> 0 then
        raise exception 'Blocked node list should remove intermediates between % and %, still saw % rows',
            src_h3::text,
            dst_h3::text,
            hop_count;
    end if;

    -- Installing a tower on the intermediate cell should block the corridor entirely.
    insert into mesh_towers (tower_id, h3, centroid_geog)
    values (3, mid_h3, h3_cell_to_geometry(mid_h3)::geography);

    select count(*)
    into hop_count
    from mesh_route_corridor_between_towers(src_h3, dst_h3);

    if hop_count <> 0 then
        raise exception 'Expected corridor to be blocked once % became a tower, got % hops',
            mid_h3::text,
            hop_count;
    end if;
end;
$$;

rollback;
