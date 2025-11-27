set client_min_messages = warning;

-- Regression test: greedy preparation must keep route-promoted towers intact so rerunning
-- the placement loop after mesh_route_bridge does not throw away freshly installed links.

begin;

truncate mesh_surface_h3_r8;
truncate mesh_towers;
truncate mesh_greedy_iterations;

do
$$
declare
    seed_a h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    seed_b h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.5, 0.0), 4326), 8);
    route_bridge h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.25, 0.05), 4326), 8);
begin
    with setup_cells as (
        select unnest(array[seed_a, seed_b, route_bridge]) as h3
    )
    insert into mesh_surface_h3_r8 (
        h3,
        ele,
        has_road,
        population,
        has_tower,
        clearance,
        path_loss,
        is_in_boundaries,
        is_in_unfit_area,
        min_distance_to_closest_tower,
        visible_population,
        visible_uncovered_population,
        visible_tower_count,
        distance_to_closest_tower
    )
    select
        sc.h3,
        0,
        true,
        10,
        false,
        null,
        null,
        true,
        false,
        0,
        null,
        null,
        0,
        5000
    from setup_cells sc;

    insert into mesh_towers (h3, source)
    values
        (seed_a, 'seed'),
        (seed_b, 'seed'),
        (route_bridge, 'route');
end;
$$;

call mesh_run_greedy_prepare();

do
$$
declare
    route_bridge h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.25, 0.05), 4326), 8);
    route_tower_count integer;
    route_has_tower boolean;
begin
    select count(*) into route_tower_count
    from mesh_towers
    where h3 = route_bridge
      and source = 'route';

    if route_tower_count <> 1 then
        raise exception 'mesh_run_greedy_prepare should preserve route tower %, found % row(s)',
            route_bridge::text,
            route_tower_count;
    end if;

    select has_tower into route_has_tower
    from mesh_surface_h3_r8
    where h3 = route_bridge;

    if route_has_tower is distinct from true then
        raise exception 'mesh_run_greedy_prepare should mark % as has_tower=true, saw %',
            route_bridge::text,
            route_has_tower;
    end if;
end;
$$;

rollback;
