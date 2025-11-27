set client_min_messages = warning;

-- Validate mesh_route cache/bridge SQL produces expected towers (db/test/mesh_route runs the scripts first).
begin;

truncate mesh_surface_h3_r8;
truncate mesh_towers;
truncate mesh_los_cache;

do
$$
declare
    tower_a_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    tower_b_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(1.2, 0.0), 4326), 8);
    bridge_one_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.4, 0.0), 4326), 8);
    bridge_two_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.8, 0.0), 4326), 8);
    short_gap_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.02, 0.0), 4326), 8);
begin
    with anchor_nodes as (
        select unnest(array[
            tower_a_h3,
            tower_b_h3,
            bridge_one_h3,
            bridge_two_h3,
            short_gap_h3
        ]) as h3
    ),
    paths as (
        select h3_grid_path_cells(n1.h3, n2.h3) as h3
        from anchor_nodes n1
        join anchor_nodes n2 on n1.h3 < n2.h3
    ),
    all_cells as (
        select h3 from anchor_nodes
        union
        select h3 from paths
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
        ac.h3,
        0,
        case
            when ac.h3 in (tower_a_h3, tower_b_h3, bridge_one_h3, bridge_two_h3, short_gap_h3) then true
            else false
        end,
        0,
        case
            when ac.h3 in (tower_a_h3, tower_b_h3) then true
            else false
        end,
        null,
        null,
        true,
        false,
        case
            when ac.h3 in (bridge_one_h3, bridge_two_h3, short_gap_h3) then 5000
            else 0
        end,
        null,
        null,
        0,
        case
            when ac.h3 in (bridge_one_h3, bridge_two_h3, short_gap_h3) then 6000
            when ac.h3 in (tower_a_h3, tower_b_h3) then 0
            else 10000
        end
    from all_cells ac;

    insert into mesh_towers (h3, source)
    values
        (tower_a_h3, 'test_route_seed'),
        (tower_b_h3, 'test_route_seed');
end;
$$;
do
$$
declare
    tower_total integer;
    cluster_total integer;
    cache_pairs integer;
    mast_height constant double precision := 28;
    frequency constant double precision := 868e6;
    tower_a_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    tower_b_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(1.2, 0.0), 4326), 8);
    bridge_one_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.4, 0.0), 4326), 8);
    bridge_two_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.8, 0.0), 4326), 8);
    short_gap_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.02, 0.0), 4326), 8);
begin
    select count(*) into tower_total from mesh_towers;

    if tower_total <> 4 then
        raise exception 'mesh_route should promote two bridge towers, expected 4 total but saw %', tower_total;
    end if;

    if not exists (
        select 1
        from mesh_towers
        where h3 = bridge_one_h3
          and source = 'route'
    ) then
        raise exception 'mesh_route failed to insert bridge node % with source=route', bridge_one_h3::text;
    end if;

    if not exists (
        select 1
        from mesh_towers
        where h3 = bridge_two_h3
          and source = 'route'
    ) then
        raise exception 'mesh_route failed to insert bridge node % with source=route', bridge_two_h3::text;
    end if;

    if exists (
        select 1
        from mesh_towers
        where h3 = short_gap_h3
    ) then
        raise exception 'mesh_route should skip short-gap candidate %, but it was inserted anyway', short_gap_h3::text;
    end if;

    select count(distinct cluster_id)
    into cluster_total
    from mesh_tower_clusters();

    if cluster_total <> 1 then
        raise exception 'All towers should end up in one cluster after routing, saw % clusters', cluster_total;
    end if;

    -- Ensure loss cache has entries for every tower/candidate pair.
    with expected_pairs as (
        select *
        from (values
            (least(tower_a_h3, tower_b_h3), greatest(tower_a_h3, tower_b_h3)),
            (least(tower_a_h3, bridge_one_h3), greatest(tower_a_h3, bridge_one_h3)),
            (least(tower_a_h3, bridge_two_h3), greatest(tower_a_h3, bridge_two_h3)),
            (least(tower_b_h3, bridge_one_h3), greatest(tower_b_h3, bridge_one_h3)),
            (least(tower_b_h3, bridge_two_h3), greatest(tower_b_h3, bridge_two_h3)),
            (least(bridge_one_h3, bridge_two_h3), greatest(bridge_one_h3, bridge_two_h3))
        ) as ep(src_h3, dst_h3)
    )
    select count(*) into cache_pairs
    from expected_pairs ep
    where exists (
        select 1
        from mesh_los_cache mlc
        where mlc.src_h3 = ep.src_h3
          and mlc.dst_h3 = ep.dst_h3
          and mlc.mast_height_src = mast_height
          and mlc.mast_height_dst = mast_height
          and mlc.frequency_hz = frequency
    );

    if cache_pairs <> 6 then
        raise exception 'Expected cached LOS entries for all six routing pairs, got % entries', cache_pairs;
    end if;

    if exists (
        select 1
        from mesh_los_cache mlc
        where (mlc.src_h3 = short_gap_h3 and mlc.dst_h3 = tower_a_h3)
           or (mlc.src_h3 = tower_a_h3 and mlc.dst_h3 = short_gap_h3)
    ) then
        raise exception 'mesh_route should not cache LOS pairs with tower_a % and short-gap %, but entry exists',
            tower_a_h3::text,
            short_gap_h3::text;
    end if;
end;
$$;

rollback;
