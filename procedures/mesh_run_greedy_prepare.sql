set client_min_messages = notice;

drop procedure if exists mesh_run_greedy_prepare();
-- Reset greedy-placement artifacts while preserving any towers the routing stage just promoted.
create or replace procedure mesh_run_greedy_prepare()
language plpgsql
as
$$
begin
    raise notice 'Resetting greedy artifacts';

    -- Leave 'seed' and 'route' towers intact so mesh_route_bridge progress survives greedy reruns.
    delete from mesh_towers
    where source not in ('seed', 'route');

    truncate mesh_greedy_iterations;

    -- Align the serial so subsequent inserts keep growing instead of reusing deleted ids.
    perform setval(
        pg_get_serial_sequence('mesh_towers', 'tower_id'),
        coalesce((select max(tower_id) from mesh_towers), 0)
    );

    -- Rehydrate derived mesh_surface flags based on the tower registry.
    update mesh_surface_h3_r8
    set has_tower = false
    where has_tower;

    update mesh_surface_h3_r8 s
    set has_tower = true
    from mesh_towers t
    where s.h3 = t.h3;

    -- Refresh distance_to_closest_tower for the entire surface (bridges included).
    update mesh_surface_h3_r8 s
    set distance_to_closest_tower = sub.dist_m
    from (
        select
            s2.h3,
            ST_Distance(s2.centroid_geog, nearest.centroid_geog) as dist_m
        from mesh_surface_h3_r8 s2
        cross join lateral (
            select t.centroid_geog
            from mesh_towers t
            order by s2.centroid_geog <-> t.centroid_geog
            limit 1
        ) as nearest
    ) sub
    where s.h3 = sub.h3;

    -- Rebuild LOS counts against the current tower set.
    update mesh_surface_h3_r8
    set visible_tower_count = null;

    with tower_points as (
        select h3, centroid_geog
        from mesh_towers
    ),
    visible_pairs as (
        select
            s.h3 as cell_h3,
            tp.h3 as tower_h3
        from mesh_surface_h3_r8 s
        join tower_points tp
            on tp.h3 <> s.h3
           and ST_DWithin(s.centroid_geog, tp.centroid_geog, 70000)
    ),
    visible_counts as (
        select
            vp.cell_h3,
            count(*) as visible_count
        from visible_pairs vp
        where h3_los_between_cells(vp.cell_h3, vp.tower_h3)
        group by vp.cell_h3
    )
    update mesh_surface_h3_r8 s
    set visible_tower_count = vc.visible_count
    from visible_counts vc
    where s.h3 = vc.cell_h3;

    update mesh_surface_h3_r8
    set visible_tower_count = 0
    where visible_tower_count is null;

    -- Clear cached RF metrics so the greedy loop recomputes them using the refreshed tower state.
    update mesh_surface_h3_r8
    set clearance = null,
        path_loss = null,
        visible_uncovered_population = case
            when has_tower then 0
            else null
        end;

    with nearest_towers as (
        select
            s_inner.h3,
            nt.tower_h3
        from mesh_surface_h3_r8 s_inner
        cross join lateral (
            select t.h3 as tower_h3
            from mesh_towers t
            where t.h3 <> s_inner.h3
            order by ST_Distance(s_inner.centroid_geog, t.centroid_geog)
            limit 1
        ) nt
        where s_inner.population > 0
          and s_inner.distance_to_closest_tower < 70000
    ),
    population_metrics as (
        select
            nt.h3,
            m.clearance,
            m.path_loss_db
        from nearest_towers nt
        cross join lateral h3_visibility_metrics(
            nt.h3,
            nt.tower_h3,
            28,
            28,
            868e6
        ) as m(clearance, path_loss_db)
    )
    update mesh_surface_h3_r8 s
    set clearance = pm.clearance,
        path_loss = pm.path_loss_db
    from population_metrics pm
    where s.h3 = pm.h3;
end;
$$;
