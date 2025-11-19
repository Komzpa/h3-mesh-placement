set client_min_messages = warning;

update mesh_surface_h3_r8
set has_reception = null,
    visible_uncovered_population = null
where has_tower is not true;

update mesh_surface_h3_r8
set has_reception = true,
    visible_uncovered_population = 0
where has_tower;

do
$$
declare
    max_distance constant double precision := 60000;
    separation constant double precision := 5000;
    recalc_reception constant double precision := 50000;
    recalc_population constant double precision := 100000;
    iteration integer := coalesce((select max(iteration) from mesh_greedy_iterations), 0);
    candidate record;
    new_tower_id integer;
begin
    loop
        update mesh_surface_h3_r8 s
        set has_reception = q.has_reception
        from (
            select
                s1.h3,
                exists (
                    select 1
                    from mesh_towers t
                    where h3_los_between_cells(s1.h3, t.h3)
                ) as has_reception
            from mesh_surface_h3_r8 s1
            where s1.has_reception is null
        ) q
        where s.h3 = q.h3;

        update mesh_surface_h3_r8
        set can_place_tower = true
        where can_place_tower is null
          and has_road
          and has_tower is not true
          and distance_to_closest_tower >= separation;

        update mesh_surface_h3_r8 s
        set visible_uncovered_population = coalesce(q.visible_pop, 0)
        from (
            select
                c.h3,
                (
                    select sum(population)
                    from mesh_surface_h3_r8 target
                    where target.population is not null
                      and target.population > 0
                      and target.has_reception is not true
                      and h3_los_between_cells(c.h3, target.h3)
                ) as visible_pop
            from mesh_surface_h3_r8 c
            where c.can_place_tower
              and c.visible_uncovered_population is null
        ) q
        where s.h3 = q.h3;

        select s.h3,
               s.visible_uncovered_population
        into candidate
        from mesh_surface_h3_r8 s
        where s.can_place_tower
          and coalesce(s.visible_uncovered_population, 0) > 0
        order by s.visible_uncovered_population desc
        limit 1;

        exit when candidate.h3 is null;

        iteration := iteration + 1;

        insert into mesh_towers (h3, source)
        values (candidate.h3, 'greedy')
        on conflict (h3) do update set source = excluded.source
        returning tower_id into new_tower_id;

        insert into mesh_greedy_iterations (iteration, chosen_h3, visible_population)
        values (iteration, candidate.h3, candidate.visible_uncovered_population);

        update mesh_surface_h3_r8
        set has_tower = true,
            has_reception = true,
            can_place_tower = false,
            visible_uncovered_population = 0,
            distance_to_closest_tower = 0
        where h3 = candidate.h3;

        update mesh_surface_h3_r8
        set has_reception = null
        where ST_Distance(h3::geography, candidate.h3::geography) <= recalc_reception
          and has_tower is not true;

        update mesh_surface_h3_r8
        set visible_uncovered_population = null
        where ST_Distance(h3::geography, candidate.h3::geography) <= recalc_population
          and has_tower is not true;

        update mesh_surface_h3_r8
        set distance_to_closest_tower = coalesce(
            least(
                distance_to_closest_tower,
                ST_Distance(h3::geography, candidate.h3::geography)
            ),
            ST_Distance(h3::geography, candidate.h3::geography)
        )
        where ST_Distance(h3::geography, candidate.h3::geography) <= recalc_population;

        update mesh_surface_h3_r8
        set can_place_tower = false
        where can_place_tower
          and ST_Distance(h3::geography, candidate.h3::geography) < separation;
    end loop;
end;
$$;

vacuum analyze mesh_surface_h3_r8;

truncate mesh_visibility_edges_active;
insert into mesh_visibility_edges_active (source_id, target_id, distance_m, is_visible, geom)
select
    t1.tower_id as source_id,
    t2.tower_id as target_id,
    ST_Distance(t1.h3::geography, t2.h3::geography) as distance_m,
    h3_los_between_cells(t1.h3, t2.h3) as is_visible,
    ST_MakeLine(t1.h3::geometry, t2.h3::geometry) as geom
from mesh_towers t1
join mesh_towers t2
    on t1.tower_id < t2.tower_id;
