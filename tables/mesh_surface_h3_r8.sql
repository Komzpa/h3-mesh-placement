set client_min_messages = warning;

drop table if exists mesh_surface_h3_r8;
-- Create core mesh surface table with indicators per H3 cell
create table mesh_surface_h3_r8 (
    h3 h3index primary key,
    geom geometry not null,
    ele double precision,
    has_road boolean default false,
    population numeric,
    has_tower boolean default false,
    has_reception boolean,
    can_place_tower boolean,
    visible_uncovered_population numeric,
    distance_to_closest_tower double precision
);

insert into mesh_surface_h3_r8 (h3, geom)
select h3, geom
from mesh_surface_domain_h3_r8;

update mesh_surface_h3_r8 s
set ele = g.ele
from gebco_elevation_h3_r8 g
where s.h3 = g.h3::h3index;

update mesh_surface_h3_r8 s
set has_road = true
where exists (
    select 1 from roads_h3_r8 r where r.h3::h3index = s.h3
);

update mesh_surface_h3_r8 s
set population = p.population
from population_h3_r8 p
where s.h3 = p.h3::h3index;

update mesh_surface_h3_r8 s
set has_tower = true,
    has_reception = true,
    can_place_tower = false
where exists (
    select 1 from mesh_towers t where t.h3 = s.h3
);

with tower_points as (
    select h3, h3::geography as geog
    from mesh_towers
)
update mesh_surface_h3_r8 s
set distance_to_closest_tower = sub.dist_m
from (
    select
        s2.h3,
        min(ST_Distance(s2.h3::geography, t.geog)) as dist_m
    from mesh_surface_h3_r8 s2
    join tower_points t on true
    group by s2.h3
) sub
where s.h3 = sub.h3;

update mesh_surface_h3_r8 s
set can_place_tower = false
where can_place_tower is distinct from false
  and (
        not exists (
            select 1
            from georgia_boundary b
            where ST_Intersects(b.geom, s.geom)
        )
        or has_road is not true
        or has_tower
        or distance_to_closest_tower < 5000
    );

update mesh_surface_h3_r8
set can_place_tower = true
where can_place_tower is null
  and has_road
  and distance_to_closest_tower >= 5000;

create index if not exists mesh_surface_h3_r8_geom_idx on mesh_surface_h3_r8 using gist (geom);
create index if not exists mesh_surface_h3_r8_brin_all on mesh_surface_h3_r8 using brin (ele, has_road, population, has_tower, has_reception, can_place_tower, visible_uncovered_population, distance_to_closest_tower);
