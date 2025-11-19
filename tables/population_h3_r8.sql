set client_min_messages = warning;

-- Create aggregated H3 population table clipped to Georgia
drop table if exists population_h3_r8;
create table population_h3_r8 as
with boundary as (
    select geom as boundary_geom from georgia_boundary
),
clipped as (
    select
        ST_CollectionExtract(k.geom, 3) as geom,
        k.population
    from kontur_population k
    join boundary b
        on ST_Intersects(k.geom, b.boundary_geom)
)
select
    cell as h3,
    sum(population)::numeric as population
from clipped c
cross join lateral h3_polygon_to_cells(c.geom, 8) as cell
group by cell;
alter table population_h3_r8 add primary key (h3);
