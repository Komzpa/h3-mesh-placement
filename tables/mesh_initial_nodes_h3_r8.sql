set client_min_messages = warning;

drop table if exists mesh_initial_nodes_h3_r8;
-- Create seed towers table projected to H3 resolution 8
create table mesh_initial_nodes_h3_r8 as
select
    h3_latlng_to_cell(ST_Transform(geom, 4326)::geography, 8) as h3,
    max(coalesce(name, 'seed')) as name,
    ST_Collect(geom) as geom
from mesh_initial_nodes
group by 1;
alter table mesh_initial_nodes_h3_r8 add primary key (h3);

insert into mesh_towers (h3, source)
select h3, 'seed'
from mesh_initial_nodes_h3_r8
on conflict (h3) do nothing;
