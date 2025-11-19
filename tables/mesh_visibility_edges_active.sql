set client_min_messages = warning;

drop table if exists mesh_visibility_edges_active;
-- Create table capturing visibility between all active towers
create table mesh_visibility_edges_active (
    source_id integer not null,
    target_id integer not null,
    source_h3 h3index not null,
    target_h3 h3index not null,
    distance_m double precision not null,
    is_visible boolean not null,
    geom geometry not null
);

insert into mesh_visibility_edges_active (source_id, target_id, source_h3, target_h3, distance_m, is_visible, geom)
select
    t1.tower_id as source_id,
    t2.tower_id as target_id,
    t1.h3 as source_h3,
    t2.h3 as target_h3,
    ST_Distance((t1.h3)::geography, (t2.h3)::geography) as distance_m,
    h3_los_between_cells(t1.h3, t2.h3) as is_visible,
    ST_MakeLine(t1.h3::geometry, t2.h3::geometry) as geom
from mesh_towers t1
join mesh_towers t2
    on t1.tower_id < t2.tower_id;

create index if not exists mesh_visibility_edges_active_geom_idx on mesh_visibility_edges_active using gist (geom);
