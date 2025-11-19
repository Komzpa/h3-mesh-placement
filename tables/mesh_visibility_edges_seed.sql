set client_min_messages = warning;

drop table if exists mesh_visibility_edges_seed;
-- Create table describing expected visibility between seed towers
create table mesh_visibility_edges_seed (
    src_h3 h3index not null,
    dst_h3 h3index not null,
    distance_m double precision not null,
    is_visible boolean not null,
    geom geometry not null
);

insert into mesh_visibility_edges_seed (src_h3, dst_h3, distance_m, is_visible, geom)
select
    a.h3 as src_h3,
    b.h3 as dst_h3,
    ST_Distance(a.h3::geography, b.h3::geography) as distance_m,
    h3_los_between_cells(a.h3, b.h3) as is_visible,
    ST_MakeLine(a.h3::geometry, b.h3::geometry) as geom
from mesh_initial_nodes_h3_r8 a
join mesh_initial_nodes_h3_r8 b
    on a.h3 < b.h3
where ST_DWithin(a.h3::geography, b.h3::geography, 60000);

comment on table mesh_visibility_edges_seed is
    'Line-of-sight matrix for seed towers. Expect true edges for (Poti,Gomismta), (Poti,Feria 2), (Komzpa,Feria 2), (Komzpa,Batumi South), (Batumi South,SoNick).';
