set client_min_messages = warning;

drop table if exists mesh_surface_domain_h3_r8;
-- Create H3 domain covering the convex hull for surface calculations
create table mesh_surface_domain_h3_r8 as
select
    cell as h3,
    h3_cell_to_boundary_geometry(cell) as geom
from h3_polygon_to_cells((select geom from georgia_convex_hull), 8) as cell;
alter table mesh_surface_domain_h3_r8 add primary key (h3);
create index if not exists mesh_surface_domain_h3_r8_geom_idx on mesh_surface_domain_h3_r8 using gist (geom);
