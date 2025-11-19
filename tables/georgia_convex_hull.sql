set client_min_messages = warning;

drop table if exists georgia_convex_hull;
-- Create convex hull covering the Georgia boundary
create table georgia_convex_hull as
select ST_ConvexHull(ST_Collect(geom))::geometry(polygon, 4326) as geom
from georgia_boundary;
