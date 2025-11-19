set client_min_messages = warning;

drop table if exists georgia_roads_geom;
-- Create cleaned multiline geometry for Georgia roads
create table georgia_roads_geom as
select ST_Multi(
           geog::geometry
       ) as geom
from osm_georgia
where tags ? 'highway'
  and ST_GeometryType(geog::geometry) in ('ST_LineString', 'ST_MultiLineString');
