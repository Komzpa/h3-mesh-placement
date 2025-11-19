set client_min_messages = warning;

drop table if exists georgia_boundary;
-- Create dissolved boundary of Georgia from OSM polygons
create table georgia_boundary as
select ST_Union(
           ST_Multi(
               geog::geometry
           )
       ) as geom
from osm_georgia
where tags ? 'boundary'
  and tags ->> 'boundary' = 'administrative'
  and tags ->> 'admin_level' = '2'
  and (
        coalesce(tags ->> 'name', '') ilike 'georgia'
        or coalesce(tags ->> 'name:en', '') ilike 'georgia'
    )
  and ST_GeometryType(geog::geometry) in ('ST_Polygon', 'ST_MultiPolygon');
