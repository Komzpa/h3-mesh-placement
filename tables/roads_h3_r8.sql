set client_min_messages = warning;

drop table if exists roads_h3_r8;
-- Create H3 table storing total road length per cell
create table roads_h3_r8 as
select
    h3_latlng_to_cell(ST_StartPoint(seg_geom)::geography, 8) as h3,
    sum(ST_Length(seg_geom::geography)) as road_length_m
from (
    select (ST_DumpSegments(ST_Segmentize(geom::geography, 200)::geometry)).geom as seg_geom
    from georgia_roads_geom
) segments
where seg_geom is not null
group by 1;
alter table roads_h3_r8 add primary key (h3);
