-- Converts raster pixels to H3 aggregates using provided parameters
-- Expect psql variables: table_name, table_name_h3, item_name, aggr_func, resolution, clip_table

drop table if exists :table_name_h3;

create table :table_name_h3 as
with clip as (
    select geom from :clip_table
)
select
    h3,
    :resolution::int as resolution,
    agg_val as :item_name
from (
    select
        h3_latlng_to_cell(p.geom::geography, :resolution) as h3,
        :aggr_func(p.val) as agg_val
    from :table_name t,
         clip,
         LATERAL ST_PixelAsCentroids(ST_Clip(t.rast, clip.geom, true)) as p
    group by 1
) s;
