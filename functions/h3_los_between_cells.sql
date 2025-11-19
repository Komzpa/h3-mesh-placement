set client_min_messages = warning;

drop function if exists h3_los_between_cells(h3index, h3index);
-- Create helper function that tests LOS between two H3 cells via 3D segments sampling with curvature correction
create or replace function h3_los_between_cells(h3_a h3index, h3_b h3index)
    returns boolean
    language sql
as
$$
with endpoints as (
    select
        h3_a as h3_a,
        h3_b as h3_b,
        ST_SetSRID(ST_MakePoint(ST_X(h3_a::geometry), ST_Y(h3_a::geometry)), 4326) as geom_a,
        ST_SetSRID(ST_MakePoint(ST_X(h3_b::geometry), ST_Y(h3_b::geometry)), 4326) as geom_b,
        coalesce((select ele from gebco_elevation_h3_r8 where h3 = h3_a), 0) + 2 as ele_a,
        coalesce((select ele from gebco_elevation_h3_r8 where h3 = h3_b), 0) + 2 as ele_b,
        ST_Distance(h3_a::geography, h3_b::geography) as total_dist_m
)
select
    case
        when (select total_dist_m from endpoints) > 60000 then false
        when exists (
            select 1
            from (
                select
                    sample_point,
                    ray_elev - (6371000 * (1 - cos(dist_from_start / 6371000))) as curved_ray_elev,
                    h3_latlng_to_cell(sample_point::geography, 8) as h3
                from (
                    select
                        ST_Segmentize(
                            ST_MakeLine(
                                ST_SetSRID(ST_MakePoint(ST_X(geom_a), ST_Y(geom_a), ele_a), 4326),
                                ST_SetSRID(ST_MakePoint(ST_X(geom_b), ST_Y(geom_b), ele_b), 4326)
                            ),
                            200
                        ) as geom,
                        geom_a,
                        geom_b
                    from endpoints
                ) seg,
                LATERAL ST_DumpPoints(seg.geom) dp,
                LATERAL (
                    select
                        dp.geom as sample_point,
                        ST_Z(dp.geom) as ray_elev,
                        ST_Distance(geom_a::geography, dp.geom::geography) as dist_from_start
                ) samples
            ) samples_with_curvature
            left join gebco_elevation_h3_r8 ge on ge.h3 = samples_with_curvature.h3
            where samples_with_curvature.h3 <> (select h3_a from endpoints)
              and samples_with_curvature.h3 <> (select h3_b from endpoints)
              and (samples_with_curvature.curved_ray_elev < coalesce(ge.ele, 0) + 2)
        ) then false
        else true
    end;
$$;
