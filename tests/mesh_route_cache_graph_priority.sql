set client_min_messages = warning;

begin;

truncate mesh_surface_h3_r8;
truncate mesh_towers;
truncate mesh_los_cache;
truncate mesh_visibility_edges;

-- Seed a tiny mesh surface with two towers and a few bridge-capable cells.
do
$$
declare
    tower_a_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    tower_b_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(1.2, 0.0), 4326), 8);
    bridge_one_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.4, 0.0), 4326), 8);
    bridge_two_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.8, 0.0), 4326), 8);
    short_gap_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.02, 0.0), 4326), 8);
begin
    with anchor_nodes as (
        select unnest(array[
            tower_a_h3,
            tower_b_h3,
            bridge_one_h3,
            bridge_two_h3,
            short_gap_h3
        ]) as h3
    ),
    paths as (
        select h3_grid_path_cells(n1.h3, n2.h3) as h3
        from anchor_nodes n1
        join anchor_nodes n2 on n1.h3 < n2.h3
    ),
    all_cells as (
        select h3 from anchor_nodes
        union
        select h3 from paths
    )
    insert into mesh_surface_h3_r8 (
        h3,
        ele,
        has_road,
        population,
        has_tower,
        clearance,
        path_loss,
        is_in_boundaries,
        is_in_unfit_area,
        min_distance_to_closest_tower,
        visible_population,
        visible_uncovered_population,
        visible_tower_count,
        distance_to_closest_tower
    )
    select
        ac.h3,
        0,
        case
            when ac.h3 in (tower_a_h3, tower_b_h3, bridge_one_h3, bridge_two_h3, short_gap_h3) then true
            else false
        end,
        0,
        case
            when ac.h3 in (tower_a_h3, tower_b_h3) then true
            else false
        end,
        null,
        null,
        true,
        false,
        case
            when ac.h3 in (bridge_one_h3, bridge_two_h3, short_gap_h3) then 5000
            else 0
        end,
        null,
        null,
        0,
        case
            when ac.h3 in (bridge_one_h3, bridge_two_h3, short_gap_h3) then 6000
            when ac.h3 in (tower_a_h3, tower_b_h3) then 0
            else 10000
        end
    from all_cells ac;

    insert into mesh_towers (h3, source)
    values
        (tower_a_h3, 'test_route_seed'),
        (tower_b_h3, 'test_route_seed');
end;
$$;

-- Craft two invisible visibility edges: one brushing against bridge_one, another far away.
with tower_pair as (
    select
        src.tower_id as source_id,
        dst.tower_id as target_id,
        src.h3 as source_h3,
        dst.h3 as target_h3
    from mesh_towers src
    join mesh_towers dst on dst.tower_id > src.tower_id
    order by src.tower_id, dst.tower_id
    limit 1
)
insert into mesh_visibility_edges (
    source_id,
    target_id,
    source_h3,
    target_h3,
    distance_m,
    is_visible,
    is_between_clusters,
    cluster_hops,
    geom
)
select
    tp.source_id,
    tp.target_id,
    tp.source_h3,
    tp.target_h3,
    ST_Distance(close_start::geography, close_end::geography),
    false,
    true,
    null,
    ST_MakeLine(close_start, close_end)
from tower_pair tp,
     lateral (
         select
             ST_SetSRID(ST_MakePoint(0.4, 0.01), 4326) as close_start,
             ST_SetSRID(ST_MakePoint(0.8, 0.01), 4326) as close_end
     ) as close_geom
union all
select
    tp.source_id,
    tp.target_id,
    tp.source_h3,
    tp.target_h3,
    ST_Distance(far_start::geography, far_end::geography),
    false,
    true,
    null,
    ST_MakeLine(far_start, far_end)
from tower_pair tp,
     lateral (
         select
             ST_SetSRID(ST_MakePoint(10.0, 10.0), 4326) as far_start,
             ST_SetSRID(ST_MakePoint(11.0, 11.0), 4326) as far_end
     ) as far_geom;

-- Mirror the candidate+priority staging tables from mesh_route_cache_graph for this miniature scene.
drop table if exists mesh_route_candidate_cells;
create table mesh_route_candidate_cells as
select
    s.h3,
    s.centroid_geog
from mesh_surface_h3_r8 s
where s.has_tower
   or s.can_place_tower;

alter table mesh_route_candidate_cells
    add primary key (h3);

create index if not exists mesh_route_candidate_cells_geog_idx
    on mesh_route_candidate_cells
    using gist (centroid_geog);

-- Precompute nearest invisible edge distance per candidate.
drop table if exists mesh_route_candidate_invisible_dist;
create table mesh_route_candidate_invisible_dist as
select
    c.h3,
    coalesce(
        (
            select ST_Distance(c.centroid_geog, e.geom::geography)
            from mesh_visibility_edges e
            where not e.is_visible
              and e.is_between_clusters
            order by c.centroid_geog::geometry <-> e.geom
            limit 1
        ),
        70000
    ) as distance_m
from mesh_route_candidate_cells c;

alter table mesh_route_candidate_invisible_dist
    add primary key (h3);

-- Build missing pairs exactly like the procedure does.
drop table if exists mesh_route_pair_candidates;
create table mesh_route_pair_candidates as
select
    c1.h3 as src_h3,
    c2.h3 as dst_h3,
    ST_Distance(c1.centroid_geog, c2.centroid_geog) as distance_m
from mesh_route_candidate_cells c1
join mesh_route_candidate_cells c2
  on c2.h3 > c1.h3
 where ST_DWithin(c1.centroid_geog, c2.centroid_geog, 70000)
   and not ST_DWithin(c1.centroid_geog, c2.centroid_geog, 5000);

create index on mesh_route_pair_candidates (src_h3, dst_h3);

drop table if exists mesh_route_missing_pairs;
create table mesh_route_missing_pairs as
select
    pr.src_h3,
    pr.dst_h3,
    least(src_priority.distance_m, dst_priority.distance_m) as priority
from mesh_route_pair_candidates pr
left join mesh_los_cache mlc
    on mlc.src_h3 = pr.src_h3
   and mlc.dst_h3 = pr.dst_h3
   and mlc.mast_height_src = 28
   and mlc.mast_height_dst = 28
   and mlc.frequency_hz = 868e6
join mesh_route_candidate_invisible_dist src_priority on src_priority.h3 = pr.src_h3
join mesh_route_candidate_invisible_dist dst_priority on dst_priority.h3 = pr.dst_h3
where mlc.src_h3 is null;

create index on mesh_route_missing_pairs using brin (priority);

-- Verify the priority equals the distance to the handmade close edge.
do
$$
declare
    bridge_one_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.4, 0.0), 4326), 8);
    expected_distance double precision;
    recorded_priority double precision;
begin
    select min(priority)
    into recorded_priority
    from mesh_route_missing_pairs;

    select ST_Distance(
        s.centroid_geog,
        ST_MakeLine(
            ST_SetSRID(ST_MakePoint(0.4, 0.01), 4326),
            ST_SetSRID(ST_MakePoint(0.8, 0.01), 4326)
        )::geography
    )
    into expected_distance
    from mesh_route_candidate_cells s
    where s.h3 = bridge_one_h3;

    if abs(recorded_priority - expected_distance) > 1 then
        raise exception 'Priority % should match nearest invisible edge distance %, expected entries for %, close edge offset by 0.01 degrees',
            recorded_priority,
            expected_distance,
            bridge_one_h3::text;
    end if;
end;
$$;

-- Ensure the prioritized selection picks the pair containing the closest bridge cell.
do
$$
declare
    bridge_one_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.4, 0.0), 4326), 8);
    prioritized_pair record;
begin
    select src_h3, dst_h3
    into prioritized_pair
    from (
        select mp.src_h3, mp.dst_h3
        from mesh_route_missing_pairs mp
        order by mp.priority, mp.src_h3, mp.dst_h3
        limit 1
    ) sub;

    if prioritized_pair.src_h3 <> bridge_one_h3 and prioritized_pair.dst_h3 <> bridge_one_h3 then
        raise exception 'Closest missing pair % <> % does not include bridge_one %, so priority ordering failed',
            prioritized_pair.src_h3::text,
            prioritized_pair.dst_h3::text,
            bridge_one_h3::text;
    end if;
end;
$$;

rollback;
