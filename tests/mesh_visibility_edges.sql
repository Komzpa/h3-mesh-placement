set client_min_messages = warning;

-- Ensure mesh_visibility_edges entries covering seed towers match h3_visibility_clearance() expectations.
begin;

do
$$
declare
    rec record;
    constant_mast_height constant double precision := 28;
    constant_frequency constant double precision := 868e6;
    max_distance constant double precision := 70000;
begin
    for rec in
        with pairs as (
            select
                a.h3 as src_h3,
                b.h3 as dst_h3,
                coalesce(a.name, 'seed') as src_name,
                coalesce(b.name, 'seed') as dst_name,
                h3_visibility_clearance(
                    a.h3,
                    b.h3,
                    constant_mast_height,
                    constant_mast_height,
                    constant_frequency
                ) as clearance,
                ST_Distance(a.h3::geography, b.h3::geography) as distance_m
            from mesh_initial_nodes_h3_r8 a
            join mesh_initial_nodes_h3_r8 b
              on a.h3 < b.h3
            where ST_Distance(a.h3::geography, b.h3::geography) <= max_distance
        )
        select
            p.src_h3,
            p.dst_h3,
            p.src_name,
            p.dst_name,
            p.distance_m,
            p.clearance,
            (p.clearance > 0) as expected_visible,
            e.is_visible as stored_visible
        from pairs p
        left join mesh_visibility_edges e
          on least(e.source_h3, e.target_h3) = p.src_h3
         and greatest(e.source_h3, e.target_h3) = p.dst_h3
    loop
        if rec.stored_visible is null then
            raise exception 'Seed visibility edge missing for % (%s) -> % (%s) at %.2f km; clearance % meters and expected visibility %',
                rec.src_name, rec.src_h3::text, rec.dst_name, rec.dst_h3::text, rec.distance_m / 1000.0, rec.clearance, rec.expected_visible;
        end if;

        if rec.stored_visible is distinct from rec.expected_visible then
            raise exception 'Seed visibility mismatch for % (%s) -> % (%s): stored % but clearance % meters implies %',
                rec.src_name, rec.src_h3::text, rec.dst_name, rec.dst_h3::text, rec.stored_visible, rec.clearance, rec.expected_visible;
        end if;
    end loop;
end;
$$;

do
$$
declare
    rec record;
begin
    for rec in
        with expectations as (
            select
                least(src_name, dst_name) as src_name,
                greatest(src_name, dst_name) as dst_name,
                expected_visible
            from (
                values
                    ('Poti', 'Feria 2', true),
                    ('Poti', 'SoNick', true),
                    ('Komzpa', 'Feria 2', true),
                    ('Komzpa', 'Batumi South', true),
                    ('Batumi South', 'SoNick', true),
                    ('Tbilisi hackerspace', 'Poti', false),
                    ('Tbilisi hackerspace', 'Feria 2', false),
                    ('Tbilisi hackerspace', 'Komzpa', false),
                    ('Tbilisi hackerspace', 'Batumi South', false),
                    ('Tbilisi hackerspace', 'SoNick', false)
            ) as v(src_name, dst_name, expected_visible)
        ),
        actual as (
            select
                least(coalesce(a.name, 'seed'), coalesce(b.name, 'seed')) as src_name,
                greatest(coalesce(a.name, 'seed'), coalesce(b.name, 'seed')) as dst_name,
                e.is_visible
            from mesh_visibility_edges e
            join mesh_initial_nodes_h3_r8 a
              on a.h3 = e.source_h3
            join mesh_initial_nodes_h3_r8 b
              on b.h3 = e.target_h3
        )
        select
            exp.src_name,
            exp.dst_name,
            exp.expected_visible,
            act.is_visible
        from expectations exp
        left join actual act
          on act.src_name = exp.src_name
         and act.dst_name = exp.dst_name
    loop
        if rec.is_visible is null then
            raise exception 'Seed visibility row missing for % -> %; expected visibility % per doc/H3 ground truth',
                rec.src_name, rec.dst_name, rec.expected_visible;
        end if;

        if rec.is_visible is distinct from rec.expected_visible then
            raise exception 'Seed visibility row mismatch for % -> %: stored %, expected % per doc/H3 ground truth',
                rec.src_name, rec.dst_name, rec.is_visible, rec.expected_visible;
        end if;
    end loop;
end;
$$;

drop table if exists tmp_test_visibility_cluster_edges;
-- Temporary helper storing LOS edges (<=70 km) so we can recompute hop counts for seed towers.
create temporary table tmp_test_visibility_cluster_edges as
select
    row_number() over () as edge_id,
    e.source_id,
    e.target_id,
    1::double precision as cost
from mesh_visibility_edges e
where e.is_visible
  and e.distance_m <= 70000;

do
$$
declare
    rec record;
begin
    for rec in
        with seed_towers as (
            -- Restrict hop validation to the documented seed towers so the loop stays small.
            select t.tower_id
            from mesh_towers t
            join mesh_initial_nodes_h3_r8 seeds on seeds.h3 = t.h3
        ),
        expected as (
            -- Recompute hop counts between every connected seed pair using pgRouting for comparison.
            select
                least(result.start_vid, result.end_vid) as source_id,
                greatest(result.start_vid, result.end_vid) as target_id,
                result.agg_cost::integer as hops
            from pgr_dijkstra(
                'select edge_id as id, source_id as source, target_id as target, cost, cost as reverse_cost from tmp_test_visibility_cluster_edges',
                coalesce(array(select tower_id::bigint from seed_towers order by tower_id), array[]::bigint[]),
                coalesce(array(select tower_id::bigint from seed_towers order by tower_id), array[]::bigint[]),
                false
            ) as result
            where result.start_vid < result.end_vid
              and result.agg_cost < 'Infinity'::double precision
              and result.node = result.end_vid
        )
        select
            e.source_id,
            e.target_id,
            exp.hops,
            e.cluster_hops
        from expected exp
        join mesh_visibility_edges e
          on e.source_id = exp.source_id
         and e.target_id = exp.target_id
    loop
        if rec.cluster_hops is null then
            raise exception 'cluster_hops missing for seed pair % -> %; expected % hops', rec.source_id, rec.target_id, rec.hops;
        end if;

        if rec.cluster_hops <> rec.hops then
            raise exception 'cluster_hops mismatch for seed pair % -> %: stored %, expected %', rec.source_id, rec.target_id, rec.cluster_hops, rec.hops;
        end if;
    end loop;
end;
$$;

drop table if exists tmp_test_visibility_cluster_edges;

rollback;
