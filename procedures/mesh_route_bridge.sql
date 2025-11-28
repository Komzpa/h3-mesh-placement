set client_min_messages = notice;

\set max_distance 70000
\set refresh_radius 70000

-- Return ordered intermediate H3 cells for cheapest bridge between two clusters.
drop function if exists mesh_route_intermediate_hexes(integer, integer);
create or replace function mesh_route_intermediate_hexes(
        start_cluster integer,
        end_cluster integer
    )
    returns table (seq integer, h3 h3index)
    language plpgsql
    volatile
as
$$
declare
    start_vids integer[];
    end_vids integer[];
    separation constant double precision := 5000;
    start_h3s h3index[];
    end_h3s h3index[];
begin
    -- Gather node ids for the source cluster from the cached routing graph.
    select
        array_agg(mrn.node_id),
        array_agg(mrn.h3)
    into start_vids, start_h3s
    from mesh_route_nodes mrn
    join mesh_towers mt on mt.h3 = mrn.h3
    join mesh_tower_clusters() tc on tc.tower_id = mt.tower_id
    where tc.cluster_id = start_cluster;

    if start_vids is null or array_length(start_vids, 1) = 0 then
        return;
    end if;

    -- Gather node ids for the destination cluster.
    select
        array_agg(mrn.node_id),
        array_agg(mrn.h3)
    into end_vids, end_h3s
    from mesh_route_nodes mrn
    join mesh_towers mt on mt.h3 = mrn.h3
    join mesh_tower_clusters() tc on tc.tower_id = mt.tower_id
    where tc.cluster_id = end_cluster;

    if end_vids is null or array_length(end_vids, 1) = 0 then
        return;
    end if;

    if to_regclass('pg_temp.mesh_route_cluster_blocked_nodes') is null then
        create temporary table mesh_route_cluster_blocked_nodes (
            node_id integer primary key
        ) on commit drop;
    else
        truncate mesh_route_cluster_blocked_nodes;
    end if;

    insert into mesh_route_cluster_blocked_nodes (node_id)
    select mrn.node_id
    from mesh_route_nodes mrn
    join mesh_surface_h3_r8 surface on surface.h3 = mrn.h3
    where (start_vids is null or mrn.node_id <> all(start_vids))
      and (end_vids is null or mrn.node_id <> all(end_vids))
      and surface.centroid_geog is not null
      and exists (
            select 1
            from mesh_towers mt
            where (start_h3s is null or mt.h3 <> all(start_h3s))
              and (end_h3s is null or mt.h3 <> all(end_h3s))
              and ST_DWithin(surface.centroid_geog, mt.centroid_geog, separation)
        );

    -- Use pgRouting to find the minimum path-loss corridor, then keep only cells that do not already host towers.
    return query
    with raw_paths as (
        select
            pdr.*,
            sum(case when pdr.path_seq = 1 then 1 else 0 end) over (order by pdr.seq) as path_id
        from pgr_dijkstra(
            'select edge_id as id,
                    source,
                    target,
                    cost,
                    reverse_cost
             from mesh_route_edges
             where source not in (select node_id from mesh_route_cluster_blocked_nodes)
               and target not in (select node_id from mesh_route_cluster_blocked_nodes)',
            start_vids,
            end_vids,
            false
        ) as pdr
    ),
    best_path as (
        select path_id
        from raw_paths
        where node <> -1
        group by path_id
        order by max(agg_cost) asc
        limit 1
    ),
    ordered_nodes as (
        select
            rp.seq,
            rn.h3
        from raw_paths rp
        join best_path bp on bp.path_id = rp.path_id
        join mesh_route_nodes rn on rn.node_id = rp.node
        join mesh_surface_h3_r8 s on s.h3 = rn.h3
        where rp.node <> -1
          and s.has_tower is not true
        order by rp.seq
    )
    select ordered_nodes.seq, ordered_nodes.h3 from ordered_nodes;
end;
$$;

do
$$
declare
    max_distance constant double precision := 70000;
    refresh_radius constant double precision := 70000;
    total_new integer := 0;
    cluster_total integer;
    start_cluster integer;
    end_cluster integer;
    pair_distance double precision;
    path_node_count integer;
    route_added integer;
    new_h3 h3index;
    new_centroid public.geography;
begin
    -- Ensure the seed step populated the routing graph before trying to bridge anything.
    if to_regclass('mesh_route_nodes') is null then
        raise notice 'mesh_route_nodes table missing, skipping routing';
        return;
    end if;

    if not exists (select 1 from mesh_route_nodes) then
        raise notice 'mesh_route_nodes not prepared, skipping routing';
        return;
    end if;

    if to_regclass('mesh_route_edges') is null then
        raise notice 'mesh_route_edges table missing, skipping routing';
        return;
    end if;

    if not exists (select 1 from mesh_route_edges) then
        raise notice 'mesh_route_edges not prepared, skipping routing';
        return;
    end if;

    -- Short-circuit if everything already belongs to one connected component.
    select count(distinct cluster_id)
    into cluster_total
    from mesh_tower_clusters();

    if cluster_total is null or cluster_total <= 1 then
        raise notice 'Tower graph already forms a single cluster, skipping routing';
        return;
    end if;

    -- Temporary table stores the path for the current cluster pair so we can reuse it across statements.
    drop table if exists mesh_route_path_nodes_work;
    create temporary table mesh_route_path_nodes_work (
        seq integer,
        h3 h3index
    ) on commit drop;
    -- Track cluster pairs that have already been attempted so we can try the rest even if one fails.
    drop table if exists mesh_route_failed_pairs;
    create temporary table mesh_route_failed_pairs (
        cluster_a integer,
        cluster_b integer,
        failure_reason text,
        unique (cluster_a, cluster_b)
    ) on commit drop;

    loop
        -- Refresh cluster counts each iteration because new towers change connectivity.
        select count(distinct cluster_id)
        into cluster_total
        from mesh_tower_clusters();

        exit when cluster_total is null or cluster_total <= 1;

        with cluster_points as (
            -- Collect tower centroids per cluster so we can measure inter-cluster distance.
            select
                tc.cluster_id,
                mt.centroid_geog
            from mesh_tower_clusters() tc
            join mesh_towers mt on mt.tower_id = tc.tower_id
        ),
        cluster_centroids as (
            select
                cluster_id,
                ST_Collect(centroid_geog::geometry)::public.geography as centroid_geog
            from cluster_points
            group by cluster_id
        ),
        cluster_pairs as (
            -- Rank cluster pairs by separation so we bridge the widest gap first, skipping failed attempts.
            select
                c1.cluster_id as cluster_a,
                c2.cluster_id as cluster_b,
                ST_Distance(c1.centroid_geog, c2.centroid_geog) as cluster_distance
            from cluster_centroids c1
            join cluster_centroids c2 on c1.cluster_id < c2.cluster_id
            where not exists (
                select 1
                from mesh_route_failed_pairs fp
                where fp.cluster_a = c1.cluster_id
                  and fp.cluster_b = c2.cluster_id
            )
        )
        -- Lock in the next cluster pair to bridge.
        select cluster_a, cluster_b, cluster_distance
        into start_cluster, end_cluster, pair_distance
        from cluster_pairs
        order by cluster_distance asc
        limit 1;

        exit when start_cluster is null or end_cluster is null;

        -- Reset the path buffer before computing the next route.
        truncate mesh_route_path_nodes_work;

        -- Materialize the proposed bridge nodes for the selected clusters.
        insert into mesh_route_path_nodes_work (seq, h3)
        select *
        from mesh_route_intermediate_hexes(start_cluster, end_cluster);

        -- Count how many new cells we would install along this corridor.
        select count(*)
        into path_node_count
        from mesh_route_path_nodes_work;

        if path_node_count = 0 then
            insert into mesh_route_failed_pairs (cluster_a, cluster_b, failure_reason)
            values (
                least(start_cluster, end_cluster),
                greatest(start_cluster, end_cluster),
                'no los corridor'
            )
            on conflict do nothing;

            raise notice 'No feasible LOS route between clusters % and % (distance % m), trying next pair',
                start_cluster,
                end_cluster,
                pair_distance;
            continue;
        end if;

        route_added := 0;

        -- Promote every intermediate H3 along the path into a route tower and refresh local metrics.
        for new_h3 in
            insert into mesh_towers (h3, source)
            select pnw.h3, 'route'
            from mesh_route_path_nodes_work pnw
            join mesh_surface_h3_r8 s on s.h3 = pnw.h3
            on conflict (h3) do nothing
            returning h3
        loop
            route_added := route_added + 1;

            -- Grab the new tower centroid for downstream updates.
            select centroid_geog
            into new_centroid
            from mesh_surface_h3_r8
            where h3 = new_h3;

            -- Mark the promoted cell as a tower and reset cached metrics.
            update mesh_surface_h3_r8
            set has_tower = true,
                clearance = null,
                path_loss = null,
                visible_uncovered_population = 0,
                distance_to_closest_tower = 0
            where h3 = new_h3;

            -- Invalidate/adjust nearby cells so refresh functions can recompute accurate stats.
            update mesh_surface_h3_r8
            set clearance = null,
                path_loss = null,
                visible_uncovered_population = null,
                visible_tower_count = null,
                distance_to_closest_tower = coalesce(
                    least(
                        distance_to_closest_tower,
                        ST_Distance(centroid_geog, new_centroid)
                    ),
                    ST_Distance(centroid_geog, new_centroid)
                )
            where h3 <> new_h3
              and ST_DWithin(centroid_geog, new_centroid, refresh_radius);

            -- Recompute tower visibility counts around the new route tower.
            perform mesh_surface_refresh_visible_tower_counts(
                new_h3,
                refresh_radius,
                max_distance
            );

            -- Refresh reception metrics in the same radius so greedy placement has up-to-date inputs.
            perform mesh_surface_refresh_reception_metrics(
                new_h3,
                refresh_radius,
                max_distance
            );
        end loop;

        if route_added = 0 then
            insert into mesh_route_failed_pairs (cluster_a, cluster_b, failure_reason)
            values (
                least(start_cluster, end_cluster),
                greatest(start_cluster, end_cluster),
                'existing towers cover path'
            )
            on conflict do nothing;

            raise notice 'Clusters % and % already linked by existing towers (distance % m), trying next pair',
                start_cluster,
                end_cluster,
                pair_distance;
            continue;
        end if;

        total_new := total_new + route_added;
    end loop;

    -- Drop helper artifacts so future runs start from a clean slate.
    drop table if exists mesh_route_path_nodes_work;
    drop table if exists mesh_route_failed_pairs;

    raise notice 'Inserted % routing towers prior to greedy placement', coalesce(total_new, 0);
end;
$$;

drop function if exists mesh_route_intermediate_hexes(integer, integer);

vacuum analyze mesh_surface_h3_r8;
