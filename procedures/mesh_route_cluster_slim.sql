set client_min_messages = notice;

-- Iteratively promote routing towers inside oversized clusters to cut hop counts.
create or replace procedure mesh_route_cluster_slim()
    language plpgsql
as
$$
declare
    max_distance constant double precision := 70000;
    refresh_radius constant double precision := 70000;
    separation constant double precision := 5000;
    hop_limit constant integer := 7;
    candidate record;
    path_node_count integer;
    expected_hops integer;
    new_h3 h3index;
    new_centroid public.geography;
    blocked_node_ids integer[];
    promoted_count integer;
    has_existing boolean;
begin
    if to_regclass('mesh_route_nodes') is null then
        raise notice 'mesh_route_nodes table missing, skipping cluster slimming';
        return;
    end if;

    if not exists (select 1 from mesh_route_nodes) then
        raise notice 'mesh_route_nodes not prepared, skipping cluster slimming';
        return;
    end if;

    if to_regclass('mesh_route_edges') is null then
        raise notice 'mesh_route_edges table missing, skipping cluster slimming';
        return;
    end if;

    if not exists (select 1 from mesh_route_edges) then
        raise notice 'mesh_route_edges not prepared, skipping cluster slimming';
        return;
    end if;

    call mesh_visibility_edges_refresh();

    if to_regclass('pg_temp.mesh_route_cluster_slim_failed') is not null then
        drop table mesh_route_cluster_slim_failed;
    end if;
    -- Track cluster pairs we cannot slim so we don't retry the same corridor forever.
    create temporary table mesh_route_cluster_slim_failed (
        source_id integer not null,
        target_id integer not null,
        reason text,
        primary key (source_id, target_id)
    ) on commit preserve rows;

    if to_regclass('pg_temp.mesh_route_cluster_slim_path') is not null then
        drop table mesh_route_cluster_slim_path;
    end if;
    -- Store the currently evaluated corridor geometry for spacing calculations.
    create temporary table mesh_route_cluster_slim_path (
        seq integer,
        h3 h3index,
        centroid_geog public.geography
    ) on commit preserve rows;

    <<slim_loop>>
    loop
        truncate mesh_route_cluster_slim_path;

        select
            e.source_id,
            e.target_id,
            e.source_h3,
            e.target_h3,
            e.cluster_hops,
            e.distance_m,
            e.distance_m / nullif(e.cluster_hops, 0) as average_hop_length
        into candidate
        from mesh_visibility_edges e
        where e.cluster_hops > hop_limit
          and not exists (
                select 1
                from mesh_route_cluster_slim_failed f
                where f.source_id = e.source_id
                  and f.target_id = e.target_id
            )
        order by
            average_hop_length asc,
            e.cluster_hops desc,
            e.distance_m desc,
            e.source_id,
            e.target_id
        limit 1;

        if candidate.source_id is null then
            exit slim_loop;
        end if;

        -- Build a pgRouting blocklist for nodes already violating tower spacing so the corridor ignores them.
        select array_agg(mrn.node_id)
        into blocked_node_ids
        from mesh_route_nodes mrn
        join mesh_surface_h3_r8 surface on surface.h3 = mrn.h3
        where mrn.h3 not in (candidate.source_h3, candidate.target_h3)
          and (
                surface.has_tower is true
                or (
                    surface.distance_to_closest_tower is not null
                    and surface.distance_to_closest_tower < separation
                )
            );

        insert into mesh_route_cluster_slim_path (seq, h3, centroid_geog)
        select
            corridor.seq,
            corridor.h3,
            surface.centroid_geog
        from mesh_route_corridor_between_towers(candidate.source_h3, candidate.target_h3, blocked_node_ids) corridor
        join mesh_surface_h3_r8 surface on surface.h3 = corridor.h3
        order by corridor.seq;

        get diagnostics path_node_count = ROW_COUNT;

        if path_node_count = 0 then
            raise notice 'Cluster slim skip % -> %: no routing corridor available',
                candidate.source_id,
                candidate.target_id;
            insert into mesh_route_cluster_slim_failed (source_id, target_id, reason)
            values (candidate.source_id, candidate.target_id, 'no routing corridor')
            on conflict (source_id, target_id) do update set reason = excluded.reason;
            continue;
        end if;

        expected_hops := path_node_count + 1;

        if expected_hops >= candidate.cluster_hops then
            raise notice 'Cluster slim skip % -> %: corridor still needs % hops (current % hops)',
                candidate.source_id,
                candidate.target_id,
                expected_hops,
                candidate.cluster_hops;
            insert into mesh_route_cluster_slim_failed (source_id, target_id, reason)
            values (
                candidate.source_id,
                candidate.target_id,
                format('corridor still needs %s hops (current %s)', expected_hops, candidate.cluster_hops)
            )
            on conflict (source_id, target_id) do update set reason = excluded.reason;
            continue;
        elsif expected_hops > hop_limit then
            raise notice 'Cluster slim skip % -> %: corridor needs % hops which exceeds hop limit %',
                candidate.source_id,
                candidate.target_id,
                expected_hops,
                hop_limit;
            insert into mesh_route_cluster_slim_failed (source_id, target_id, reason)
            values (
                candidate.source_id,
                candidate.target_id,
                format('corridor still needs %s hops which exceeds hop limit %s', expected_hops, hop_limit)
            )
            on conflict (source_id, target_id) do update set reason = excluded.reason;
            continue;
        end if;

        promoted_count := 0;

        for new_h3, new_centroid in
            select path.h3, path.centroid_geog
            from mesh_route_cluster_slim_path path
            order by path.seq
        loop
            select has_tower
            into has_existing
            from mesh_surface_h3_r8
            where h3 = new_h3;

            if has_existing is true then
                continue;
            end if;

            insert into mesh_towers (h3, source)
            values (new_h3, 'cluster_slim')
            on conflict (h3) do nothing;

            if not found then
                continue;
            end if;

            promoted_count := promoted_count + 1;

            update mesh_surface_h3_r8
            set has_tower = true,
                clearance = null,
                path_loss = null,
                visible_uncovered_population = 0,
                distance_to_closest_tower = 0
            where h3 = new_h3;

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

            perform mesh_surface_refresh_visible_tower_counts(
                new_h3,
                refresh_radius,
                max_distance
            );

            perform mesh_surface_refresh_reception_metrics(
                new_h3,
                refresh_radius,
                max_distance
            );
        end loop;

        call mesh_visibility_edges_refresh();

        raise notice 'Slimmed cluster edge % -> %: % -> % hops using % tower(s) (%.1f km)',
            candidate.source_id,
            candidate.target_id,
            candidate.cluster_hops,
            expected_hops,
            promoted_count,
            candidate.distance_m / 1000.0;
    end loop;

    if to_regclass('pg_temp.mesh_route_cluster_slim_failed') is not null then
        drop table mesh_route_cluster_slim_failed;
    end if;
    if to_regclass('pg_temp.mesh_route_cluster_slim_path') is not null then
        drop table mesh_route_cluster_slim_path;
    end if;

end;
$$;
