set client_min_messages = warning;

drop function if exists mesh_route_corridor_between_towers(h3index, h3index);
drop function if exists mesh_route_corridor_between_towers(h3index, h3index, integer[]);
-- Recover intermediate routing nodes between two towers using the cached pgRouting graph.
create or replace function mesh_route_corridor_between_towers(
        source_h3 h3index,
        target_h3 h3index,
        blocked_nodes integer[] default null
    )
    returns table (seq integer, h3 h3index)
    language plpgsql
    volatile
as
$$
declare
    start_node integer;
    end_node integer;
    separation constant double precision := 5000;
begin
    select node_id
    into start_node
    from mesh_route_nodes
    where mesh_route_nodes.h3 = source_h3;

    select node_id
    into end_node
    from mesh_route_nodes
    where mesh_route_nodes.h3 = target_h3;

    if start_node is null or end_node is null then
        return;
    end if;

    if to_regclass('pg_temp.mesh_route_blocked_nodes') is null then
        create temporary table mesh_route_blocked_nodes (
            node_id integer primary key
        ) on commit drop;
    else
        truncate mesh_route_blocked_nodes;
    end if;

    insert into mesh_route_blocked_nodes (node_id)
    select mrn.node_id
    from mesh_route_nodes mrn
    join mesh_surface_h3_r8 surface on surface.h3 = mrn.h3
    where mrn.node_id not in (start_node, end_node)
      and surface.centroid_geog is not null
      and exists (
            select 1
            from mesh_towers mt
            where mt.h3 not in (source_h3, target_h3)
              and ST_DWithin(surface.centroid_geog, mt.centroid_geog, separation)
        );

    if blocked_nodes is not null then
        insert into mesh_route_blocked_nodes (node_id)
        select unnest(blocked_nodes)
        on conflict (node_id) do nothing;
    end if;

    return query
    with path_vertices as (
        -- Run pgRouting across the cached LOS graph to recover the minimum-cost corridor.
        select *
        from pgr_dijkstra(
            'select edge_id as id, source, target, cost, reverse_cost
             from mesh_route_edges
             where source not in (select node_id from mesh_route_blocked_nodes)
               and target not in (select node_id from mesh_route_blocked_nodes)',
            start_node,
            end_node,
            false
        )
        where node <> -1
        order by seq
    ),
    ordered_nodes as (
        -- Attach H3 cells for each traversed vertex, skipping the endpoints because they already host towers.
        select
            row_number() over (order by pv.seq)::integer as seq,
            mrn.h3
        from path_vertices pv
        join mesh_route_nodes mrn on mrn.node_id = pv.node
        where mrn.h3 not in (source_h3, target_h3)
        order by pv.seq
    )
    select ordered_nodes.seq, ordered_nodes.h3 from ordered_nodes;
end;
$$;
