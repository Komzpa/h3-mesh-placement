set client_min_messages = warning;

drop table if exists mesh_route_graph_edges;
drop table if exists mesh_route_graph_nodes;

-- Materialize routing nodes for every boundary-approved, road-eligible mesh cell.
create table mesh_route_graph_nodes (
    node_id integer generated always as identity primary key,
    h3 h3index not null unique,
    geom geometry not null,
    geog public.geography not null
);

insert into mesh_route_graph_nodes (h3, geom, geog)
select
    surface.h3,
    surface.centroid_geog::geometry,
    surface.centroid_geog
from mesh_surface_h3_r8 surface
where surface.is_in_boundaries
  and not surface.is_in_unfit_area;

-- Index routing nodes for spatial corridor filtering.
create index if not exists mesh_route_graph_nodes_geom_idx on mesh_route_graph_nodes using gist (geom);

-- Precompute adjacency edges that bake in population/road penalties and a downhill penalty.
create table mesh_route_graph_edges (
    edge_id bigint generated always as identity primary key,
    source_node_id integer not null references mesh_route_graph_nodes(node_id),
    target_node_id integer not null references mesh_route_graph_nodes(node_id),
    cost double precision not null
);

-- Enumerate directed neighbor pairs so we can score each adjacency.
with node_neighbors as (
    select
        src.node_id as source_node_id,
        src.h3 as source_h3,
        dst.node_id as target_node_id,
        dst.h3 as target_h3
    from mesh_route_graph_nodes src
    join lateral (
        select h3_grid_disk(src.h3, 1) as neighbor
    ) n on true
    join mesh_route_graph_nodes dst on dst.h3 = n.neighbor
    where dst.h3 <> src.h3
)
insert into mesh_route_graph_edges (source_node_id, target_node_id, cost)
select
    nn.source_node_id,
    nn.target_node_id,
    1
    + case when coalesce(target_surface.population, 0) = 0 then 1 else 0 end
    + case when not coalesce(target_surface.has_road, false) then 1 else 0 end
    + case
        when source_surface.ele is not null
         and target_surface.ele is not null
         and source_surface.ele > target_surface.ele
        then 1
        else 0
      end
from node_neighbors nn
join mesh_surface_h3_r8 target_surface on target_surface.h3 = nn.target_h3
join mesh_surface_h3_r8 source_surface on source_surface.h3 = nn.source_h3;

-- Index adjacency edges by source node to speed up pgRouting extractions.
create index if not exists mesh_route_graph_edges_source_idx on mesh_route_graph_edges (source_node_id);
create index if not exists mesh_route_graph_edges_target_idx on mesh_route_graph_edges (target_node_id);

truncate mesh_route_graph_cache;
