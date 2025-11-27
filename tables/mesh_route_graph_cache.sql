set client_min_messages = warning;

drop table if exists mesh_route_graph_cache;
-- Cache pgRouting linework per unique tower pair so repeated lookups skip recomputation
create table mesh_route_graph_cache (
    source_h3 h3index not null,
    target_h3 h3index not null,
    geom geometry(LineString, 4326) not null,
    created_at timestamptz default now(),
    primary key (source_h3, target_h3)
);

comment on table mesh_route_graph_cache is
    'Stores canonical tower-pair routing geometries so mesh_visibility_invisible_route_geom() can reuse them.';
