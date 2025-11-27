set client_min_messages = warning;

drop table if exists mesh_visibility_edges;
-- Create table capturing visibility diagnostics for all towers
create table mesh_visibility_edges (
    source_id integer not null,
    target_id integer not null,
    source_h3 h3index not null,
    target_h3 h3index not null,
    distance_m double precision not null,
    is_visible boolean not null,
    is_between_clusters boolean not null default false,
    geom geometry not null
);

comment on table mesh_visibility_edges is
    'Pairs of towers with their LOS results for quick inspection.';

create index if not exists mesh_visibility_edges_geom_idx on mesh_visibility_edges using gist (geom);
create index if not exists mesh_visibility_edges_source_idx on mesh_visibility_edges using btree (source_id) include (target_id, distance_m, is_visible);
