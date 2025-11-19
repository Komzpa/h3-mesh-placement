set client_min_messages = warning;

drop table if exists mesh_greedy_iterations;
-- Create log table for greedy placement iterations
create table mesh_greedy_iterations (
    iteration integer primary key,
    chosen_h3 h3index not null,
    visible_population numeric not null,
    created_at timestamptz not null default now()
);
