set client_min_messages = warning;

-- Create metadata table for pipeline settings
create table if not exists mesh_pipeline_settings (
    setting text primary key,
    value text not null,
    updated_at timestamptz not null default now()
);

insert into mesh_pipeline_settings (setting, value)
values
    ('h3_res', '8')
on conflict (setting) do update
set value = excluded.value,
    updated_at = now();
