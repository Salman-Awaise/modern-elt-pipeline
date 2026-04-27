create schema if not exists raw;
create schema if not exists analytics;
create schema if not exists audit;

create table if not exists audit.pipeline_runs (
    run_id text primary key,
    pipeline_name text not null,
    status text not null,
    started_at timestamp not null,
    finished_at timestamp,
    rows_extracted integer default 0,
    rows_loaded integer default 0,
    message text
);
