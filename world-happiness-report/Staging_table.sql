drop table if exists staging_world_happiness;

create table if not exists staging_world_happiness(
  country varchar(128),
  region varchar(256),
  happiness_score float,
  year int
);