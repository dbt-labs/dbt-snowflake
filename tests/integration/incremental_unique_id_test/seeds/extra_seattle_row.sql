-- insert new row, which should be in incremental model with
--  first three columns unique
insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('WA','King','Seattle','2022-02-10');

insert into {schema}.seed
    (state, county, city, last_visit_date)
values ('WA','King','Seattle','2022-02-10');
