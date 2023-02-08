#
# Seed queries
#

drop_seed_table_query = """
drop table if exists {database}.{schema}.seed cascade;
""".lstrip()

create_seed_table_query = """
create table {database}.{schema}.seed ( 
    id BIGSERIAL PRIMARY KEY,
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	email VARCHAR(50),
	gender VARCHAR(50),
	ip_address VARCHAR(20)
);
""".lstrip()

drop_agg_table_query = """
drop table if exists {database}.{schema}.agg cascade;
"""

create_agg_table_query = """
create table {database}.{schema}.agg (
	last_name VARCHAR(50),
	count BIGINT
);
""".lstrip()

insert_into_seed_query = """
insert into {database}.{schema}.seed (first_name, last_name, email, gender, ip_address) values
('Jack', 'Hunter', 'jhunter0@pbs.org', 'Male', '59.80.20.168'),
('Kathryn', 'Walker', 'kwalker1@ezinearticles.com', 'Female', '194.121.179.35'),
('Gerald', 'Ryan', 'gryan2@com.com', 'Male', '11.3.212.243');
""".lstrip()

insert_into_agg_query ="""
insert into {database}.{schema}.agg (last_name, count) values
('Hunter', 2), ('Walker', 2), ('Ryan', 2);
""".lstrip()

seed_queries = [
    drop_seed_table_query,
    create_seed_table_query,
    drop_agg_table_query,
    create_agg_table_query,
    insert_into_seed_query,
    insert_into_agg_query
]
