# - VERY rudimentary test script to run latest + specific branch image builds and test them all by running `--version`
# TODO: create a real test suite

clear \
&& echo "\n\n"\
"########################################\n"\
"##### Testing dbt-snowflake latest #####\n"\
"########################################\n"\
&& docker build --tag dbt-snowflake \
  --target dbt-snowflake \
  docker \
&& docker run dbt-snowflake --version \
\
&& echo "\n\n"\
"#########################################\n"\
"##### Testing dbt-snowflake-1.0.0b1 #####\n"\
"#########################################\n"\
&& docker build --tag dbt-snowflake-1.0.0b1 \
  --target dbt-snowflake \
  --build-arg commit_ref=v1.0.0b1 \
  docker \
&& docker run dbt-snowflake-1.0.0b1 --version
