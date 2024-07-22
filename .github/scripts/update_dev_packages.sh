#!/bin/bash -e
set -e


adapters_git_branch=$1
core_git_branch=$2
target_req_file="pyproject.toml"
core_req_sed_pattern="s|dbt-core.git.*#subdirectory=core|dbt-core.git@${core_git_branch}#subdirectory=core|g"
adapters_req_sed_pattern="s|dbt-adapters.git|dbt-adapters.git@${adapters_git_branch}|g"
if [[ "$OSTYPE" == darwin* ]]; then
 # mac ships with a different version of sed that requires a delimiter arg
 sed -i "" "$core_req_sed_pattern" $target_req_file
 sed -i "" "$adapters_req_sed_pattern" $target_req_file
else
 sed -i "$core_req_sed_pattern" $target_req_file
 sed -i "$adapters_req_sed_pattern" $target_req_file
fi
