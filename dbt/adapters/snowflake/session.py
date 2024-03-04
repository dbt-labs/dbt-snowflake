import json
from typing import Dict, List

from dbt.adapters.clients.tags import get_relation_tags
from dbt.adapters.contracts.metadata import RelationTag


def _is_json_loadable(value: str):
    try:
        json.loads(value)
        return True
    except json.decoder.JSONDecodeError:
        return False


def _update_query_tag_from_str(new_tag_value: str, existing_tag: str) -> str:
    if existing_tag:
        existing_tag_dict = json.loads(existing_tag)
    else:
        existing_tag_dict = {}
    if _is_json_loadable(new_tag_value):
        new_tag_value_dict = json.loads(new_tag_value)
        existing_tag_dict.update(new_tag_value_dict)
    else:
        # we need to assign a default key if user has not supplied a dict
        existing_tag_dict["dbt"] = new_tag_value
    return json.dumps(existing_tag_dict)


def _update_query_tag_from_list(new_tag_values: List[RelationTag], existing_tag: str) -> str:
    new_tag_value_dict = {rel_tag.name: rel_tag.value for rel_tag in new_tag_values}
    if existing_tag:
        existing_tag_dict = json.loads(existing_tag)
        existing_tag_dict.update(new_tag_value_dict)
        return json.dumps(existing_tag_dict)

    return json.dumps(new_tag_value_dict)


def build_sessions_params(user_defined_tags: str) -> Dict[str, str]:
    session_parameters = {}

    relation_tags = get_relation_tags()
    query_tag = ""
    if user_defined_tags and relation_tags:
        query_tag = _update_query_tag_from_str(user_defined_tags, query_tag)
        query_tag = _update_query_tag_from_list(relation_tags, query_tag)

    elif user_defined_tags:
        query_tag = user_defined_tags

    elif relation_tags:
        query_tag = _update_query_tag_from_list(relation_tags, query_tag)

    if query_tag:
        session_parameters.update({"QUERY_TAG": query_tag})

    return session_parameters
