import pytest
from dbt.adapters.contracts.metadata import RelationTag
from dbt.adapters.snowflake import session

scenarios = [
    ["a user defined tag", []],
    [
        "",
        [RelationTag(name="some", value="tag"), RelationTag(name="SomeOther", value="kindOfTag")],
    ],
    ["a user defined tag", [RelationTag(name="some", value="tag")]],
    ["", []],
]


@pytest.mark.parametrize("user_defined_tags, relation_tags", scenarios)
def test_build_session_params(user_defined_tags, relation_tags, monkeypatch):
    monkeypatch.setattr(session, "get_relation_tags", lambda: relation_tags)
    result = session.build_sessions_params(user_defined_tags)
    if user_defined_tags and relation_tags:
        assert result == {"QUERY_TAG": '{"dbt": "a user defined tag", "some": "tag"}'}
    elif user_defined_tags:
        assert result == {"QUERY_TAG": user_defined_tags}
    elif relation_tags:
        assert result == {"QUERY_TAG": '{"some": "tag", "SomeOther": "kindOfTag"}'}
    else:
        assert result == {}
