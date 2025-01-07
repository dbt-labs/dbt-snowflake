from dbt.tests.util import AnyString, AnyInteger


def snowflake_stats():
    return {
        "has_stats": {
            "id": "has_stats",
            "label": "Has Stats?",
            "value": True,
            "description": "Indicates whether there are statistics for this table",
            "include": False,
        },
        "bytes": {
            "id": "bytes",
            "label": "Approximate Size",
            "value": AnyInteger(),
            "description": "Approximate size of the table as reported by Snowflake",
            "include": True,
        },
        "last_modified": {
            "id": "last_modified",
            "label": "Last Modified",
            "value": AnyString(),
            "description": "The timestamp for last update/change",
            "include": True,
        },
        "row_count": {
            "id": "row_count",
            "label": "Row Count",
            "value": 1.0,
            "description": "An approximate count of rows in this table",
            "include": True,
        },
    }
