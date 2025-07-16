import pytest
from dbt.adapters.snowflake.relation import SnowflakeRelation


@pytest.fixture
def iceberg_config() -> dict:
    """Fixture providing standard Iceberg configuration."""
    return {
        "schema": "my_schema",
        "identifier": "my_table",
        "external_volume": "s3_iceberg_snow",
        "base_location_root": "root_path",
        "base_location_subpath": "subpath",
    }


def get_actual_base_location(config: dict[str, str]) -> str:
    """Get the actual base location from the configuration by parsing the DDL predicates."""

    relation = SnowflakeRelation.create(
        schema=config["schema"],
        identifier=config["identifier"],
    )

    actual_ddl_predicates = relation.get_iceberg_ddl_options(config).strip()
    actual_base_location = actual_ddl_predicates.split("base_location = ")[1]

    return actual_base_location


def test_iceberg_path_and_subpath(iceberg_config: dict[str, str]):
    """Test when base_location_root and base_location_subpath are provided"""
    expected_base_location = (
        f"'{iceberg_config['base_location_root']}/"
        f"{iceberg_config['schema']}/"
        f"{iceberg_config['identifier']}/"
        f"{iceberg_config['base_location_subpath']}'"
    ).strip()

    assert get_actual_base_location(iceberg_config) == expected_base_location


def test_iceberg_only_subpath(iceberg_config: dict[str, str]):
    """Test when only base_location_subpath is provided"""
    del iceberg_config["base_location_root"]

    expected_base_location = (
        f"'_dbt/"
        f"{iceberg_config['schema']}/"
        f"{iceberg_config['identifier']}/"
        f"{iceberg_config['base_location_subpath']}'"
    ).strip()

    assert get_actual_base_location(iceberg_config) == expected_base_location


def test_iceberg_only_path(iceberg_config: dict[str, str]):
    """Test when only base_location_root is provided"""
    del iceberg_config["base_location_subpath"]

    expected_base_location = (
        f"'{iceberg_config['base_location_root']}/"
        f"{iceberg_config['schema']}/"
        f"{iceberg_config['identifier']}'"
    ).strip()

    assert get_actual_base_location(iceberg_config) == expected_base_location


def test_iceberg_no_path(iceberg_config: dict[str, str]):
    """Test when no base_location_root or is base_location_subpath provided"""
    del iceberg_config["base_location_root"]
    del iceberg_config["base_location_subpath"]

    expected_base_location = (
        f"'_dbt/" f"{iceberg_config['schema']}/" f"{iceberg_config['identifier']}'"
    ).strip()

    assert get_actual_base_location(iceberg_config) == expected_base_location
