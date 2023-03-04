import csv
from pathlib import Path

import pytest

from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from dbt.tests.util import (
    mkdir,
    rm_dir,
    run_dbt,
    read_file
)


class TestSimpleBigSeedBatched(SeedConfigBase):

    @pytest.fixture(scope="class")
    def seeds(self):
        seed_data = ["seed_id"]
        seed_data.extend([str(i) for i in range(20_000)])
        return {"big_batched_seed.csv": "\n".join(seed_data)}

    def test_big_batched_seed(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1
