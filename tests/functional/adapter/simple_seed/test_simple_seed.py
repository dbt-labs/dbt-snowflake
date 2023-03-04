import csv
import pytest
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase

from pathlib import Path
from dbt.tests.util import (
    mkdir,
    rm_dir,
    run_dbt,
    read_file
)

class TestSimpleBigSeedBatched(SeedConfigBase):
    @staticmethod
    def _make_big_seed(test_data_dir):
        mkdir(test_data_dir)
        big_seed_path = test_data_dir / Path("tmp.csv")
        with open(big_seed_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow(["seed_id"])
            for i in range(0, 20000):
                writer.writerow([i])
        return big_seed_path

    @pytest.fixture(scope="class")
    def seeds(self, test_data_dir):
        big_seed_path = self._make_big_seed(test_data_dir)
        big_seed = read_file(big_seed_path)
        yield {
            "big_batched_seed.csv": big_seed
        }
        rm_dir(test_data_dir)

    # TODO: FileExistsError ./data already exists
    def test_big_batched_seed(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 1
