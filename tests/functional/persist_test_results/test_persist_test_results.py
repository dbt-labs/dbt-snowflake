from typing import Dict, Set, Tuple

from dbt.tests.adapter.persist_test_results.basic import PersistTestResults

from tests.functional.persist_test_results.utils import (
    delete_record,
    get_relation_summary_in_schema,
    insert_record,
)


class TestPersistTestResults(PersistTestResults):
    def get_audit_relation_summary(self, project) -> Set[Tuple]:
        return get_relation_summary_in_schema(project, self.audit_schema)

    def insert_record(self, project, record: Dict[str, str]):
        insert_record(project, project.test_schema, self.model_table, record)

    def delete_record(self, project, record: Dict[str, str]):
        delete_record(project, project.test_schema, self.model_table, record)
