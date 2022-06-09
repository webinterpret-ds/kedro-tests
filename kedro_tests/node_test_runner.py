from typing import Text

from kedro.pipeline import node
from kedro.runner import run_node

from kedro_tests.base_test_runner import BaseTestRunner


class NodeTestRunner(BaseTestRunner):
    # TODO: add docs and example
    node_name: Text

    @property
    def node(self) -> node:
        return self.pipeline._nodes_by_name[self.node_name]

    def get_memory_dataset_names(self):
        """
        Get node datasets undefined in the data catalog
        """
        node_datasets = self.node.inputs + self.node.outputs

        return [
            name for name in node_datasets
            if name not in self.catalog_config and not name.startswith("params:")
        ]

    def test_node(self) -> None:
        self.catalog = self._setup_catalog()
        run_node(self.node, self.catalog)

        self.run_asserts()

    def run_asserts(self) -> None:
        ...
