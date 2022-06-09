from kedro.runner import SequentialRunner

from kedro_tests.base_test_runner import BaseTestRunner


class PipelineTestRunner(BaseTestRunner):
    # TODO: add docs and example

    def get_memory_dataset_names(self):
        """
        Get pipeline datasets undefined in the data catalog
        """
        pipeline_datasets = set.union(self.pipeline.inputs(), self.pipeline.outputs())

        return [
            name for name in pipeline_datasets
            if name not in self.catalog_config and not name.startswith("params:")
        ]

    def test_pipeline(self) -> None:
        self.catalog = self._setup_catalog()

        runner = SequentialRunner()
        runner.run(self.pipeline, self.catalog)

        self.run_asserts()

    def run_asserts(self) -> None:
        ...
