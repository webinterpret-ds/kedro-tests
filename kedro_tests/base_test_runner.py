from abc import (
    ABC,
    abstractmethod,
)
from functools import singledispatchmethod
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Text,
    Union,
)

import pandas as pd
from kedro.io import (
    DataCatalog,
    MemoryDataSet,
)
from kedro.pipeline import Pipeline

from kedro_tests.utils import get_parameters


class BaseTestRunner(ABC):
    create_pipeline: Callable[[], Pipeline]
    parameters: Dict[Text, Union[Text, Any]] = {}
    catalog_config: Dict[Text, Any] = {}
    datasets: Dict[Text, Any] = {}
    catalog: Any = None

    @property
    def pipeline(self) -> Pipeline:
        return self.create_pipeline()

    def _setup_catalog(self) -> DataCatalog:
        catalog = DataCatalog.from_config(self.catalog_config)
        catalog.add_feed_dict(get_parameters(self.parameters, parent_key="params", sep='.'))

        for name in self.get_memory_dataset_names():
            catalog.add(name, MemoryDataSet())

        for name, dataset in self.datasets.items():
            catalog.save(name, dataset)

        return catalog

    @singledispatchmethod
    def assert_datasets(self, data: Any, name: str) -> None:
        assert self.catalog.load(name) == data

    @assert_datasets.register(pd.DataFrame)
    def _(self, data: pd.DataFrame, name: str):
        result = self.catalog.load(name)

        result.reset_index(drop=["index"], inplace=True)
        data.reset_index(drop=True, inplace=True)

        assert set(result.columns) == set(data.columns)
        assert result.equals(data[result.columns])

    @abstractmethod
    def get_memory_dataset_names(self) -> List:
        ...
