import typing

import pandas
import pyarrow.parquet


def iterate_parquet(parquet_handle: pyarrow.parquet.ParquetFile,
                    batch_size: int = 65535,
                    columns: list[str] = None) -> typing.Generator[pandas.DataFrame, typing.Any, None]:

    for data_batch in parquet_handle.iter_batches(batch_size=batch_size, columns=columns):
        batch_dataframe: pandas.DataFrame = data_batch.to_pandas()
        yield batch_dataframe
