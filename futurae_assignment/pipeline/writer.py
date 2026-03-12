import apache_beam as beam
import pyarrow as pa
from apache_beam.io.parquetio import WriteToParquet


class WriteParquet(beam.PTransform):
    def __init__(self, path: str, schema: pa.Schema):
        super().__init__()
        self._path = path
        self._schema = schema

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (
            pcoll
            | "AsDict" >> beam.Map(lambda e: e._asdict())
            | "Write"
            >> WriteToParquet(
                self._path, schema=self._schema, file_name_suffix=".parquet"
            )
        )
