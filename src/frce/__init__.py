from __future__ import annotations

import os
import sys
from datetime import date, datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

try:
    from pyspark.errors import PySparkValueError
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except Exception:
    SparkSession = None
else:
    _original_create_dataframe = SparkSession.createDataFrame

    def _type_for_value(value):
        if isinstance(value, bool):
            return BooleanType()
        if isinstance(value, int):
            return LongType()
        if isinstance(value, float):
            return DoubleType()
        if isinstance(value, datetime):
            return TimestampType()
        if isinstance(value, date):
            return DateType()
        return StringType()

    def _schema_from_named_rows(data, names: list[str]) -> StructType:
        fields = []
        rows = list(data)
        for index, name in enumerate(names):
            value = next((row[index] for row in rows if row[index] is not None), None)
            fields.append(StructField(name, _type_for_value(value), True))
        return StructType(fields)

    def _is_cannot_determine_type(exc: PySparkValueError) -> bool:
        for method_name in ("getCondition", "getErrorClass"):
            method = getattr(exc, method_name, None)
            if callable(method) and method() == "CANNOT_DETERMINE_TYPE":
                return True
        return False

    def _create_dataframe_with_nulltype_fallback(self, data=None, schema=None, *args, **kwargs):
        try:
            return _original_create_dataframe(self, data, schema, *args, **kwargs)
        except PySparkValueError as exc:
            if (
                _is_cannot_determine_type(exc)
                and isinstance(schema, list)
                and data is not None
            ):
                rows = list(data)
                return _original_create_dataframe(
                    self,
                    rows,
                    _schema_from_named_rows(rows, schema),
                    *args,
                    **kwargs,
                )
            raise

    if not getattr(SparkSession.createDataFrame, "_frce_nulltype_patch", False):
        _create_dataframe_with_nulltype_fallback._frce_nulltype_patch = True
        SparkSession.createDataFrame = _create_dataframe_with_nulltype_fallback

__all__ = ["__version__"]

__version__ = "1.0.0"
