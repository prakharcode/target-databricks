"""databricks target class."""

from __future__ import annotations

import decimal
import json

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_databricks.sinks import databricksSink


class Targetdatabricks(Target):
    """Sample target for databricks."""

    name = "target-databricks"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="Databricks host for connection",
        ),
        th.Property(
            "access_token",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="Databricks token for connection",
        ),
        th.Property(
            "http_path",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="Databricks http path for connection",
        ),
        th.Property(
            "catalog",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="Databricks catalog for connection",
        ),
        th.Property(
            "aws_access_key_id",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="AWS access key for staging files",
        ),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="AWS secret access key for staging files",
        ),
        th.Property(
            "aws_region",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="AWS region for staging files",
        ),
        th.Property(
            "aws_profile_name",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="AWS profile name for staging files",
        ),
        th.Property(
            "bucket",
            th.StringType,
            secret=False,
            description="AWS s3 bucket for staging files",
        ),
        th.Property(
            "prefix",
            th.StringType,
            secret=False,
            description="AWS s3 bucket prefix for staging files",
        ),
        th.Property(
            "get_schema_from_tap",
            th.BooleanType,
            secret=False,
            description="Boolean to determine schema from tap for staging files",
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            secret=False,
            description="schema name to push the records to, (table name will be stream name by default)",
        ),
        th.Property(
            "clean_up_staged_files",
            th.BooleanType,
            secret=False,
            description="to clean staged files after processing the data",
        ),
        th.Property(
            "include_process_date",
            th.BooleanType,
            secret=False,
            description="to include of a timestamp when the record was processed",
        ),
    ).to_dict()

    default_sink_class = databricksSink

    def deserialize_json(self, line: str) -> dict:
        """Override base target's method to overcome Decimal cast,
        only applied when generating parquet schema from tap schema.

        :param line: serialized record from stream
        :type line: str
        :return: deserialized record
        :rtype: dict
        """
        try:
            get_tap_schema = self.config.get("get_schema_from_tap", False)
            if get_tap_schema:
                return json.loads(line)  # type: ignore[no-any-return]
            else:
                return json.loads(  # type: ignore[no-any-return]
                    line, parse_float=decimal.Decimal
                )
        except json.decoder.JSONDecodeError as exc:
            self.logger.error("Unable to parse:\n%s", line, exc_info=exc)
            raise


if __name__ == "__main__":
    Targetdatabricks.cli()
