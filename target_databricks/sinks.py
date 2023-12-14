"""databricks target sink class, which handles writing streams."""

from __future__ import annotations

import typing as t
from datetime import datetime
from urllib.parse import urlparse

from boto3 import Session
from singer_sdk.sinks import SQLSink

from target_databricks.connector import databricksConnector
from target_databricks.stage.s3_parquet import s3ParquetStage

if t.TYPE_CHECKING:
    from singer_sdk import PluginBase


def parse_s3_uri(s3_uri):
    # Add a default scheme if it's missing
    if not s3_uri.startswith("s3://"):
        s3_uri = "s3://" + s3_uri

    parsed_uri = urlparse(s3_uri)

    # Check if the scheme is 's3'
    if parsed_uri.scheme == "s3":
        # Extract bucket name and object key (prefix)
        bucket_name = parsed_uri.netloc
        object_key = parsed_uri.path.lstrip("/")

        return bucket_name, object_key
    else:
        raise ValueError("Invalid S3 URI. Scheme must be 's3'.")


class databricksSink(SQLSink):
    """databricks target sink class."""

    connector_class = databricksConnector

    MAX_SIZE_DEFAULT = 100000

    def __init__(  # noqa: PLR0913
        self,
        target: PluginBase,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ) -> None:
        """Initialize Snowflake Sink."""
        self.target = target
        self.aws_session = None
        super().__init__(
            target=target,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )

    @property
    def schema_name(self) -> str | None:
        schema = super().schema_name or self.config.get("schema")
        return schema

    @property
    def database_name(self) -> str | None:
        db = super().database_name or self.config.get("catalog")
        return db

    @property
    def table_name(self) -> str:
        return super().table_name

    def activate_version(self, new_version: int) -> None:
        """
        Not handling table version manuipulation as
        we prefer merging schema. This handling is done by
        in SQLConnector class.
        """
        self.logger.warning(
            "Got activate version message but not doing any \
                            version manipulation, if needed implement at sink level."
        )

    def conform_name(
        self,
        name: str,
        object_type: str | None = None,  # noqa: ARG002
    ) -> str:
        """Conform a stream property name to one suitable for the target system.

        Currently just passing throught the name as it is but this method can be
        altered to change the name of column/table as suited.

        Args:
            name: Property name.
            object_type: One of ``database``, ``schema``, ``table`` or ``column``.


        Returns:
            The name as is it.
        """
        return name

    def setup(self) -> None:
        """Set up Sink.

        This method is called on Sink creation,
        and creates the Table entities in
        the target database.
        """
        try:
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=self.schema,
                primary_keys=self.key_properties,
            )
        except Exception:
            (
                self.logger.exception(
                    "Error creating %s in  %s",
                    self.full_table_name,
                    self.schema_name,
                ),
            )
            raise

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: t.Iterable[dict[str, t.Any]],
    ) -> int | None:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        if self.config.get("include_process_date", False):
            records = self.append_process_date(records)

        if self.aws_session is None:
            self.aws_session = Session(
                aws_access_key_id=self.config.get("aws_access_key_id", None),
                aws_secret_access_key=self.config.get("aws_secret_access_key", None),
                aws_session_token=self.config.get("aws_session_token", None),
                region_name=self.config.get("aws_region"),
                profile_name=self.config.get("aws_profile_name", None),
            )

        stager = s3ParquetStage(
            target_name=self.target.name,
            aws_access_key_id=self.aws_session.get_credentials().access_key,
            aws_secret_access_key=self.aws_session.get_credentials().secret_key,
            aws_session_token=self.aws_session.get_credentials().token,
            region_name=self.aws_session.region_name,
            bucket=self.config.get("bucket"),
            prefix=self.config.get("prefix"),
            full_table_name=self.full_table_name,
            include_process_date=True,
        )
        source_refenrce, s3_loc = stager.get_batch_file(records=records, schema=schema)

        self.insert_batch_file_via_stage(
            full_table_name=full_table_name,
            source_refrence=source_refenrce,
            s3_loc=s3_loc,
        )
        return len(records) if isinstance(records, list) else None

    def append_process_date(self, records) -> dict:
        """A function that appends the current UTC to every record"""

        def process_date(record):
            record["_PROCESS_DATE"] = datetime.utcnow()
            return record

        return list(map(lambda x: process_date(x), records))

    def insert_batch_file_via_stage(
        self,
        full_table_name: str,
        source_refrence: str,
        s3_loc: str,
    ) -> None:
        """Process a batch file with the given batch context.

        Args:
            encoding: The batch file encoding.
            files: The batch files to process.
        """
        self.logger.info("Processing batch of files.")
        try:
            if self.key_properties:
                # merge into destination table
                self.connector.merge_from_stage(
                    full_table_name=full_table_name,
                    schema=self.schema,
                    source_refrence=source_refrence,
                    key_properties=self.key_properties,
                )
            else:
                # append into destination table
                self.connector.append_from_stage(
                    full_table_name=full_table_name,
                    schema=self.schema,
                    source_refrence=source_refrence,
                )

        finally:
            # clean up local files
            if self.config.get("clean_up_staged_files"):
                self.logger.info("Cleaning up after batch processing")
                # s3 remove source_refrence
                s3 = self.aws_session.client("s3")
                try:
                    # List objects in the specified bucket and prefix
                    bucket_name, key = parse_s3_uri(s3_uri=s3_loc)
                    s3.delete_object(Bucket=bucket_name, Key=key)
                    self.logger.info("All objects deleted successfully")
                except Exception as e:
                    self.logger.error(
                        f"{e} => No objects found in the specified bucket and prefix",
                        exc_info=True,
                    )
