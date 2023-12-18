from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import List, Optional, Tuple, Union
from uuid import uuid4

import pyarrow
from pyarrow import Table, fs
from pyarrow.parquet import ParquetWriter


class s3ParquetStage:
    def __init__(
        self,
        target_name: str,
        full_table_name: str,
        region_name: str,
        bucket: str,
        include_process_date: bool,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        prefix: Optional[str] = None,
    ):
        self.file_system = fs.S3FileSystem(
            access_key=aws_access_key_id,
            secret_key=aws_secret_access_key,
            session_token=aws_session_token,
            region=region_name,
        )
        logging.info(f"{target_name}")
        self.bucket = bucket
        self.prefix = prefix if prefix else ""
        self.full_table_name = full_table_name
        self.parquet_schema = None
        self.key = None
        self.include_process_date = include_process_date

        # prefix with target name
        self.prefix = f"{self.prefix}/{target_name}"

    def get_key(self) -> str:
        random_key = f"{int(datetime.now().timestamp())}/{uuid4()}"
        return (
            f"{self.bucket}/{self.prefix}/{self.full_table_name}/{random_key}.parquet"
        )

    @property
    def logger(self) -> logging.Logger:
        """Get logger.

        Returns:
            Plugin logger.
        """
        return logging.getLogger("s3ParquetStage")

    def sanitize(self, value):
        if isinstance(value, dict) and not value:
            # pyarrow can't process empty struct
            return None
        if isinstance(value, str):
            # pyarrow can't process empty struct
            try:
                return value.encode("utf-16", "surrogatepass").decode("utf-16")
            except Exception as e:
                self.logger.warning(
                    f"surrogate encoding failed, serializing string {e}", exc_info=True
                )
                return json.dumps(value)
        return value

    def create_schema(self, schema) -> pyarrow.schema:
        """Generates schema from the records schema present in the tap.
        This is effective way to declare schema instead of relying on pyarrow to
        detect schema type.

        Note: At level 0 (outermost level) any key that is of type datetime in record
        is converted to datetime by base target class. Hence string at level 0 is handled with
        type datetime.

        :return: schema made from stream's schema definition
        :rtype: pyarrow.schema
        """

        def process_anyof_schema(anyOf: List) -> Tuple[List, Union[str, None]]:
            """This function takes in original array of anyOf's schema detected
            and reduces it to the detected schema, based on rules, right now
            just detects whether it is string or not.

            :param anyOf: Multiple types of anyOf schema from original schema
            :type anyOf: List
            :return: Returns final schema detected from multiple anyOf and format
            :rtype: Tuple[List, str|None]
            """
            types, formats = [], []
            for val in anyOf:
                typ = val.get("type")
                if val.get("format"):
                    formats.append(val["format"])
                if type(typ) is not list:
                    types.append(typ)
                else:
                    types.extend(typ)
            types = set(types)
            formats = list(set(formats))
            ret_type = []
            if "string" in types:
                ret_type.append("string")
            if "null" in types:
                ret_type.append("null")
            return ret_type, formats[0] if formats else None

        # TODO: handle non nullable types; by default nullable
        def get_schema_from_array(items: dict, level: int):
            """Returns item schema for an array.

            :param items: items definition of array
            :type items: dict
            :param level: depth level of array in jsonschema
            :type level: int
            :return: detected datatype for all items of array.
            :rtype: pyarrow datatype
            """
            type = items.get("type")
            # if there's anyOf instead of single type
            any_of_types = items.get("anyOf")
            # if the items are objects
            properties = items.get("properties")
            # if the items are an array itself
            items = items.get("items")

            if any_of_types:
                self.logger.info("array with anyof type schema detected.")
                type, _ = process_anyof_schema(anyOf=any_of_types)

            if "integer" in type:
                return pyarrow.int64()
            elif "number" in type:
                return pyarrow.float64()
            elif "string" in type:
                return pyarrow.string()
            elif "boolean" in type:
                return pyarrow.bool_()
            elif "array" in type:
                return pyarrow.list_(get_schema_from_array(items=items, level=level))
            elif "object" in type:
                return pyarrow.struct(
                    get_schema_from_object(properties=properties, level=level + 1)
                )
            else:
                return pyarrow.null()

        def get_schema_from_object(properties: dict, level: int = 0):
            """Returns schema for an object.

            :param properties: properties definition of object
            :type properties: dict
            :param level: depth level of object in jsonschema
            :type level: int
            :return: detected fields for properties in object.
            :rtype: pyarrow datatype
            """
            fields = []
            for key, val in properties.items():
                if "type" in val.keys():
                    type = val["type"]
                    format = val.get("format")
                elif "anyOf" in val.keys():
                    type, format = process_anyof_schema(val["anyOf"])
                else:
                    self.logger.warning("type information not given")
                    type = ["string", "null"]

                if "integer" in type:
                    fields.append(pyarrow.field(key, pyarrow.int64()))
                elif "number" in type:
                    fields.append(pyarrow.field(key, pyarrow.float64()))
                elif "boolean" in type:
                    fields.append(pyarrow.field(key, pyarrow.bool_()))
                elif "string" in type:
                    if format and level == 0:
                        # this is done to handle explicit datetime conversion
                        # which happens only at level 1 of a record
                        if format == "date":
                            fields.append(pyarrow.field(key, pyarrow.date64()))
                        elif format == "time":
                            fields.append(pyarrow.field(key, pyarrow.time64()))
                        else:
                            fields.append(
                                pyarrow.field(key, pyarrow.timestamp("ms", tz="utc"))
                            )
                    else:
                        fields.append(pyarrow.field(key, pyarrow.string()))
                elif "array" in type:
                    items = val.get("items")
                    if items:
                        item_type = get_schema_from_array(items=items, level=level)
                        if item_type == pyarrow.null():
                            self.logger.warn(
                                f"""
                                            key: {key} is defined as list of null, while this would be
                                            correct for list of all null but it is better to define
                                            exact item types for the list, if not null."""
                            )
                        fields.append(pyarrow.field(key, pyarrow.list_(item_type)))
                    else:
                        self.logger.warn(
                            f"""
                                        key: {key} is defined as list of null, while this would be
                                        correct for list of all null but it is better to define
                                        exact item types for the list, if not null."""
                        )
                        fields.append(pyarrow.field(key, pyarrow.list_(pyarrow.null())))
                elif "object" in type:
                    prop = val.get("properties")
                    inner_fields = get_schema_from_object(
                        properties=prop, level=level + 1
                    )
                    if not inner_fields:
                        self.logger.warn(
                            f"""
                                        key: {key} has no fields defined, this may cause
                                        saving parquet failure as parquet doesn't support
                                        empty/null complex types [array, structs] """
                        )
                    fields.append(pyarrow.field(key, pyarrow.struct(inner_fields)))
            return fields

        properties = schema.get("properties")
        parquet_schema = pyarrow.schema(get_schema_from_object(properties=properties))

        # append process_date that is added in format_base
        if self.include_process_date:
            key = "_PROCESS_DATE"
            parquet_schema = parquet_schema.append(
                pyarrow.field(key, pyarrow.timestamp("ms", tz="utc"))
            )

        self.parquet_schema = parquet_schema
        return parquet_schema

    def create_batch(self, records, schema) -> Table:
        """Creates a pyarrow Table object from the record set."""
        try:
            parquet_schema = (
                self.parquet_schema
                if self.parquet_schema
                else self.create_schema(schema=schema)
            )
            fields = set([property.name for property in parquet_schema])
            input = {f: [self.sanitize(row.get(f)) for row in records] for f in fields}

            ret = Table.from_pydict(mapping=input, schema=parquet_schema)

        except Exception as e:
            self.logger.error("Failed to create parquet dataframe.")
            self.logger.error(e)
            raise e

        return ret

    def write_batch(self, records, schema) -> str:
        df = self.create_batch(records, schema)
        try:
            key = self.get_key()
            self.logger.info(f"Writing to s3 at: {key}")
            ParquetWriter(
                key,
                df.schema,
                compression="gzip",
                filesystem=self.file_system,
            ).write_table(df)
            return key
        except Exception as e:
            self.logger.error(e)
            if type(e) is pyarrow.lib.ArrowNotImplementedError:
                self.logger.error(
                    """Failed to write parquet file to S3. Complex types [array, object] in schema cannot be left without type definition """
                )
            else:
                self.logger.error("Failed to write parquet file to S3.")
            raise e

    def get_batch_file(self, records, schema) -> Tuple[str, str]:
        s3_url = self.write_batch(records=records, schema=schema)
        return f"parquet.`s3://{s3_url}`", s3_url
