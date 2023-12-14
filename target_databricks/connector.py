from __future__ import annotations

from operator import contains, eq
from typing import TYPE_CHECKING, Any, Iterable, Sequence, cast

import sqlalchemy
from singer_sdk import typing as th
from singer_sdk.connectors import SQLConnector
from sqlalchemy.sql import text
from databricks.sqlalchemy._types import TIMESTAMP_NTZ, DatabricksStringType

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class TypeMap:
    def __init__(self, operator, map_value, match_value=None) -> None:  # noqa: ANN001
        self.operator = operator
        self.map_value = map_value
        self.match_value = match_value

    def match(self, compare_value):  # noqa: ANN001
        try:
            if self.match_value:
                return self.operator(compare_value, self.match_value)
            return self.operator(compare_value)
        except TypeError:
            return False

def evaluate_typemaps(type_maps, compare_value, unmatched_value):  # noqa: ANN001
    for type_map in type_maps:
        if type_map.match(compare_value):
            return type_map.map_value
    return unmatched_value

class databricksConnector(SQLConnector):
    """Databricks Target Connector.

    This class handles all DDL and type conversions.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.table_cache: dict = {}
        self.schema_cache: dict = {}
        super().__init__(*args, **kwargs)

    
    def get_table_columns(
        self,
        full_table_name: str,
        column_names: list[str] | None = None,
    ) -> dict[str, sqlalchemy.Column]:
        
        """Return a list of table columns.

        Args:
            full_table_name: Fully qualified table name.
            column_names: A list of column names to filter to.

        Returns:
            An ordered list of column objects.
        """
        if full_table_name in self.table_cache:
            return self.table_cache[full_table_name]
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        inspector = sqlalchemy.inspect(self._engine)
        columns = inspector.get_columns(table_name, schema_name)

        parsed_columns = {
            col_meta["name"]: sqlalchemy.Column(
                col_meta["name"],
                self._convert_type(col_meta["type"]),
                nullable=col_meta.get("nullable", False),
            )
            for col_meta in columns
            if not column_names or col_meta["name"].casefold() in {col.casefold() for col in column_names}
        }
        self.table_cache[full_table_name] = parsed_columns
        return parsed_columns
    
    def get_sqlalchemy_url(self) -> str:
        """Generates a SQLAlchemy URL for databricks.

        Args:
            config: The configuration for the connector.
        """

        host = self.config["host"]
        access_token = self.config["access_token"]
        http_path = self.config["http_path"]
        catalog = self.config["catalog"]

        connect_url = f"databricks://token:{access_token}@{host}?http_path={http_path}&catalog={catalog}"

        return connect_url
    
    def create_engine(self) -> Engine:
        """Creates and returns a new engine. Do not call outside of _engine.

        NOTE: Do not call this method. The only place that this method should
        be called is inside the self._engine method. If you'd like to access
        the engine on a connector, use self._engine.

        This method exists solely so that tap/target developers can override it
        on their subclass of SQLConnector to perform custom engine creation
        logic.

        Returns:
            A new SQLAlchemy Engine.
        """
        if "http_path" not in self.config:
            raise ValueError("http_path missing in the engine config")
        

        engine = sqlalchemy.create_engine(
            self.get_sqlalchemy_url(),
            echo=False,
            connect_args={"session_configuration":{"spark.databricks.delta.schema.autoMerge.enabled":"true"}}
        )

        # TODO: add schema check
        # db_names = [db[1] for db in engine.connection.execute(text("SHOW DATABASES;")).fetchall()]
        # if self.config["database"] not in db_names:
        #     msg = f"Database '{self.config['database']}' does not exist or the user/role doesn't have access to it."
        #     raise Exception(msg)  # noqa: TRY002
        return engine
    
    def prepare_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str],
        partition_keys: list[str] | None = None,
    ) -> None:
        """This is to responsible to create a table,
        with minimum schema (primary and partition) keys.
        It is expected by the target that user has schema evolution available.

        If more complex adaptation are required, where the plugin is expected to 
        handle the schema change via alter commands, check alter ddl in 
        target snowflake.


        Args:
            full_table_name: the target table name.
            schema: the JSON Schema for the table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.
        """
        if not self.table_exists(full_table_name=full_table_name):
            self.logger.info(f"{full_table_name} doesn't exists, creating..")
            self.create_empty_table(
                full_table_name=full_table_name,
                schema=schema,
                primary_keys=primary_keys,
                partition_keys=partition_keys
                )
            return
    
    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        Uses custom Snowflake types from [snowflake-sqlalchemy](https://github.com/snowflakedb/snowflake-sqlalchemy/blob/main/src/snowflake/sqlalchemy/custom_types.py)

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        
        # target_type = SQLConnector.to_sql_type(jsonschema_type)
        target_type = 'string'   # to avoid varchar using hand-written schema
        # snowflake max and default varchar length
        # https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
        # maxlength = jsonschema_type.get("maxLength", SNOWFLAKE_MAX_STRING_LENGTH)
        # define type maps

        string_submaps = [
            TypeMap(eq, TIMESTAMP_NTZ(), "date-time"),
            TypeMap(contains, sqlalchemy.types.TIME(), "time"),
            TypeMap(eq, sqlalchemy.types.DATE(), "date"),
            TypeMap(eq, DatabricksStringType(), None),
        ]
        type_maps = [
            TypeMap(th._jsonschema_type_check, sqlalchemy.types.Integer, ("integer",)),  # noqa: SLF001
            TypeMap(th._jsonschema_type_check, 'string', ("object",)),  # noqa: SLF001
            TypeMap(th._jsonschema_type_check, 'string', ("array",)),  # noqa: SLF001
            TypeMap(th._jsonschema_type_check, sqlalchemy.types.Float, ("number",)),  # noqa: SLF001
            TypeMap(th._jsonschema_type_check, 'string', ("string",))
        ]

        # apply type maps
        if th._jsonschema_type_check(jsonschema_type, ("string",)):  # noqa: SLF001
            datelike_type = th.get_datelike_property_type(jsonschema_type)
            if datelike_type:
                target_type = evaluate_typemaps(string_submaps, datelike_type, target_type)
            else:
                target_type = evaluate_typemaps(type_maps, jsonschema_type, target_type)
        else:
            target_type = evaluate_typemaps(type_maps, jsonschema_type, target_type)

        return cast(sqlalchemy.types.TypeEngine, target_type)
    
    def schema_exists(self, schema_name: str) -> bool:
        if schema_name in self.schema_cache:
            return True
        schema_names = sqlalchemy.inspect(self._engine).get_schema_names()
        self.schema_cache = schema_names
        return schema_name in schema_names
    
    def _get_create_table_statement(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: str,
        partition_keys: Iterable[str]
    ):
        """Get databricks create table statement."""
        create_table_block = "CREATE TABLE IF NOT EXISTS"
        column_specification_list = []

        try:
            properties: dict = schema["properties"]
        except KeyError as e:
            msg = f"Schema for '{full_table_name}' does not define properties: {schema}"
            raise RuntimeError(msg) from e
        
        for property_name, property_jsonschema in properties.items():
            self.logger.info(property_name)
            if property_name in primary_keys:
                pk = f"{property_name} {self.to_sql_type(property_jsonschema)}"
                column_specification_list.append(pk)
            
            if partition_keys:
                if property_name in partition_keys:
                    partition = (property_name,
                        self.to_sql_type(property_jsonschema))
                    column_specification_list.append(partition)

        column_specification_block = "(" + ",".join(column_specification_list) +")"
        partition_columns = ",".join(partition_keys) if partition_keys else None
        partition_block = f"PARTITIONED BY {partition_columns}" if partition_columns else ""

        create_statetment = f"""{create_table_block} {full_table_name} 
                {column_specification_block}
                {partition_block}"""
        
        return (text(create_statetment) , {})

    def _get_merge_from_stage_statement(  # noqa: ANN202, PLR0913
        self,
        full_table_name: str,
        source_refrence: str,
        key_properties: Iterable[str],
    ):
        """Get databricks MERGE statement."""

        join_expr = " and ".join(
            [f"d.{key} <=> s.{key}" for key in key_properties],
        )

        dedup_cols = ", ".join(list(key_properties)) if key_properties else None
        dedup_order_by = dedup_cols
        
        if self.config.get("include_process_date"):
            dedup_order_by = "_PROCESS_DATE"

        dedup = f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {dedup_cols} ORDER BY {dedup_order_by}) = 1" if dedup_cols else None
        return (
            text(
                f"merge into {full_table_name} d using "  # noqa: S608, ISC003
                + f"(select * from {source_refrence} "  # noqa: S608
                + f"{dedup}) s "
                + f"on {join_expr} "
                + f"when matched then update set * "
                + f"when not matched then insert * "
            ),
            {},
        )
    
    def _get_append_statement(
            self, 
            full_table_name, 
            source_refrence):  # noqa: ANN202, ANN001
        """Get databricks append statement."""
        
        return (
            text(
                f"merge into {full_table_name} d using "  # noqa: S608, ISC003
                + f"(select * from {source_refrence} "  # noqa: S608
                + f"on 1=0 "
                + f"when matched then update set * "
                + f"when not matched then insert * "
            ),
            {},
        )

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
    ) -> None:
        
        create_statement, kwargs = self._get_create_table_statement(
                full_table_name=full_table_name,
                schema=schema,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
        )
        with self._connect() as conn:
            self.logger.info("Creating empty table: %s", create_statement)
            conn.execute(create_statement, **kwargs)

    def merge_from_stage(  # noqa: PLR0913
        self,
        full_table_name: str,
        schema: dict,
        source_refrence: str,
        key_properties: Sequence[str],
    ):
        """Merge data from a stage (s3) into a table.

        Args:
            sync_id: The sync ID for the batch.
            schema: The schema of the data.
            key_properties: The primary key properties of the data.
        """
        
        with self._connect() as conn:
            merge_statement, kwargs = self._get_merge_from_stage_statement(
                full_table_name=full_table_name,
                source_refrence=source_refrence,
                key_properties=key_properties,
            )
            self.logger.info("Merging with SQL: %s", merge_statement)
            conn.execute(merge_statement, **kwargs)
    
    def append_from_stage(
        self,
        full_table_name: str,
        schema: dict,
        source_refrence: str,
    ):
        """Copy data from a stage into a table.

        Args:
            full_table_name: The fully-qualified name of the table.
            schema: The schema of the data.
            sync_id: The sync ID for the batch.
            file_format: The name of the file format.
        """
        with self._connect() as conn:
            appened_statement, kwargs = self._get_append_statement(
                full_table_name=full_table_name,
                source_refrence=source_refrence,
                schema=schema,
            )
            self.logger.info("Appending with SQL: %s", appened_statement)
            conn.execute(appened_statement, **kwargs)