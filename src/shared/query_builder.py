"""SQLAlchemy-based query builder for safe, parameterized SQL generation.

Supports multiple database dialects (PostgreSQL, MySQL, etc.) and provides
SQL injection protection through proper identifier quoting and value parameterization.
"""

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import Column, MetaData, Table, and_, asc, desc, select, text
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.engine import Dialect as SADialect
from sqlalchemy.sql import Select

logger = logging.getLogger(__name__)


class FilterOperator(Enum):
    """Supported filter operators."""
    EQ = "="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    IN = "in"
    NOT_IN = "not_in"
    LIKE = "like"
    ILIKE = "ilike"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class Filter:
    """Structured filter definition."""
    field: str
    op: str
    value: Any = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Filter":
        """Create Filter from dictionary."""
        return cls(
            field=data["field"],
            op=data.get("op", "="),
            value=data.get("value")
        )


@dataclass
class QueryConfig:
    """Configuration for query building."""
    schema_name: Optional[str] = None
    table_name: str = ""
    columns: List[str] = None
    filters: List[Filter] = None
    cursor_field: Optional[str] = None
    cursor_value: Optional[Any] = None
    cursor_mode: str = "inclusive"
    order_by: Optional[str] = None
    order_direction: str = "asc"
    limit: Optional[int] = None
    offset: Optional[int] = None

    def __post_init__(self):
        if self.columns is None:
            self.columns = ["*"]
        if self.filters is None:
            self.filters = []


def _get_sqlalchemy_dialect(dialect: str) -> SADialect:
    """Get SQLAlchemy dialect instance from dialect string.

    Args:
        dialect: Database dialect string from config (e.g., 'postgresql', 'postgres', 'mysql')

    Returns:
        SQLAlchemy dialect instance
    """
    dialect_lower = dialect.lower()

    if dialect_lower in ("postgresql", "postgres"):
        return postgresql.dialect()
    elif dialect_lower in ("mysql", "mariadb"):
        return mysql.dialect()
    else:
        raise ValueError(f"Unsupported dialect: {dialect}")


def _is_postgresql_dialect(dialect: str) -> bool:
    """Check if dialect is PostgreSQL (includes 'postgres' alias)."""
    return dialect.lower() in ("postgresql", "postgres")


class QueryBuilder:
    """
    SQLAlchemy-based query builder for generating safe, parameterized SQL.

    Supports PostgreSQL, MySQL, and can be extended for other dialects.
    All identifiers are properly quoted and all values are parameterized.
    """

    # Map string operator to FilterOperator enum
    OPERATOR_MAP = {
        "=": FilterOperator.EQ,
        "==": FilterOperator.EQ,
        "eq": FilterOperator.EQ,
        "!=": FilterOperator.NE,
        "<>": FilterOperator.NE,
        "ne": FilterOperator.NE,
        ">": FilterOperator.GT,
        "gt": FilterOperator.GT,
        ">=": FilterOperator.GTE,
        "gte": FilterOperator.GTE,
        "<": FilterOperator.LT,
        "lt": FilterOperator.LT,
        "<=": FilterOperator.LTE,
        "lte": FilterOperator.LTE,
        "in": FilterOperator.IN,
        "not_in": FilterOperator.NOT_IN,
        "like": FilterOperator.LIKE,
        "ilike": FilterOperator.ILIKE,
        "is_null": FilterOperator.IS_NULL,
        "is_not_null": FilterOperator.IS_NOT_NULL,
    }

    def __init__(self, dialect: str):
        """Initialize query builder with specified dialect.

        Args:
            dialect: Database dialect string from config (e.g., 'postgresql', 'mysql')
        """
        self.dialect = dialect
        self._sa_dialect = _get_sqlalchemy_dialect(dialect)

    def build_select_query(self, config: QueryConfig) -> Tuple[str, List[Any]]:
        """Build a SELECT query from configuration.

        Args:
            config: Query configuration

        Returns:
            Tuple of (query_string, params_list)
        """
        # Create table reference with proper schema
        metadata = MetaData()
        table = Table(
            config.table_name,
            metadata,
            schema=config.schema_name,
        )

        # Build column list
        if config.columns == ["*"] or not config.columns:
            query = select(text("*")).select_from(table)
        else:
            columns = [Column(col) for col in config.columns]
            query = select(*columns).select_from(table)

        # Collect parameters
        params = []

        # Apply filters
        conditions = []
        for filter_def in config.filters:
            condition, filter_params = self._build_filter_condition(
                filter_def, len(params)
            )
            if condition is not None:
                conditions.append(condition)
                params.extend(filter_params)

        # Apply cursor-based filtering for incremental reads
        if config.cursor_field and config.cursor_value is not None:
            cursor_condition, cursor_params = self._build_cursor_condition(
                config.cursor_field,
                config.cursor_value,
                config.cursor_mode,
                len(params)
            )
            conditions.append(cursor_condition)
            params.extend(cursor_params)

        if conditions:
            query = query.where(and_(*conditions))

        # Apply ordering
        order_field = config.order_by or config.cursor_field
        if order_field:
            order_col = Column(order_field)
            if config.order_direction.lower() == "desc":
                query = query.order_by(desc(order_col))
            else:
                query = query.order_by(asc(order_col))

        # Apply limit/offset
        if config.limit is not None:
            query = query.limit(config.limit)
        if config.offset is not None:
            query = query.offset(config.offset)

        # Compile to dialect-specific SQL
        return self._compile_query(query, params)

    def _build_filter_condition(
        self, filter_def: Filter, param_offset: int
    ) -> Tuple[Optional[Any], List[Any]]:
        """Build a single filter condition.

        Args:
            filter_def: Filter definition
            param_offset: Current parameter count for placeholder numbering

        Returns:
            Tuple of (SQLAlchemy condition, list of parameter values)
        """
        field = filter_def.field
        op_str = filter_def.op.lower() if isinstance(filter_def.op, str) else filter_def.op
        value = filter_def.value

        # Map string operator to enum
        op = self.OPERATOR_MAP.get(op_str)
        if op is None:
            logger.warning(f"Unknown filter operator: {op_str}, skipping filter")
            return None, []

        col = Column(field)

        # Build condition based on operator
        if op == FilterOperator.IS_NULL:
            return col.is_(None), []
        elif op == FilterOperator.IS_NOT_NULL:
            return col.isnot(None), []
        elif op == FilterOperator.IN:
            if not isinstance(value, (list, tuple)):
                value = [value]
            return col.in_(value), list(value)
        elif op == FilterOperator.NOT_IN:
            if not isinstance(value, (list, tuple)):
                value = [value]
            return col.notin_(value), list(value)
        elif op == FilterOperator.LIKE:
            return col.like(value), [value]
        elif op == FilterOperator.ILIKE:
            # ILIKE is PostgreSQL-specific; for MySQL use LIKE with LOWER()
            if _is_postgresql_dialect(self.dialect):
                return col.ilike(value), [value]
            else:
                from sqlalchemy import func
                return func.lower(col).like(func.lower(value)), [value]
        elif op == FilterOperator.EQ:
            return col == value, [value]
        elif op == FilterOperator.NE:
            return col != value, [value]
        elif op == FilterOperator.GT:
            return col > value, [value]
        elif op == FilterOperator.GTE:
            return col >= value, [value]
        elif op == FilterOperator.LT:
            return col < value, [value]
        elif op == FilterOperator.LTE:
            return col <= value, [value]

        return None, []

    def _build_cursor_condition(
        self,
        cursor_field: str,
        cursor_value: Any,
        cursor_mode: str,
        param_offset: int
    ) -> Tuple[Any, List[Any]]:
        """Build cursor-based condition for incremental reads.

        Args:
            cursor_field: Field to use for cursor
            cursor_value: Current cursor value
            cursor_mode: 'inclusive' (>=) or 'exclusive' (>)
            param_offset: Current parameter count

        Returns:
            Tuple of (SQLAlchemy condition, list of parameter values)
        """
        col = Column(cursor_field)

        if cursor_mode == "inclusive":
            return col >= cursor_value, [cursor_value]
        else:
            return col > cursor_value, [cursor_value]

    def _compile_query(
        self, query: Select, params: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Compile SQLAlchemy query to string with parameters.

        Args:
            query: SQLAlchemy Select object
            params: List of parameter values

        Returns:
            Tuple of (query_string, params_list)
        """
        # Compile with literal_binds=False to get parameterized query
        compiled = query.compile(
            dialect=self._sa_dialect,
            compile_kwargs={"literal_binds": False}
        )

        query_str = str(compiled)

        # For PostgreSQL, convert :param_N to $N format for asyncpg
        if _is_postgresql_dialect(self.dialect):
            query_str, params = self._convert_to_positional_params(
                query_str, compiled.params, params
            )

        logger.debug(f"Compiled query: {query_str}")
        logger.debug(f"Parameters: {params}")

        return query_str, params

    def _convert_to_positional_params(
        self,
        query_str: str,
        compiled_params: Dict[str, Any],
        explicit_params: List[Any]
    ) -> Tuple[str, List[Any]]:
        """Convert named parameters to positional ($1, $2) for asyncpg.

        Args:
            query_str: Query with named parameters
            compiled_params: Parameters from SQLAlchemy compilation
            explicit_params: Explicitly provided parameter values

        Returns:
            Tuple of (query_string with $N placeholders, ordered params list)
        """
        # If we have explicit params, they're already in order
        if explicit_params:
            param_index = 0

            def replace_param(match):
                nonlocal param_index
                param_index += 1
                return f"${param_index}"

            # Match :param_name patterns
            query_str = re.sub(r':[\w_]+', replace_param, query_str)
            return query_str, explicit_params

        # Otherwise extract from compiled params
        ordered_params = []
        param_mapping = {}

        for i, (name, value) in enumerate(compiled_params.items(), 1):
            param_mapping[f":{name}"] = f"${i}"
            ordered_params.append(value)

        for old, new in param_mapping.items():
            query_str = query_str.replace(old, new)

        return query_str, ordered_params


def build_select_query(
    dialect: str,
    schema_name: Optional[str],
    table_name: str,
    config: Dict[str, Any],
    cursor_value: Optional[Any] = None
) -> Tuple[str, List[Any]]:
    """Convenience function to build a SELECT query.

    Args:
        dialect: Database dialect string from config (e.g., 'postgresql', 'postgres', 'mysql')
        schema_name: Database schema name
        table_name: Table name
        config: Query configuration dictionary containing:
            - columns: List of column names or ["*"]
            - filters: List of filter dicts with field, op, value
            - cursor_field: Field for incremental cursor
            - cursor_value: Current cursor value (can also be passed directly)
            - cursor_mode: 'inclusive' or 'exclusive'
            - order_by: Field to order by
            - order_direction: 'asc' or 'desc'
            - limit: Max rows to return
            - offset: Rows to skip
        cursor_value: Override cursor value from config

    Returns:
        Tuple of (query_string, params_list)
    """
    builder = QueryBuilder(dialect)

    # Parse filters from config
    filters = []
    for f in config.get("filters", []):
        if isinstance(f, Filter):
            filters.append(f)
        elif isinstance(f, dict):
            filters.append(Filter.from_dict(f))

    # Build query config
    query_config = QueryConfig(
        schema_name=schema_name,
        table_name=table_name,
        columns=config.get("columns", ["*"]),
        filters=filters,
        cursor_field=config.get("cursor_field"),
        cursor_value=cursor_value if cursor_value is not None else config.get("cursor_value"),
        cursor_mode=config.get("cursor_mode", "inclusive"),
        order_by=config.get("order_by"),
        order_direction=config.get("order_direction", "asc"),
        limit=config.get("limit"),
        offset=config.get("offset"),
    )

    return builder.build_select_query(query_config)