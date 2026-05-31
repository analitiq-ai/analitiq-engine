"""SQLAlchemy-based query builder for safe, parameterized SQL generation.

Supports multiple database dialects (PostgreSQL, MySQL, etc.) and provides
SQL injection protection through proper identifier quoting and value parameterization.
"""

import importlib
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from sqlalchemy import (
    Column,
    MetaData,
    Table,
    and_,
    asc,
    desc,
    literal_column,
    select,
    text,
)
from sqlalchemy.dialects import mssql, mysql, sqlite
from sqlalchemy.dialects.postgresql.asyncpg import dialect as _asyncpg_dialect
from sqlalchemy.engine import Dialect as SADialect
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import quoted_name


# Parameters returned by build_select_query: positional list for
# paramstyles that bind by index (qmark, format, numeric, numeric_dollar)
# and a name->value dict for named paramstyles (named, pyformat).
# Snowflake / BigQuery dialects compile to named/pyformat by default;
# their drivers consume dicts.
ParamsLike = Union[List[Any], Dict[str, Any]]

logger = logging.getLogger(__name__)


def _positional_params(
    positiontup: List[str], bind_params: Dict[str, Any]
) -> List[Any]:
    """Map an ordered positional bind-name tuple to its values.

    ``positiontup`` is SA's ordered list of bind names for a positional
    paramstyle; a name can repeat (MSSQL ROW_NUMBER pagination reuses
    ``param_1``), so iterating it (not the dict) preserves the right count and
    order. The BigQuery dialect tags each entry with its bind type
    (``status_1:STRING``) while ``bind_params`` is keyed by the bare name, so
    fall back to the prefix before the ``:`` when the tagged name misses. SA
    sanitizes bind names to ``[A-Za-z0-9_]``, so a real name never contains a
    ``:`` and the fallback cannot collide.
    """
    return [
        bind_params[name if name in bind_params else name.split(":", 1)[0]]
        for name in positiontup
    ]


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


# Third-party SQLAlchemy dialects loaded on demand. Each entry names the
# importable module that calling ``import_module`` triggers the dialect
# registration side effect for, so we can resolve ``dialect()`` after.
_LAZY_DIALECT_PACKAGES: Dict[str, str] = {
    "snowflake": "snowflake.sqlalchemy",
    "bigquery": "sqlalchemy_bigquery",
    "redshift": "sqlalchemy_redshift.dialect",
    "duckdb": "duckdb_engine",
    "clickhouse": "clickhouse_sqlalchemy",
}


# Built-in SQLAlchemy dialect factories. ``postgresql``/``postgres`` and
# ``mysql``/``mariadb`` are aliases — same SA dialect underneath.
#
# PostgreSQL uses the ``asyncpg``-specific dialect so compiled SQL ships
# with ``$N`` placeholders (asyncpg's native paramstyle). The default
# ``postgresql.dialect()`` would emit ``%(name)s`` and require a manual
# conversion pass that drifts out of sync whenever SQLAlchemy adds new
# bound parameters (limit / offset being the obvious case).
_BUILTIN_DIALECT_FACTORIES: Dict[str, Callable[[], SADialect]] = {
    "postgresql": _asyncpg_dialect,
    "postgres": _asyncpg_dialect,
    "mysql": mysql.dialect,
    "mariadb": mysql.dialect,
    "mssql": mssql.dialect,
    "sqlite": sqlite.dialect,
}


def _get_sqlalchemy_dialect(
    dialect: str, paramstyle: Optional[str] = None
) -> SADialect:
    """Resolve a dialect string to a SQLAlchemy dialect instance.

    Built-in dialects (postgresql, mysql, mssql, sqlite) resolve directly.
    Third-party dialects (snowflake, bigquery, redshift, duckdb,
    clickhouse) are loaded by importing the package that registers them
    with SQLAlchemy's dialect registry; this lets the engine compile SQL
    for those dialects without forcing the package into the base install.

    ``paramstyle`` forces the dialect's bind-parameter style at
    construction (so ``dialect.positional`` is set consistently). The
    ADBC-only path passes ``"qmark"`` because every ADBC driver binds
    ``?`` placeholders. For PostgreSQL a forced paramstyle also swaps the
    asyncpg dialect for the default (non-asyncpg) PostgreSQL dialect:
    asyncpg renders inline bind casts (``?::INTEGER``) that only the
    asyncpg driver understands, whereas the ADBC libpq driver wants bare
    ``?`` placeholders.

    Raises ``ValueError`` for unknown dialects and ``ImportError`` (with
    actionable text) when a third-party dialect package is missing.
    """
    dialect_lower = dialect.lower()
    kwargs: Dict[str, Any] = {}
    if paramstyle is not None:
        kwargs["paramstyle"] = paramstyle

    factory = _BUILTIN_DIALECT_FACTORIES.get(dialect_lower)
    if factory is not None:
        if paramstyle is not None and _is_postgresql_dialect(dialect_lower):
            from sqlalchemy.dialects import postgresql
            return postgresql.dialect(**kwargs)
        return factory(**kwargs)

    package = _LAZY_DIALECT_PACKAGES.get(dialect_lower)
    if package is None:
        raise ValueError(f"Unsupported dialect: {dialect}")

    try:
        importlib.import_module(package)
    except ImportError as exc:
        raise ImportError(
            f"SQLAlchemy dialect for {dialect!r} requires the "
            f"{package!r} package. Install the matching extra "
            f"(e.g. `poetry install -E {dialect_lower}`)."
        ) from exc

    # The package's import side effect registers the dialect; resolve
    # it via SQLAlchemy's URL machinery so we don't hard-code each
    # third-party dialect's module path.
    from sqlalchemy.dialects import registry
    cls = registry.load(dialect_lower)
    return cls(**kwargs)


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

    def __init__(
        self,
        dialect: str,
        *,
        paramstyle: Optional[str] = None,
        quote_identifiers: bool = False,
        inline_paging: bool = False,
    ):
        """Initialize query builder with specified dialect.

        Args:
            dialect: Database dialect string from config (e.g., 'postgresql', 'mysql').
            paramstyle: Force the dialect's bind-parameter style (the
                ADBC-only path passes ``"qmark"``); ``None`` keeps the
                driver's native style.
            quote_identifiers: Force quoting of every table/column/schema
                name. The ADBC destination quotes all identifiers, so the
                ADBC source must too: Snowflake folds unquoted names to
                upper case and BigQuery treats names case-sensitively, so
                an unquoted name could resolve to a different object than
                the one the destination wrote.
            inline_paging: Render ``LIMIT``/``OFFSET`` as literal integers
                rather than bound parameters. Snowflake rejects bind
                variables in ``LIMIT``/``OFFSET``; filter and cursor
                values stay parameterized.
        """
        self.dialect = dialect
        self._quote_identifiers = quote_identifiers
        self._inline_paging = inline_paging
        self._sa_dialect = _get_sqlalchemy_dialect(dialect, paramstyle=paramstyle)

    def _ident(self, name: str) -> Any:
        """Wrap *name* so SQLAlchemy quotes it when ``quote_identifiers``."""
        return quoted_name(name, quote=True) if self._quote_identifiers else name

    def _paging_value(self, value: int) -> Any:
        """Render a LIMIT/OFFSET value as a literal int or a bound param."""
        if self._inline_paging:
            return literal_column(str(int(value)))
        return value

    def build_select_query(self, config: QueryConfig) -> Tuple[str, ParamsLike]:
        """Build a SELECT query from configuration.

        Returns ``(sql, params)`` where ``params`` is either:

        * a positional ``list`` for dialects whose driver binds by index
          (PG asyncpg / SQLite qmark / MySQL format / MSSQL qmark), or
        * a name->value ``dict`` for dialects whose driver binds by
          name (Snowflake pyformat, BigQuery named, generic ``:foo``
          dialects).

        Callers must dispatch on the returned type before passing to
        ``exec_driver_sql``.
        """
        # Create table reference with proper schema
        metadata = MetaData()
        table = Table(
            self._ident(config.table_name),
            metadata,
            schema=self._ident(config.schema_name) if config.schema_name else None,
        )

        # Build column list
        if config.columns == ["*"] or not config.columns:
            query = select(text("*")).select_from(table)
        else:
            columns = [Column(self._ident(col)) for col in config.columns]
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
            order_col = Column(self._ident(order_field))
            if config.order_direction.lower() == "desc":
                query = query.order_by(desc(order_col))
            else:
                query = query.order_by(asc(order_col))
        elif config.offset is not None and self.dialect.lower() == "mssql":
            # T-SQL refuses OFFSET / FETCH NEXT without ORDER BY.
            # ``ORDER BY (SELECT NULL)`` is Microsoft's documented escape
            # hatch for paginated reads that don't need a stable order.
            query = query.order_by(text("(SELECT NULL)"))

        # Apply limit/offset
        if config.limit is not None:
            query = query.limit(self._paging_value(config.limit))
        if config.offset is not None:
            query = query.offset(self._paging_value(config.offset))

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

        col = Column(self._ident(field))

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
        col = Column(self._ident(cursor_field))

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
        # MSSQL's default dialect compiles to ``:name`` (named paramstyle),
        # but aioodbc / pyodbc DBAPI drivers consume ``?`` (qmark) and
        # ``exec_driver_sql`` bypasses SA's bind translation. Force the
        # MSSQL compile to qmark so the SQL placeholders match what the
        # driver expects. Other dialects already compile to a paramstyle
        # their async driver accepts directly.
        # ``render_postcompile`` expands "expanding" bind parameters
        # (``IN (...)``) into individual placeholders at compile time.
        # Both transports execute the raw compiled string -- the SA path
        # via ``exec_driver_sql`` (which bypasses SA's bind expansion) and
        # the ADBC path via ``cursor.execute`` -- so an unexpanded
        # ``__[POSTCOMPILE_...]`` marker would otherwise reach the driver.
        compile_kwargs: Dict[str, Any] = {
            "literal_binds": False,
            "render_postcompile": True,
        }
        sa_dialect = self._sa_dialect
        if sa_dialect.name == "mssql":
            from sqlalchemy.dialects import mssql as _mssql
            sa_dialect = _mssql.dialect(paramstyle="qmark")

        compiled = query.compile(dialect=sa_dialect, compile_kwargs=compile_kwargs)

        query_str = str(compiled)

        # SA sets ``compiled.positiontup`` only for positional
        # paramstyles (qmark, format, numeric, numeric_dollar). For
        # named/pyformat it's None -- iterating would TypeError.
        # Snowflake / BigQuery dialects fall into the named bucket,
        # so callers must accept the dict form too.
        params: ParamsLike
        if compiled.positiontup is not None:
            params = _positional_params(compiled.positiontup, compiled.params)
        else:
            params = dict(compiled.params)

        logger.debug(f"Compiled query: {query_str}")
        logger.debug(f"Parameters: {params}")

        return query_str, params


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