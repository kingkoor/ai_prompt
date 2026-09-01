"""
NetSuite SuiteAnalytics Connect (JDBC) connector.

Uses jaydebeapi / JPype1 with the OpenAccess driver. JARs are loaded into a
Python-side JVM (jpype.startJVM), which works on serverless compute where
Spark's ADD JAR is blocked.

Credentials (username, password, account ID, role ID) come from Azure Key
Vault - no secrets in code.

Performance notes
-----------------
Rows are read straight off the JDBC ResultSet with getString() rather than
through jaydebeapi's cursor. Everything is stringified for bronze anyway, so
this skips a per-cell type-dispatch layer on the hot path.

setFetchSize controls how many rows the driver pulls per network round trip.
The driver default is small; on a multi-million-row table that is the single
biggest cost. Raise FETCH_SIZE before tuning anything else.

Timestamp columns are cast in Spark (columnar, cheap). They must be real
timestamps in bronze: watermark.py derives the incremental watermark from
MAX(incremental_column) and branches on the column TYPE, so a STRING date
column would produce a lexicographic max and silently break incremental loads.

Parallel reads
--------------
Pass key= and workers=N to split the key space into N ranges, each read on its
own JDBC connection in its own thread. Threads help because JNI calls release
the GIL while waiting on the network. The ceiling is NetSuite's licensed
SuiteAnalytics Connect concurrent-connection limit, not this code - if N
exceeds it the extra workers fail to connect. Start at 2-4.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import jaydebeapi
import jpype
from azure.keyvault.secrets import SecretClient
from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, expr
from pyspark.sql.types import StructType, StructField, StringType

_ENV_CONFIG = {
    "prod_jdbc_netsuite": {
        "account_id_secret": "prod-netsuite-account-id",
        "role_id_secret": "prod-netsuite-role-id",
        "username_secret": "prod-netsuite-jdbc-user",
        "password_secret": "prod-netsuite-jdbc-password",
        "port": 1708,
        "driver": "com.netsuite.jdbc.openaccess.OpenAccessDriver",
        "jars": [
            "/Volumes/bbdatawarehouse_dev/bronze/jdbc_drivers/NQjc.jar",
            "/Volumes/bbdatawarehouse_dev/bronze/jdbc_drivers/json-20231013.jar",
        ],
    }
}

KEY_VAULT_URL = "https://bb-databricks-keyvault.vault.azure.net/"
SERVICE_CREDENTIAL = "dataops_dbaccess"

FETCH_SIZE = 10_000      # JDBC rows per network round trip
BATCH_ROWS = 100_000     # rows accumulated before each Delta flush
_LOG = "[jdbc_netsuite]"

_TIMESTAMP_FORMATS = ("M/d/yyyy h:mm a", "M/d/yyyy")


def _guess_timestamp_columns(columns: list[str]) -> list[str]:
    """Identify likely timestamp columns based on column names."""
    return [c for c in columns if "date" in c.lower()]


class NetSuiteJdbcConnector:
    """JDBC connector for NetSuite via jaydebeapi (serverless-compatible)."""

    def __init__(
        self,
        environment: str = "prod_jdbc_netsuite",
        spark: Optional[SparkSession] = None,
    ) -> None:
        if environment not in _ENV_CONFIG:
            raise ValueError(
                f"Unknown environment '{environment}'. "
                f"Valid options: {list(_ENV_CONFIG.keys())}"
            )
        self._env = _ENV_CONFIG[environment]
        self._spark = spark or SparkSession.getActiveSession()
        if self._spark is None:
            raise RuntimeError("No active SparkSession was found.")

        self._account_id, self._role_id, self._username, self._password = (
            self._load_secrets(environment)
        )
        acct = f"acct{self._account_id}".lower().replace("_", "-")
        self.host = f"{acct}.connect.api.netsuite.com"
        self._conn = None
        self._lock = threading.Lock()
        print(f"{_LOG} Initialized | Account: {self._account_id} | Host: {self.host}")

    # ------------------------------------------------------------------
    # Azure Key Vault
    # ------------------------------------------------------------------

    @staticmethod
    def _load_secrets(environment: str) -> tuple[str, str, str, str]:
        """Retrieve account ID, role ID, username, and password from Key Vault."""
        env = _ENV_CONFIG[environment]
        credential = dbutils.credentials.getServiceCredentialsProvider(SERVICE_CREDENTIAL)
        kv = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)

        values = {
            name: kv.get_secret(env[f"{name}_secret"]).value
            for name in ("account_id", "role_id", "username", "password")
        }
        missing = [k for k, v in values.items() if not v]
        if missing:
            raise RuntimeError(
                f"Key Vault returned empty {', '.join(missing)} for '{environment}'."
            )
        return (
            values["account_id"], values["role_id"],
            values["username"], values["password"],
        )

    # ------------------------------------------------------------------
    # JDBC connection (jaydebeapi / JPype1)
    # ------------------------------------------------------------------

    @property
    def jdbc_url(self) -> str:
        """Build the NetSuite-specific JDBC connection URL."""
        return (
            f"jdbc:ns://{self.host}:{self._env['port']};"
            f"ServerDataSource=NetSuite2.com;"
            f"Encrypted=1;"
            f"NegotiateSSLClose=false;"
            f"TCPKeepAlive=true;"
            f"CustomProperties=(AccountID=ACCT{self._account_id};"
            f"RoleID={self._role_id};Uppercase=1)"
        )

    def _open(self):
        """Open a NEW JDBC connection. Each parallel worker needs its own."""
        with self._lock:
            # startJVM is not thread-safe and can only run once per process.
            if not jpype.isJVMStarted():
                jpype.startJVM(classpath=self._env["jars"])
        return jaydebeapi.connect(
            self._env["driver"],
            self.jdbc_url,
            [self._username, self._password],
        )

    def connect(self):
        """Open (or reuse) the shared connection used by serial reads."""
        if self._conn is None:
            self._conn = self._open()
        return self._conn

    def close(self) -> None:
        """Close the shared JDBC connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def test_connection(self) -> bool:
        """Quick connectivity check."""
        try:
            conn = self.connect()
            stmt = conn.jconn.createStatement()
            try:
                rs = stmt.executeQuery("SELECT 1 AS ok FROM DUAL")
                return rs.next()
            finally:
                stmt.close()
        except Exception as exc:
            print(f"{_LOG} Connection test failed: {exc}")
            return False

    # ------------------------------------------------------------------
    # Row reading
    # ------------------------------------------------------------------

    @staticmethod
    def _columns(rs) -> list[str]:
        meta = rs.getMetaData()
        return [str(meta.getColumnLabel(i)) for i in range(1, meta.getColumnCount() + 1)]

    def _read_batches(self, conn, sql: str, fetch_size: int):
        """
        Yield (columns, rows) batches straight off the JDBC ResultSet.

        setFetchSize is the reason this uses a raw Statement instead of a
        jaydebeapi cursor - jaydebeapi builds the statement inside execute()
        and never exposes it, so the fetch size can't be set through it.
        """
        stmt = conn.jconn.createStatement()
        stmt.setFetchSize(fetch_size)
        try:
            rs = stmt.executeQuery(sql)
            columns = self._columns(rs)
            width = range(1, len(columns) + 1)

            batch, yielded = [], False
            while rs.next():
                batch.append(tuple(rs.getString(i) for i in width))
                if len(batch) >= BATCH_ROWS:
                    yield columns, batch
                    batch, yielded = [], True
            # Yield at least once so the caller always learns the schema.
            if batch or not yielded:
                yield columns, batch
        finally:
            stmt.close()

    def _to_dataframe(
        self,
        rows: list,
        columns: list[str],
        timestamp_columns: Optional[list[str]] = None,
    ) -> DataFrame:
        """
        Convert raw JDBC rows to a Spark DataFrame.

        Values arrive as strings from getString(). Timestamp columns are then
        cast with format fallbacks - these are columnar Spark expressions, so
        they cost far less than the per-cell JNI reads above.
        """
        schema = StructType([StructField(c, StringType(), True) for c in columns])
        df = self._spark.createDataFrame(rows, schema=schema)

        if not timestamp_columns:
            return df

        existing = {c.lower() for c in columns}
        transforms = {}
        for c in timestamp_columns:
            if c.lower() not in existing:
                continue
            attempts = [
                expr(f"try_to_timestamp(`{c}`, '{f}')") for f in _TIMESTAMP_FORMATS
            ]
            attempts.append(expr(f"try_to_timestamp(`{c}`)"))
            transforms[c] = coalesce(*attempts)
        return df.withColumns(transforms) if transforms else df

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def read_query(self, sql: str, fetch_size: int = FETCH_SIZE) -> DataFrame:
        """Execute sql and return a Spark DataFrame. Small results only."""
        conn = self.connect()
        columns, rows = [], []
        for columns, batch in self._read_batches(conn, sql, fetch_size):
            rows.extend(batch)
        if not columns:
            return self._spark.createDataFrame([], schema=StructType([]))
        return self._to_dataframe(rows, columns, _guess_timestamp_columns(columns))

    def read_table(
        self,
        table: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
    ) -> DataFrame:
        """Read a table with optional column selection and filtering."""
        selected = ", ".join(columns) if columns else "*"
        sql = f"SELECT {selected} FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return self.read_query(sql)

    def query(
        self,
        sql: str,
        staging_table: str,
        key: Optional[str] = None,
        timestamp_columns: Optional[list[str]] = None,
        fetch_size: int = FETCH_SIZE,
        workers: int = 1,
        **kwargs,
    ) -> DataFrame:
        """
        Execute a JDBC query and write results to a staging Delta table.

        Matches the interface of the SuiteQL NetSuite connector so the extract
        framework can call either connector interchangeably.

        Parameters
        ----------
        sql : str
            SELECT statement - the framework adds watermark predicates for
            incremental loads.
        staging_table : str
            Fully qualified Delta table to stage results into.
        key : str, optional
            Numeric, unique column used to split the read into ranges.
            Required when workers > 1; ignored otherwise.
        timestamp_columns : list[str], optional
            Columns to cast to TimestampType. Auto-detected from column names
            containing 'date' when not provided.
        fetch_size : int
            JDBC rows per network round trip.
        workers : int
            Parallel JDBC connections. Capped by the NetSuite SuiteAnalytics
            Connect concurrent-connection licence.
        """
        if not staging_table:
            raise ValueError(
                "staging_table is required. "
                "Returning large results directly can exhaust driver memory."
            )
        if workers > 1 and not key:
            raise ValueError("workers > 1 requires key= (a numeric, unique column).")

        print(f"{_LOG} Query: {sql}")
        started = time.time()

        if workers > 1:
            total, columns = self._parallel(
                sql, staging_table, key, timestamp_columns, fetch_size, workers
            )
        else:
            total, columns = self._serial(
                sql, staging_table, timestamp_columns, fetch_size
            )

        elapsed = time.time() - started
        rate = total / elapsed if elapsed > 0 else 0
        print(f"{_LOG} Complete: {total:,} rows -> {staging_table} "
              f"| {elapsed:.1f}s | {rate:,.0f} rows/s")

        if total == 0:
            schema = StructType([StructField(c, StringType(), True) for c in columns])
            return self._spark.createDataFrame([], schema=schema)
        return self._spark.table(staging_table)

    def _write(self, rows, columns, timestamp_columns, staging_table, first: bool) -> None:
        """Flush one batch to the staging table. Schema is merged only once."""
        df = self._to_dataframe(rows, columns, timestamp_columns)
        writer = df.write.format("delta").mode("overwrite" if first else "append")
        if first:
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(staging_table)

    def _serial(self, sql, staging_table, timestamp_columns, fetch_size):
        conn = self.connect()
        total, columns, first = 0, [], True
        for columns, rows in self._read_batches(conn, sql, fetch_size):
            timestamp_columns = timestamp_columns or _guess_timestamp_columns(columns)
            self._write(rows, columns, timestamp_columns, staging_table, first)
            first = False
            total += len(rows)
            print(f"{_LOG} Flush: {len(rows):,} rows | total={total:,}")
        return total, columns

    def _parallel(self, sql, staging_table, key, timestamp_columns, fetch_size, workers):
        """Split the key space into ranges and read each on its own connection."""
        lo, hi = self._key_bounds(sql, key)
        if lo is None:
            return 0, self._empty_columns(sql)

        # Create the table up front so every worker can simply append.
        columns = self._empty_columns(sql)
        timestamp_columns = timestamp_columns or _guess_timestamp_columns(columns)
        (self._to_dataframe([], columns, timestamp_columns)
             .write.format("delta").mode("overwrite")
             .option("overwriteSchema", "true").saveAsTable(staging_table))

        step = max(1, (hi - lo + 1) // workers)
        ranges = [(lo + i * step, lo + (i + 1) * step if i < workers - 1 else hi + 1)
                  for i in range(workers)]
        print(f"{_LOG} Parallel: {workers} workers over {key} [{lo}, {hi}]")

        def run(bounds):
            start, end = bounds
            ranged = (f'SELECT * FROM ({sql}) t '
                      f'WHERE t."{key}" >= {start} AND t."{key}" < {end}')
            conn = self._open()
            count = 0
            try:
                for cols, rows in self._read_batches(conn, ranged, fetch_size):
                    # Delta append+append never conflicts, so no lock is needed.
                    self._to_dataframe(rows, cols, timestamp_columns) \
                        .write.format("delta").mode("append").saveAsTable(staging_table)
                    count += len(rows)
                    print(f"{_LOG} [{start}-{end}] {count:,} rows")
            finally:
                conn.close()
            return count

        total = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(run, r) for r in ranges]
            for future in as_completed(futures):
                total += future.result()
        return total, columns

    def _key_bounds(self, sql: str, key: str):
        """MIN/MAX of the split key, so ranges cover only rows that exist."""
        conn = self.connect()
        stmt = conn.jconn.createStatement()
        try:
            rs = stmt.executeQuery(
                f'SELECT MIN(t."{key}") AS lo, MAX(t."{key}") AS hi '
                f'FROM ({sql}) t'
            )
            if not rs.next():
                return None, None
            lo, hi = rs.getString(1), rs.getString(2)
            return (int(lo), int(hi)) if lo and hi else (None, None)
        finally:
            stmt.close()

    def _empty_columns(self, sql: str) -> list[str]:
        """Column names only - runs the query with a false predicate."""
        conn = self.connect()
        stmt = conn.jconn.createStatement()
        try:
            rs = stmt.executeQuery(f"SELECT * FROM ({sql}) t WHERE 1=0")
            return self._columns(rs)
        finally:
            stmt.close()
