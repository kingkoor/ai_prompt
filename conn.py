"""Generic Data Vault loaders for hubs, links, and satellites.

All loaders are INSERT-only and idempotent. Bronze is assumed to be
append-only with deduplication handled at read time.

Required tenant columns (ETL_SourceSystem, ETL_Brand, ETL_Region) are
automatically injected into all entities. If a bronze source table is
missing any of them, the loader raises an error before attempting INSERT.

Bronze data is filtered by config.source_system, config.brand, and
config.region so that each loader run processes only the target tenant's data.
"""

from datavault_util.config import VaultConfig, LinkConfig, REQUIRED_TENANT_COLUMNS, REQUIRED_SATELLITE_COLUMNS
from datavault_util.hash_utils import (
    HASH_FUNCTION,
    hash_hub_key,
    hash_composite_key,
    hash_diff,
)

from datavault_util.metadata_utils import (
    get_hash_column_names,
    get_source_primary_key_partition,
)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _validate_bronze_columns(spark, bronze_table: str, entity_name: str) -> None:
    """Verify that the bronze source table contains all required tenant columns.

    Raises:
        ValueError: If any of REQUIRED_TENANT_COLUMNS are missing from the table.
    """
    try:
        existing_cols = {f.name for f in spark.table(bronze_table).schema.fields}
    except Exception as e:
        raise ValueError(
            f"Cannot read schema of bronze table '{bronze_table}' "
            f"(referenced by {entity_name}): {e}"
        ) from e

    missing = [c for c in REQUIRED_TENANT_COLUMNS if c not in existing_cols]
    if missing:
        raise ValueError(
            f"Bronze table '{bronze_table}' is missing required tenant columns: "
            f"{missing}. All bronze tables must include {REQUIRED_TENANT_COLUMNS}."
        )


def _tenant_filter(alias: str, config: VaultConfig) -> str:
    """Build a WHERE clause fragment filtering by source system, brand, and region."""
    return (
        f"{alias}.ETL_SourceSystem = '{config.source_system}' "
        f"AND {alias}.ETL_Brand = '{config.brand}' "
        f"AND {alias}.ETL_Region = '{config.region}'"
    )


def _resolve_driving_key_expr(link: LinkConfig) -> str:
    """Resolve the SQL expression that produces the configured driving key value.

    driving_key_column refers to the column as stored on the silver link table:
    * hub reference column_name -> hashed hub key expression from bronze source column
    * extra_bk column_name -> raw bronze source column
    """
    if not link.driving_key_column:
        raise ValueError(
            f"Link '{link.table_name}' requires driving_key_column for relationship_pattern=1"
        )

    if link.extra_bk and link.driving_key_column == link.extra_bk.column_name:
        return f"src.{link.extra_bk.source_column}"

    for ref in link.hub_references:
        if ref.column_name == link.driving_key_column:
            return hash_hub_key("src", ref.source_column, REQUIRED_TENANT_COLUMNS)

    raise ValueError(
        f"Link '{link.table_name}' driving_key_column='{link.driving_key_column}' does not match "
        f"any hub_references.column_name or extra_bk.column_name"
    )


# ---------------------------------------------------------------------
# Hub Loader
# ---------------------------------------------------------------------

def load_hubs(
    spark,
    config: VaultConfig,
    entity_name: str | None = None,
) -> dict[str, int]:
    """Load hubs from config - insert new business keys only.

    Deduplicates append-only bronze by taking DISTINCT business keys
    (plus tenant columns). Registers ALL keys ever seen (including
    deleted records) since a hub is a permanent registry of business entities.

    Hub hash key = MD5(CONCAT_WS('||', BK, ETL_SourceSystem, ETL_Brand, ETL_Region))
    """
    bronze_schema = f"{config.target_catalog}.{config.bronze_schema}"
    silver_schema = f"{config.target_catalog}.{config.silver_schema}"
    record_source = config.record_source
    results = {}

    hubs_to_load = config.hubs
    if entity_name:
        hubs_to_load = [h for h in config.hubs if h.table_name == entity_name]
        if not hubs_to_load:
            raise ValueError(f"Hub '{entity_name}' not found in config.")

    for hub in hubs_to_load:
        bronze_table = f"{bronze_schema}.{hub.source_table}"
        _validate_bronze_columns(spark, bronze_table, hub.table_name)

        bk_col = hub.business_key_column
        hub_hash_expr = hash_hub_key("src", bk_col, REQUIRED_TENANT_COLUMNS)

        insert_cols = [hub.hash_key_name, hub.bk_column_name]
        select_exprs = [hub_hash_expr, f"src.{bk_col}"]

        for col in REQUIRED_TENANT_COLUMNS:
            insert_cols.append(col)
            select_exprs.append(f"src.{col}")

        insert_cols.extend(["ETL_LoadDate", "ETL_RecordSource"])
        select_exprs.extend(["current_timestamp()", f"'{record_source}'"])

        distinct_cols = [bk_col] + REQUIRED_TENANT_COLUMNS
        distinct_clause = ", ".join(distinct_cols)

        insert_clause = ", ".join(insert_cols)
        select_clause = ", ".join(select_exprs)
        tenant_filter = _tenant_filter("src", config)

        sql = f"""
            INSERT INTO {silver_schema}.{hub.table_name} ({insert_clause})
            SELECT {select_clause}
            FROM (
                SELECT DISTINCT {distinct_clause}
                FROM {bronze_table} src
                WHERE {bk_col} IS NOT NULL
                  AND {tenant_filter}
            ) src
            WHERE {hub_hash_expr} NOT IN (
                SELECT {hub.hash_key_name} FROM {silver_schema}.{hub.table_name}
            )
        """

        result = spark.sql(sql)
        rows = result.first()["num_affected_rows"]
        results[hub.table_name] = rows
        print(f"  Hub {hub.table_name}: {rows} new keys inserted")

    return results


# ---------------------------------------------------------------------
# Link Loader
# ---------------------------------------------------------------------

def load_links(
    spark,
    config: VaultConfig,
    entity_name: str | None = None,
) -> dict[str, int]:
    """Load links from config - insert new relationships only.

    Deduplicates append-only bronze by taking the latest row per business key
    (ROW_NUMBER partitioned by primary key, ordered by ETL_LoadedAt DESC).
    Excludes hard-deleted records from creating new relationships.
    """
    bronze_schema = f"{config.target_catalog}.{config.bronze_schema}"
    silver_schema = f"{config.target_catalog}.{config.silver_schema}"
    record_source = config.record_source
    results = {}

    links_to_load = config.links
    if entity_name:
        links_to_load = [
            l for l in config.links
            if l.table_name == entity_name
        ]

        if not links_to_load:
            raise ValueError(
                f"Link '{entity_name}' not found in config."
            )

    for link in links_to_load:
        bronze_table = f"{bronze_schema}.{link.source_table}"

        _validate_bronze_columns(
            spark,
            bronze_table,
            link.table_name,
        )

        # hash_column_names = get_hash_column_names(link)

        all_hash_cols = (
            get_hash_column_names(link)
            + REQUIRED_TENANT_COLUMNS
        )

        source_primary_key_partition = (
            get_source_primary_key_partition(link)
        )

        link_hash_expr = hash_composite_key(
            "src",
            all_hash_cols,
        )

        hub_ref_exprs = [
            hash_hub_key(
                "src",
                ref.source_column,
                REQUIRED_TENANT_COLUMNS,
            )
            for ref in link.hub_references
        ]

        insert_cols = ["LinkHashKey"]
        select_exprs = [link_hash_expr]

        if link.extra_bk:
            insert_cols.append(
                link.extra_bk.column_name
            )

            select_exprs.append(
                f"src.{link.extra_bk.source_column}"
            )

        for i, ref in enumerate(
            link.hub_references
        ):
            insert_cols.append(
                ref.column_name
            )

            select_exprs.append(
                hub_ref_exprs[i]
            )

        for col in REQUIRED_TENANT_COLUMNS:
            insert_cols.append(col)
            select_exprs.append(
                f"src.{col}"
            )

        insert_cols.extend(
            [
                "ETL_LoadDate",
                "ETL_RecordSource",
            ]
        )

        select_exprs.extend(
            [
                "current_timestamp()",
                f"'{record_source}'",
            ]
        )

        insert_clause = ", ".join(
            insert_cols
        )

        select_clause = ", ".join(
            select_exprs
        )

        filter_clause = link.filter

        tenant_filter = _tenant_filter(
            "src",
            config,
        )

        # Partition by the source system's configured primary key(s)
        # to retain only the latest version of each source record.

        sql = f"""
            INSERT INTO {silver_schema}.{link.table_name} ({insert_clause})
            SELECT {select_clause}
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {source_primary_key_partition}
                        ORDER BY ETL_LoadedAt DESC
                    ) AS _rn
                FROM {bronze_table}
                WHERE ETL_RecordStatus != 'hard_deleted'
            ) src
            WHERE src._rn = 1
              AND {tenant_filter}
              AND {filter_clause}
              AND {link_hash_expr} NOT IN (
                    SELECT LinkHashKey
                    FROM {silver_schema}.{link.table_name}
              )
        """

        result = spark.sql(sql)

        rows = result.first()[
            "num_affected_rows"
        ]

        results[
            link.table_name
        ] = rows

        print(
            f"  Link {link.table_name}: "
            f"{rows} new relationships inserted"
        )

    return results


# ---------------------------------------------------------------------
# Effectivity Satellite Loader
# ---------------------------------------------------------------------

def load_effectivity_satellites(
    spark,
    config: VaultConfig,
    entity_name: str | None = None,
) -> dict[str, int]:
    """Load auto-generated effectivity satellites from mutable link metadata.

    Pattern 1 (driving key): one active relationship per driver at a time.
    Pattern 2 (full-set): current bronze relationship set is authoritative.

    The loader inserts state-transition rows only. close_satellite_versions()
    then stamps ETL_EndDate on superseded open rows, leaving exactly one open
    row per LinkHashKey that reflects current active/inactive state.
    """
    bronze_schema = f"{config.target_catalog}.{config.bronze_schema}"
    silver_schema = f"{config.target_catalog}.{config.silver_schema}"
    record_source = config.record_source
    results = {}

    links_to_load = [l for l in config.links if l.relationship_pattern in (1, 2)]
    if entity_name:
        links_to_load = [l for l in links_to_load if l.table_name == entity_name]
        if not links_to_load:
            raise ValueError(f"Mutable link '{entity_name}' not found in config.")

    for link in links_to_load:
        bronze_table = f"{bronze_schema}.{link.source_table}"
        link_table = f"{silver_schema}.{link.table_name}"
        source_primary_key_partition = (
            get_source_primary_key_partition(link)
        )

        effsat_table = f"{silver_schema}.{config.source_system}_effsat_{link.table_name}_{config.brand}".lower()
        _validate_bronze_columns(spark, bronze_table, link.table_name)

        all_hash_cols = get_hash_column_names(link) + REQUIRED_TENANT_COLUMNS
        link_hash_expr = hash_composite_key("src", all_hash_cols)
        tenant_filter = _tenant_filter("src", config)
        filter_clause = link.filter

        # Tenant column names + literal values for INSERT (consistent with all vault tables)
        tenant_insert_cols = ", ".join(REQUIRED_TENANT_COLUMNS)
        tenant_insert_vals = f"'{config.source_system}', '{config.brand}', '{config.region}'"

        if link.relationship_pattern == 1:
            driving_key_expr = _resolve_driving_key_expr(link)
            driving_key_column = link.driving_key_column
            # Rank first, then apply the link filter to each driver's latest row: a latest row
            # that fails the filter (FK cleared) ends the relationship. Only drivers present in
            # bronze are judged - bronze keeps 7 days, so a missing driver is unchanged, not removed.
            sql = f"""
                WITH bronze_current AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY {driving_key_expr} ORDER BY ETL_LoadedAt DESC
                    ) AS _rn
                    FROM {bronze_table} src
                    WHERE ETL_RecordStatus != 'hard_deleted'
                      AND {tenant_filter}
                ),
                latest AS (
                    SELECT
                        CASE WHEN {filter_clause} THEN {link_hash_expr} END AS LinkHashKey,
                        {driving_key_expr} AS DrivingKeyValue
                    FROM bronze_current src
                    WHERE src._rn = 1
                ),
                expected_current AS (
                    SELECT LinkHashKey, DrivingKeyValue
                    FROM latest
                    WHERE LinkHashKey IS NOT NULL
                ),
                eff_open AS (
                    SELECT LinkHashKey, IsActive
                    FROM {effsat_table}
                    WHERE ETL_EndDate IS NULL
                ),
                to_activate AS (
                    SELECT ec.LinkHashKey, TRUE AS IsActive
                    FROM expected_current ec
                    LEFT JOIN eff_open eo
                        ON eo.LinkHashKey = ec.LinkHashKey
                        AND eo.IsActive = TRUE
                    WHERE eo.LinkHashKey IS NULL
                ),
                to_deactivate AS (
                    SELECT DISTINCT eo.LinkHashKey, FALSE AS IsActive
                    FROM eff_open eo
                    JOIN {link_table} lk
                        ON lk.LinkHashKey = eo.LinkHashKey
                    JOIN latest lt
                        ON lk.{driving_key_column} = lt.DrivingKeyValue
                    WHERE eo.IsActive = TRUE
                      AND (lt.LinkHashKey IS NULL OR lt.LinkHashKey != eo.LinkHashKey)
                )
                INSERT INTO {effsat_table} (LinkHashKey, IsActive, {tenant_insert_cols}, ETL_LoadDate, ETL_RecordSource)
                SELECT LinkHashKey, IsActive, {tenant_insert_vals}, current_timestamp(), '{record_source}'
                FROM (
                    SELECT * FROM to_activate
                    UNION ALL
                    SELECT * FROM to_deactivate
                ) changes
            """
        else:
            sql = f"""
                WITH bronze_current AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY {source_primary_key_partition} ORDER BY ETL_LoadedAt DESC
                    ) AS _rn
                    FROM {bronze_table}
                    WHERE ETL_RecordStatus != 'hard_deleted'
                ),
                expected_current AS (
                    SELECT DISTINCT {link_hash_expr} AS LinkHashKey
                    FROM bronze_current src
                    WHERE src._rn = 1
                      AND {tenant_filter}
                      AND {filter_clause}
                ),
                eff_open AS (
                    SELECT LinkHashKey, IsActive
                    FROM {effsat_table}
                    WHERE ETL_EndDate IS NULL
                ),
                to_activate AS (
                    SELECT ec.LinkHashKey, TRUE AS IsActive
                    FROM expected_current ec
                    LEFT JOIN eff_open eo
                        ON eo.LinkHashKey = ec.LinkHashKey
                        AND eo.IsActive = TRUE
                    WHERE eo.LinkHashKey IS NULL
                ),
                to_deactivate AS (
                    SELECT DISTINCT eo.LinkHashKey, FALSE AS IsActive
                    FROM eff_open eo
                    LEFT JOIN expected_current ec
                        ON ec.LinkHashKey = eo.LinkHashKey
                    WHERE eo.IsActive = TRUE
                      AND ec.LinkHashKey IS NULL
                )
                INSERT INTO {effsat_table} (LinkHashKey, IsActive, {tenant_insert_cols}, ETL_LoadDate, ETL_RecordSource)
                SELECT LinkHashKey, IsActive, {tenant_insert_vals}, current_timestamp(), '{record_source}'
                FROM (
                    SELECT * FROM to_activate
                    UNION ALL
                    SELECT * FROM to_deactivate
                ) changes
            """

        result = spark.sql(sql)
        rows = result.first()["num_affected_rows"]
        results[effsat_table.split(".")[-1]] = rows
        print(f"  Effectivity Satellite {effsat_table.split('.')[-1]}: {rows} state changes inserted")

    return results


# ---------------------------------------------------------------------
# Satellite Loader
# ---------------------------------------------------------------------

def load_satellites(
    spark,
    config: VaultConfig,
    entity_name: str | None = None,
) -> dict[str, int]:
    """Load satellites from config - pure INSERT-ONLY, no MERGE/UPDATE."""
    bronze_schema = f"{config.target_catalog}.{config.bronze_schema}"
    silver_schema = f"{config.target_catalog}.{config.silver_schema}"
    record_source = config.record_source
    results = {}

    sats_to_load = [s for s in config.satellites if not s.is_auto_generated]
    if entity_name:
        sats_to_load = [s for s in sats_to_load if s.table_name == entity_name]
        if not sats_to_load:
            raise ValueError(f"Satellite '{entity_name}' not found in config.")

    for sat in sats_to_load:
        bronze_table = f"{bronze_schema}.{sat.source_table}"
        _validate_bronze_columns(spark, bronze_table, sat.table_name)

        if sat.is_link_satellite:
            all_link_cols = sat.parent_key_source + REQUIRED_TENANT_COLUMNS
            parent_hash_expr = hash_composite_key("src", all_link_cols)
        else:
            parent_hash_expr = hash_hub_key("src", sat.parent_key_source[0], REQUIRED_TENANT_COLUMNS)

        all_attributes = dict(sat.attributes)
        for col in REQUIRED_TENANT_COLUMNS + REQUIRED_SATELLITE_COLUMNS:
            if col not in all_attributes:
                all_attributes[col] = col

        source_cols = list(all_attributes.values())
        hash_diff_expr = hash_diff("src", source_cols)

        if len(sat.parent_key_source) == 1:
            partition_key = sat.parent_key_source[0]
        else:
            partition_key = ", ".join(sat.parent_key_source)

        target_cols = [sat.parent_key, "ETL_LoadDate", "ETL_HashDiff", "ETL_RecordSource"]
        select_exprs = [parent_hash_expr, "current_timestamp()", hash_diff_expr, f"'{record_source}'"]

        for target_col, source_col in all_attributes.items():
            target_cols.append(target_col)
            select_exprs.append(f"src.{source_col}")

        insert_clause = ", ".join(target_cols)
        select_clause = ",\n                ".join(select_exprs)
        parent_key = sat.parent_key
        sat_table = f"{silver_schema}.{sat.table_name}"
        tenant_filter = _tenant_filter("src", config)

        sql = f"""
            WITH bronze_current AS (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {partition_key} ORDER BY ETL_LoadedAt DESC
                ) AS _rn
                FROM {bronze_table}
            ),
            sat_latest AS (
                SELECT {parent_key}, ETL_HashDiff
                FROM {sat_table}
                WHERE ETL_EndDate IS NULL
                  AND ETL_SourceSystem = '{config.source_system}'
                  AND ETL_Brand = '{config.brand}'
                  AND ETL_Region = '{config.region}'
            )
            INSERT INTO {sat_table} ({insert_clause})
            SELECT
                {select_clause}
            FROM bronze_current src
            WHERE src._rn = 1
              AND {tenant_filter}
              AND (
                NOT EXISTS (
                    SELECT 1 FROM sat_latest sl
                    WHERE sl.{parent_key} = {parent_hash_expr}
                )
                OR
                EXISTS (
                    SELECT 1 FROM sat_latest sl
                    WHERE sl.{parent_key} = {parent_hash_expr}
                      AND sl.ETL_HashDiff != {hash_diff_expr}
                )
              )
        """

        result = spark.sql(sql)
        rows = result.first()["num_affected_rows"]
        results[sat.table_name] = rows
        print(f"  Satellite {sat.table_name}: {rows} new versions inserted")

    return results


# ---------------------------------------------------------------------
# Hard-Delete Reconciliation
# ---------------------------------------------------------------------

def reconcile_hard_deletes(
    spark,
    config: VaultConfig,
    load_mode: str,
) -> dict[str, int]:
    """Detect and mark hard-deleted records in satellites."""
    if load_mode not in ("reconcile", "reconcile_ids"):
        raise ValueError(f"reconcile_hard_deletes requires load_mode 'reconcile' or 'reconcile_ids', got '{load_mode}'")

    bronze_schema = f"{config.target_catalog}.{config.bronze_schema}"
    silver_schema = f"{config.target_catalog}.{config.silver_schema}"
    record_source = config.record_source
    results = {}

    for sat in config.satellites:
        if sat.is_auto_generated:
            print(f"  Satellite {sat.table_name}: skipped (auto-generated effectivity satellite)")
            continue
        if load_mode == "reconcile_ids" and sat.is_link_satellite:
            print(f"  Satellite {sat.table_name}: skipped (link satellite, reconcile_ids mode)")
            continue

        parent_key = sat.parent_key
        sat_table = f"{silver_schema}.{sat.table_name}"

        if load_mode == "reconcile_ids":
            bronze_ref_table = f"{bronze_schema}.{sat.source_table}_ids"
        else:
            bronze_ref_table = f"{bronze_schema}.{sat.source_table}"

        if sat.is_link_satellite:
            all_link_cols = sat.parent_key_source + REQUIRED_TENANT_COLUMNS
            bronze_key_expr = hash_composite_key("bk", all_link_cols)
        else:
            bronze_key_expr = hash_hub_key("bk", sat.parent_key_source[0], REQUIRED_TENANT_COLUMNS)

        null_filters = " AND ".join(f"bk.{col} IS NOT NULL" for col in sat.parent_key_source)

        all_attributes = dict(sat.attributes)
        for col in REQUIRED_TENANT_COLUMNS + REQUIRED_SATELLITE_COLUMNS:
            if col not in all_attributes:
                all_attributes[col] = col

        target_cols = [parent_key, "ETL_LoadDate", "ETL_HashDiff", "ETL_RecordSource"]
        select_exprs = [f"sa.{parent_key}", "current_timestamp()"]

        hash_diff_parts = []
        attr_select = []
        for target_col in all_attributes.keys():
            target_cols.append(target_col)
            if target_col == "ETL_RecordStatus":
                hash_diff_parts.append("'hard_deleted'")
                attr_select.append("'hard_deleted'")
            else:
                hash_diff_parts.append(f"sa.{target_col}")
                attr_select.append(f"sa.{target_col}")

        hash_diff_expr = f"{HASH_FUNCTION}(CONCAT_WS('||', {', '.join(hash_diff_parts)}))"
        select_exprs.append(hash_diff_expr)
        select_exprs.append(f"'{record_source}'")
        select_exprs.extend(attr_select)

        insert_clause = ", ".join(target_cols)
        select_clause = ",\n                ".join(select_exprs)

        sql = f"""
            WITH sat_active AS (
                SELECT * FROM {sat_table}
                WHERE ETL_EndDate IS NULL
                  AND ETL_RecordStatus != 'hard_deleted'
                  AND ETL_SourceSystem = '{config.source_system}'
                  AND ETL_Brand = '{config.brand}'
                  AND ETL_Region = '{config.region}'
            ),
            bronze_keys AS (
                SELECT DISTINCT {bronze_key_expr} AS _key_hash
                FROM {bronze_ref_table} bk
                WHERE {null_filters}
                  AND {_tenant_filter('bk', config)}
            )
            INSERT INTO {sat_table} ({insert_clause})
            SELECT {select_clause}
            FROM sat_active sa
            WHERE sa.{parent_key} NOT IN (SELECT _key_hash FROM bronze_keys)
        """

        result = spark.sql(sql)
        rows = result.first()["num_affected_rows"]
        results[sat.table_name] = rows
        print(f"  Satellite {sat.table_name}: {rows} records marked hard_deleted")

    return results


# ---------------------------------------------------------------------
# Version Closing - sets ETL_EndDate on superseded satellite rows
# ---------------------------------------------------------------------

def close_satellite_versions(
    spark,
    config: VaultConfig,
) -> dict[str, int]:
    """Close superseded satellite versions by stamping ETL_EndDate."""
    silver_schema = f"{config.target_catalog}.{config.silver_schema}"
    results = {}

    for sat in config.satellites:
        parent_key = sat.parent_key
        sat_table = f"{silver_schema}.{sat.table_name}"

        close_sql = (
            f"MERGE INTO {sat_table} AS target "
            f"USING ("
            f"  SELECT {parent_key}, MAX(ETL_LoadDate) AS latest_load_date "
            f"  FROM {sat_table} "
            f"  WHERE ETL_EndDate IS NULL "
            f"  AND ETL_SourceSystem = '{config.source_system}'"
            f"  AND ETL_Brand = '{config.brand}'"
            f"  AND ETL_Region = '{config.region}'"
            f"  GROUP BY {parent_key} "
            f"  HAVING COUNT(*) > 1"
            f") AS src "
            f"ON target.{parent_key} = src.{parent_key} "
            f"  AND target.ETL_EndDate IS NULL "
            f"  AND target.ETL_LoadDate < src.latest_load_date "
            f"WHEN MATCHED THEN UPDATE SET target.ETL_EndDate = current_timestamp()"
        )

        result = spark.sql(close_sql)
        rows = result.first()["num_affected_rows"]
        results[sat.table_name] = rows
        if rows > 0:
            print(f"  Satellite {sat.table_name}: {rows} previous versions closed")

    return results
