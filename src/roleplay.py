"""
Utility to materialize role-playing dimensions as temporary or global temp views from a base dimension.
"""

from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
import logging

# Configure module-level logger (caller may override level/handler)
logger = logging.getLogger(__name__)
if not logger.handlers:
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)


def _get_roleplaying_df(
    p_base_df: DataFrame,
    role_name: str,
    role_cfg: Dict[str, Dict[str, str]],
    p_columns_keep: Optional[List[str]] = None,
) -> DataFrame:
    """
    Applies filtering and column aliasing for a single role-playing dimension.

    This function implements a schema-aware aliasing strategy:
    1. It automatically prefixes all columns from the base DataFrame with the `role_name`.
    2. This behavior can be overridden for specific columns by providing an explicit
       mapping in `role_cfg['columns']`.

    Parameters
    ----------
    p_base_df : DataFrame
        Base dimension dataframe (e.g., dim_date).
    role_name : str
        The name of the role (e.g., "order_date"), used as the default prefix.
    role_cfg : Dict[str, Dict[str, str]]
        Configuration for the role.
        - "columns": A dictionary of explicit `{"base_col": "alias_col"}` mappings that
          override the automatic prefixing.
        - "filter": An optional SQL predicate string for row filtering.
    p_columns_keep : Optional[List[str]]
        If provided, restricts the final selection to this list of aliased columns.

    Returns
    -------
    DataFrame
        A new DataFrame with applied filters and aliased columns.
    """
    cols_cfg_overrides = role_cfg.get("columns", {})
    df = p_base_df
    sql_pred = role_cfg.get("filter")

    if sql_pred:
        logger.debug("Applying filter for role '%s': %s", role_name, sql_pred)
        df = df.filter(sql_pred)

    select_exprs = []
    for base_col in df.columns:
        if base_col in cols_cfg_overrides:
            # Apply explicit rename from config if it exists (override)
            alias_col = cols_cfg_overrides[base_col]
        else:
            # Otherwise, automatically prefix with the role name
            alias_col = f"{role_name}_{base_col}"
        select_exprs.append(df[base_col].alias(alias_col))

    # Select all generated expressions
    df_transformed = df.select(select_exprs)

    # If p_columns_keep is provided, select only those columns from the transformed frame
    if p_columns_keep:
        final_aliased_columns = df_transformed.columns
        missing_cols = [c for c in p_columns_keep if c not in final_aliased_columns]
        if missing_cols:
            raise ValueError(
                f"Requested columns {missing_cols} to keep for role '{role_name}' "
                f"are not present in the final list of aliased columns: {final_aliased_columns}."
            )
        df_transformed = df_transformed.select(p_columns_keep)

    return df_transformed


def f_create_roleplaying_global_temp_view(
    p_spark: SparkSession,
    p_base_df: DataFrame,
    p_role_map: Dict[str, Dict[str, str]],
    p_view_prefix: str = "rp_",
    p_columns_keep: Optional[List[str]] = None,
    p_use_global_views: bool = True,  # toggle for local vs. global views
) -> None:
    """
    Create one temp view per role-playing dimension by aliasing the base dimension.

    This function is schema-aware. For each role defined in `p_role_map`, it automatically
    aliases all columns of the base DataFrame `p_base_df` by prefixing them with the
    role's name. This behavior can be overridden with explicit mappings.

    Parameters
    ----------
    p_spark : SparkSession
        Active Spark session.
    p_base_df : DataFrame
        Base dimension dataframe (e.g., dim_date).
    p_role_map : Dict[str, Dict[str, str]]
        Configuration dictionary for the roles. For each `role_name`:
        - All columns from `p_base_df` are automatically prefixed with the `role_name`
          (e.g., `is_weekend` -> `order_date_is_weekend`).
        - The `columns` key allows overriding this behavior with explicit mappings
          (e.g., `{"date_key": "order_date_key"}`).
        - The `filter` key applies an optional SQL filter expression.
        Example:
        {
          "order_date": {
              "columns": {"date_key": "order_date_key", "full_date": "order_date"},
              # other columns like 'is_weekend' will become 'order_date_is_weekend'
          },
          "ship_date": {
              "columns": {"date_key": "ship_date_key"},
              "filter": "is_weekend = false"
          }
        }
    p_view_prefix : str
        Prefix for created temp view names. Final name: {p_view_prefix}{role_name}
    p_columns_keep : Optional[List[str]]
        If provided, restrict selection to these aliased columns.
    p_use_global_views : bool
        If True, creates global_temp views; if False, creates session temp views.

    Returns
    -------
    None
        Creates temp views accessible via spark.table().
    """
    assert p_base_df is not None, "p_base_df must not be None"
    assert isinstance(p_role_map, dict) and p_role_map, "p_role_map must be a non-empty dict"

    for role_name, role_cfg in p_role_map.items():
        df = _get_roleplaying_df(p_base_df, role_name, role_cfg, p_columns_keep)

        view_name = f"{p_view_prefix}{role_name}"

        if p_use_global_views:
            df.createOrReplaceGlobalTempView(view_name)
            logger.info("Created global temp view: global_temp.%s (cols: %s)", view_name, ", ".join(df.columns))
        else:
            df.createOrReplaceTempView(view_name)
            logger.info("Created session temp view: %s (cols: %s)", view_name, ", ".join(df.columns))
