"""
Data ingestion and validation helpers for the KPI intelligence pipeline.

This file sits in the ingestion layer of the project. It loads CSV inputs,
normalizes schemas, coerces dates and numbers, raises hard errors for missing
required columns, and records soft validation warnings for data quality issues.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .config import DATASET_CONFIG


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # ================================
    # Function: _normalize_columns
    # Purpose: Standardizes raw column names into a predictable snake_case form.
    # Inputs:
    #   - df (pd.DataFrame): raw dataset as loaded from CSV
    # Output:
    #   - pd.DataFrame with normalized column names
    # ================================
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _parse_dates(df: pd.DataFrame, columns: List[str], dataset_name: str, warnings: List[str]) -> pd.DataFrame:
    # ================================
    # Function: _parse_dates
    # Purpose: Converts configured date columns to pandas timestamps.
    # Inputs:
    #   - df (pd.DataFrame): dataset to update
    #   - columns (List[str]): candidate date columns
    #   - dataset_name (str): dataset label for warning messages
    #   - warnings (List[str]): shared warning collector
    # Output:
    #   - pd.DataFrame with parsed datetime columns
    # Important Logic:
    #   - Invalid dates are coerced to NaT and logged as warnings
    # ================================
    for col in columns:
        if col in df.columns:
            before_nulls = df[col].isna().sum()
            df[col] = pd.to_datetime(df[col], errors="coerce")
            after_nulls = df[col].isna().sum()
            if after_nulls > before_nulls:
                warnings.append(f"{dataset_name}: column '{col}' had {after_nulls - before_nulls} unparsable date values coerced to NaT.")
    return df


def _coerce_numeric(df: pd.DataFrame, columns: List[str], dataset_name: str, warnings: List[str]) -> pd.DataFrame:
    # ================================
    # Function: _coerce_numeric
    # Purpose: Converts configured numeric columns to numeric dtype.
    # Inputs:
    #   - df (pd.DataFrame): dataset to update
    #   - columns (List[str]): candidate numeric columns
    #   - dataset_name (str): dataset label for warning messages
    #   - warnings (List[str]): shared warning collector
    # Output:
    #   - pd.DataFrame with numeric coercion applied
    # Important Logic:
    #   - Non-numeric values become NaN and are recorded as warnings
    # ================================
    for col in columns:
        if col in df.columns:
            before_nulls = df[col].isna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            after_nulls = df[col].isna().sum()
            if after_nulls > before_nulls:
                warnings.append(f"{dataset_name}: column '{col}' had {after_nulls - before_nulls} non-numeric values coerced to NaN.")
    return df


def _validate_required_columns(df: pd.DataFrame, required_columns: List[str], dataset_name: str) -> None:
    # ================================
    # Function: _validate_required_columns
    # Purpose: Fails fast when a required schema field is missing.
    # Inputs:
    #   - df (pd.DataFrame): normalized dataset
    #   - required_columns (List[str]): mandatory columns for the dataset
    #   - dataset_name (str): dataset label for the error message
    # Output:
    #   - None
    # ================================
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"{dataset_name}: missing required columns: {missing}")


def _warn_iqr_outliers(series: pd.Series, dataset_name: str, column: str, warnings: List[str]) -> None:
    # ================================
    # Function: _warn_iqr_outliers
    # Purpose: Flags statistically unusual values using an IQR-based rule.
    # Inputs:
    #   - series (pd.Series): numeric values to inspect
    #   - dataset_name (str): dataset label for the warning
    #   - column (str): source column name
    #   - warnings (List[str]): shared warning collector
    # Output:
    #   - None
    # ================================
    clean = series.dropna()
    if len(clean) < 4:
        return
    q1 = clean.quantile(0.25)
    q3 = clean.quantile(0.75)
    iqr = q3 - q1
    if pd.isna(iqr) or iqr == 0:
        return
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outlier_count = int(((clean < lower) | (clean > upper)).sum())
    if outlier_count:
        warnings.append(f"{dataset_name}: {outlier_count} potential outliers detected in '{column}' using IQR bounds.")


def _validate_quality(df: pd.DataFrame, dataset_name: str, warnings: List[str]) -> None:
    # ================================
    # Function: _validate_quality
    # Purpose: Applies dataset-specific business validation rules.
    # Inputs:
    #   - df (pd.DataFrame): cleaned dataset
    #   - dataset_name (str): dataset label that selects the rule set
    #   - warnings (List[str]): shared warning collector
    # Output:
    #   - None
    # Important Logic:
    #   - Checks KPI-critical fields such as quantities, percentages, and key IDs
    # ================================
    if dataset_name == "inbound_parts":
        if (df["qty_ordered"] < 0).any() or (df["qty_received"] < 0).any():
            warnings.append("inbound_parts: negative quantities detected.")
        if (df["received_date"] < df["expected_date"]).any():
            early_count = int((df["received_date"] < df["expected_date"]).sum())
            warnings.append(f"inbound_parts: {early_count} receipts arrived before expected date; valid but notable.")
        _warn_iqr_outliers(df["inbound_lead_time_days"], dataset_name, "inbound_lead_time_days", warnings)
    elif dataset_name == "outbound_parts":
        if ((df["fill_rate"] < 0) | (df["fill_rate"] > 1)).any():
            warnings.append("outbound_parts: fill_rate values outside [0,1] detected.")
        if (df["qty_shipped"] > df["qty_ordered"]).any():
            warnings.append("outbound_parts: shipped quantity exceeds ordered quantity in some rows.")
        _warn_iqr_outliers(df["fill_rate"], dataset_name, "fill_rate", warnings)
    elif dataset_name == "inventory_snapshot":
        if ((df["stockout_flag"] < 0) | (df["stockout_flag"] > 1)).any():
            warnings.append("inventory_snapshot: stockout_flag values outside [0,1] detected.")
    elif dataset_name == "warehouse_productivity":
        if ((df["equipment_utilization_pct"] < 0) | (df["equipment_utilization_pct"] > 1)).any():
            warnings.append("warehouse_productivity: equipment utilization outside [0,1] detected.")
        if ((df["sla_adherence_pct"] < 0) | (df["sla_adherence_pct"] > 1)).any():
            warnings.append("warehouse_productivity: SLA adherence outside [0,1] detected.")
    elif dataset_name == "employee_productivity":
        if (df["hours_worked"] <= 0).any():
            warnings.append("employee_productivity: non-positive hours_worked detected.")

    # Generic key checks catch datasets that technically load but cannot support
    # downstream slicing or traceability because identifier columns are empty.
    key_candidates = [c for c in ["warehouse_id", "part_number", "employee_id", "supplier_id", "customer_id"] if c in df.columns]
    for col in key_candidates:
        if df[col].isna().all():
            warnings.append(f"{dataset_name}: key column '{col}' is entirely null.")


def load_dataset(data_dir: Path, dataset_name: str, warnings: List[str]) -> pd.DataFrame:
    # ================================
    # Function: load_dataset
    # Purpose: Loads and validates a single configured dataset from disk.
    # Inputs:
    #   - data_dir (Path): root folder containing CSV inputs
    #   - dataset_name (str): dataset key from DATASET_CONFIG
    #   - warnings (List[str]): shared warning collector
    # Output:
    #   - pd.DataFrame ready for KPI computation
    # ================================
    cfg = DATASET_CONFIG[dataset_name]
    path = data_dir / cfg["file"]
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    # Apply a fixed ingestion sequence so downstream KPI code can rely on a
    # consistent schema and dtype contract.
    df = pd.read_csv(path)
    df = _normalize_columns(df)
    _validate_required_columns(df, cfg["required_columns"], dataset_name)
    df = _parse_dates(df, cfg["date_columns"], dataset_name, warnings)
    df = _coerce_numeric(df, cfg["numeric_columns"], dataset_name, warnings)
    _validate_quality(df, dataset_name, warnings)
    return df


def load_all(data_dir: Path) -> Tuple[Dict[str, pd.DataFrame], Dict[str, int], List[str]]:
    # ================================
    # Function: load_all
    # Purpose: Loads every configured dataset used by the KPI pipeline.
    # Inputs:
    #   - data_dir (Path): root folder containing CSV inputs
    # Output:
    #   - Tuple of datasets, row counts, and validation warnings
    # ================================
    warnings: List[str] = []
    datasets: Dict[str, pd.DataFrame] = {}
    row_counts: Dict[str, int] = {}

    for dataset_name in DATASET_CONFIG:
        df = load_dataset(data_dir, dataset_name, warnings)
        datasets[dataset_name] = df
        row_counts[dataset_name] = int(len(df))

    return datasets, row_counts, warnings


def derive_default_period(datasets: Dict[str, pd.DataFrame]) -> Tuple[pd.Timestamp, pd.Timestamp, str]:
    # ================================
    # Function: derive_default_period
    # Purpose: Selects the latest full month shared across all core datasets.
    # Inputs:
    #   - datasets (Dict[str, pd.DataFrame]): loaded source tables
    # Output:
    #   - Tuple of start timestamp, end timestamp, and display label
    # Important Logic:
    #   - Uses the earliest "latest month" across datasets so all KPI domains
    #     report on a common period with complete data coverage
    # ================================
    max_month_starts = []
    for dataset_name, cfg in DATASET_CONFIG.items():
        event_col = cfg["event_date"]
        s = datasets[dataset_name][event_col].dropna()
        if s.empty:
            raise ValueError(f"Cannot derive default period; dataset {dataset_name} has no valid event dates.")
        latest = s.max().to_period("M").to_timestamp()
        max_month_starts.append(latest)

    common_month_start = min(max_month_starts)
    start = common_month_start.normalize()
    end = (common_month_start + pd.offsets.MonthEnd(1)).normalize()
    label = start.strftime("%B %Y")
    return start, end, label
