#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ALRTSP.py – Credit Cards Alert ETL Pipeline
Fully reconstructed and production-ready version.
"""

# ========= Stage 1: Configuration & Trino connection =========

from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import configparser
from datetime import date, timedelta, datetime
import pandas as pd
import numpy as np

# --- Trino imports (install with `pip install trino`) ---
from trino import dbapi
from trino.auth import BasicAuthentication


# ---------- Logging setup ----------
def setup_logging(log_file: Optional[Path] = None, level: int = logging.INFO) -> None:
    fmt = "%(asctime)s %(levelname)s %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handlers = [logging.StreamHandler()]
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers)


# ---------- Config loader ----------
def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    parser = configparser.ConfigParser()
    parser.read(path, encoding="utf-8")
    section = "ALRTSP" if "ALRTSP" in parser else parser.default_section
    c = parser[section]

    conf = {
        "host": c.get("host", fallback="localhost"),
        "port": c.getint("port", fallback=443),
        "user": c.get("username", fallback=c.get("user", fallback="trino_user")),
        "password": c.get("password", fallback=""),
        "http_scheme": c.get("http_scheme", fallback=c.get("http_schene", fallback="https")),
        "catalog": c.get("catalog", fallback=c.get("catelog", fallback="edl_in")),
        "schema": c.get("schema", fallback=c.get("schena", fallback="prod_brte_ess")),
        "source": c.get("source", fallback="c86_alerts_cards"),
        "regpath": c.get("regpath", fallback=c.get("cegpath", fallback="/sas/RSD/REG")),
        "out_root": c.get("out_root", fallback=c.get("out root", fallback="./c86/output/alert/cards")),
        "log_root": c.get("log_root", fallback=c.get("log root", fallback="./c86/log/alert/cards")),
        "env": c.get("env", fallback="DEV"),
    }
    conf["out_root"] = str(Path(conf["out_root"]).resolve())
    conf["log_root"] = str(Path(conf["log_root"]).resolve())
    return conf


# ---------- Trino connection ----------
def get_trino_conn(conf: Dict[str, Any]):
    auth = None
    password = (conf.get("password") or "").strip()
    user = conf["user"]
    if password:
        auth = BasicAuthentication(user, password)
    return dbapi.connect(
        host=conf["host"],
        port=int(conf["port"]),
        user=user,
        http_scheme=conf.get("http_scheme", "https"),
        auth=auth,
        catalog=conf["catalog"],
        schema=conf["schema"],
        source=conf.get("source") or user,
    )


def _bootstrap(config_path: Path) -> Dict[str, Any]:
    conf = load_config(config_path)
    log_dir = Path(conf["log_root"])
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"ALRTSP_{conf['env']}.log"
    setup_logging(log_file)
    logging.info(
        "Config loaded | env=%s host=%s:%s catalog=%s schema=%s out_root=%s",
        conf["env"], conf["host"], conf["port"], conf["catalog"], conf["schema"], conf["out_root"]
    )
    return conf


# ========= Stage 2: Date Logic =========

def compute_dates(today: Optional[date] = None) -> Dict[str, date]:
    if today is None:
        today = date.today()

    start_dt_ini = date(2022, 6, 30)
    week_end_dt_ini = date(2022, 7, 25)
    weekday = today.weekday()  # Monday=0 ... Sunday=6
    days_since_friday = (weekday - 4) % 7
    week_start_friday = today - timedelta(days=days_since_friday)
    end_dt = week_start_friday - timedelta(days=2)

    start_dt = start_dt_ini if end_dt < week_end_dt_ini else end_dt - timedelta(days=6)
    week_start_dt = start_dt
    week_end_dt = end_dt
    pardt = start_dt - timedelta(days=7)
    week_end_dt_p1 = end_dt + timedelta(days=1)
    label = today.strftime("%Y%m%d")

    return {
        "report_dt": today,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "week_start_dt": week_start_dt,
        "week_end_dt": week_end_dt,
        "week_end_dt_p1": week_end_dt_p1,
        "pardt": pardt,
        "label": label,
        "ymd": label,
    }


# ========= Stage 3: SQL Helpers and Query Templates =========

def _SAFE_TS(expr: str) -> str:
    return f"""
    TRY(
        CAST(
            from_iso8601_timestamp(
                regexp_replace(
                    regexp_replace({expr}, ' ', 'T'),
                    'Z$', ''
                )
            ) AS timestamp
        )
    )
    """.strip()


ESS_PROCESS_TS = _SAFE_TS("element_at(eventattributes, 'ess_process_timestamp')")
ESS_SRC_TS = _SAFE_TS("element_at(eventattributes, 'ess_src_event_timestamp')")
SRC_HDR = "TRY(json_parse(element_at(eventattributes, 'SourceEventHeader')))"
EVT_PAYLOAD = "TRY(json_parse(element_at(eventattributes, 'eventPayload')))"
EVT_ID = "TRY(json_extract_scalar({SRC_HDR}, '$.eventId'))"
EVT_TS = _SAFE_TS("json_extract_scalar({SRC_HDR}, '$.eventTimestamp')")


def q_xb86_cards(schema: str, week_start: date, week_end_p1: date, pardt: date) -> str:
    return f"""
    WITH cards AS (
        SELECT
            event_activity_type,
            source_event_id,
            partition_date,
            TRY(date_parse(partition_date, '%Y%m%d')) AS partition_date_parsed,
            {ESS_PROCESS_TS} AS ess_process_timestamp,
            {ESS_SRC_TS} AS ess_src_event_timestamp,
            {EVT_ID} AS eventid,
            {EVT_TS} AS eventtimestamp,
            {EVT_PAYLOAD} AS eventpayload
        FROM {schema}.fxbo__credit_card_system_interface
    )
    SELECT
        source_event_id,
        partition_date,
        ess_process_timestamp,
        ess_src_event_timestamp,
        eventpayload,
        eventtimestamp,
        TRY(json_extract_scalar(eventpayload, '$.accountId')) AS accountid,
        TRY(json_extract_scalar(eventpayload, '$.alertType')) AS alerttype,
        TRY(CAST(json_extract_scalar(eventpayload, '$.thresholdAmount') AS DECIMAL(12,2))) AS thresholdamount,
        TRY(json_extract_scalar(eventpayload, '$.customerId')) AS customerid,
        TRY(json_extract_scalar(eventpayload, '$.accountCurrency')) AS accountcurrency,
        TRY(CAST(json_extract_scalar(eventpayload, '$.creditLimit') AS DECIMAL(12,2))) AS creditlimit,
        TRY(json_extract_scalar(eventpayload, '$.maskedAccount')) AS maskedaccount,
        TRY(json_extract_scalar(eventpayload, '$.decisionId')) AS decisionid,
        TRY(CAST(json_extract_scalar(eventpayload, '$.alertAmount') AS DECIMAL(12,2))) AS alertamount
    FROM cards
    WHERE
        partition_date_parsed > DATE '{pardt.isoformat()}'
        AND event_activity_type = 'Alert Decision Cards'
        AND TRY(json_extract_scalar(eventpayload, '$.alertType')) = 'AVAIL_CREDIT_REMAINING'
        AND {_SAFE_TS('CAST(eventtimestamp AS varchar)')} BETWEEN
            TIMESTAMP '{week_start.isoformat()} 00:00:00'
            AND TIMESTAMP '{week_end_p1.isoformat()} 00:00:00'
    """.strip()


def q_cards_dec_pref(schema: str, week_end_p1: date) -> str:
    return f"""
    SELECT
        {EVT_TS} AS eventtimestamp_p,
        event_channel_type AS event_channel_type_p,
        event_activity_type AS event_activity_type_p,
        TRY(date_parse(partition_date, '%Y%m%d')) AS partition_date_p,
        {ESS_PROCESS_TS} AS ess_process_timestamp_p,
        {ESS_SRC_TS} AS ess_src_event_timestamp_p,
        {EVT_ID} AS eventid_p,
        TRY(json_extract_scalar({SRC_HDR}, '$.eventActivityName')) AS eventactivityname_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.preferenceType')) AS preferencetype_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.clientID')) AS clientid_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.isBusiness')) AS isbusiness_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.sendAlertEligible')) AS sendalerteligible_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.active')) AS active_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.threshold')) AS threshold_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.custID')) AS custid_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.account')) AS account_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.maskedAccountNo')) AS maskedaccountno_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.externalAccount')) AS externalaccount_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.productType')) AS producttype_p
    FROM {schema}.ffs0__client_alert_preferences_dep__initial_load
    WHERE
        {_SAFE_TS('eventtimestamp')} > TIMESTAMP '{week_end_p1.isoformat()} 00:00:00'
        AND event_activity_type IN ('Create Account Preference', 'Update Account Preference')
        AND TRY(json_extract_scalar({EVT_PAYLOAD}, '$.preferenceType')) = 'AVAIL_CREDIT_REMAINING'
        AND TRY(json_extract_scalar({EVT_PAYLOAD}, '$.productType')) = 'CREDITCARD'
    """.strip()


def q_ffte_inbox(schema: str, week_start: date, week_end_p1: date) -> str:
    return f"""
    WITH base AS (
        SELECT
            {ESS_PROCESS_TS} AS ess_process_timestamp_a,
            {EVT_TS} AS eventtimestamp_a,
            TRY(json_extract_scalar({EVT_PAYLOAD}, '$.decisionId')) AS decisionid_a,
            TRY(CAST(json_extract_scalar({EVT_PAYLOAD}, '$.alertAmount') AS DECIMAL(12,2))) AS alertamount_a
        FROM {schema}.fft0__alert_inbox_dep
        WHERE
            event_activity_type = 'Alert Delivery Audit'
            AND TRY(json_extract_scalar({EVT_PAYLOAD}, '$.alertType')) = 'AVAIL_CREDIT_REMAINING'
            AND {_SAFE_TS('eventtimestamp')} >= TIMESTAMP '{week_start.isoformat()} 00:00:00'
            AND {_SAFE_TS('eventtimestamp')} < TIMESTAMP '{week_end_p1.isoformat()} 00:00:00'
            AND TRY(json_extract_scalar({EVT_PAYLOAD}, '$.decisionId')) IS NOT NULL
    ),
    dedup AS (
        SELECT
            eventtimestamp_a,
            decisionid_a,
            alertamount_a,
            ROW_NUMBER() OVER (PARTITION BY decisionid_a ORDER BY eventtimestamp_a DESC) AS rn
        FROM base
    )
    SELECT
        ess_process_timestamp_a,
        eventtimestamp_a,
        decisionid_a,
        alertamount_a
    FROM dedup
    WHERE rn = 1
    """.strip()


# ========= Stage 4: ETL / Pipeline Execution and Data Processing =========

def read_trino_df(conn, sql: str) -> pd.DataFrame:
    logging.info("Executing SQL:\n%s", sql)
    df = pd.read_sql(sql, conn)
    logging.info("Rows fetched: %d", len(df))
    return df


def _pick(columns, candidates):
    for c in candidates:
        if c in columns:
            return c
    return None


def _to_lowercase_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower().strip() for c in df.columns]
    return df


def _clean_blanks_to_nan(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).replace(r"^\s*$", np.nan, regex=True)


def run_pipeline(conf: dict):
    dt = compute_dates()
    label = dt["label"]
    out_root = Path(conf["out_root"])
    log_root = Path(conf["log_root"])
    out_dir = out_root / label
    out_dir.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)

    log_file = log_root / f"c86_alerts_cards_{label}.log"
    setup_logging(log_file)

    with get_trino_conn(conf) as conn:
        schema = f"{conf['catalog']}.{conf['schema']}"
        df_cards = read_trino_df(conn, q_xb86_cards(schema, dt["week_start_dt"], dt["week_end_dt_p1"], dt["pardt"]))
        df_pref = read_trino_df(conn, q_cards_dec_pref(schema, dt["week_end_dt_p1"]))
        df_inbox = read_trino_df(conn, q_ffte_inbox(schema, dt["week_start_dt"], dt["week_end_dt_p1"]))

    df_cards = _to_lowercase_columns(df_cards)
    df_pref = _to_lowercase_columns(df_pref)
    df_inbox = _to_lowercase_columns(df_inbox)

    left_acc = _pick(df_cards.columns, ["accountid_key", "accountid"])
    left_cust = _pick(df_cards.columns, ["customerid_key", "customerid"])
    right_acc = _pick(df_pref.columns, ["externalaccount_p", "externalaccount", "account_p", "account", "externalaccountnumber", "externalaccountno"])
    right_cust = _pick(df_pref.columns, ["custid_p", "custid", "clientid_p", "clientid"])

    _clean_blanks_to_nan(df_cards, [left_acc, left_cust])
    _clean_blanks_to_nan(df_pref, [right_acc, right_cust])

    if not all([left_acc, left_cust, right_acc, right_cust]) or df_pref.empty:
        logging.warning("Preference dataset not suitable for join. Skipping preference join.")
        df_cards_dec_pref = df_cards.copy()
    else:
        df_cards[left_acc] = df_cards[left_acc].astype(str)
        df_cards[left_cust] = df_cards[left_cust].astype(str)
        df_pref[right_acc] = df_pref[right_acc].astype(str)
        df_pref[right_cust] = df_pref[right_cust].astype(str)

        df_join = pd.merge(
            df_cards,
            df_pref,
            how="left",
            left_on=[left_acc, left_cust],
            right_on=[right_acc, right_cust],
            suffixes=("", "_p"),
        )

        if "eventtimestamp" in df_join.columns and "eventtimestamp_p" in df_join.columns:
            df_join["dec_tm_ge_pref_tm"] = (
                df_join["eventtimestamp"] > df_join["eventtimestamp_p"]
            ).map({True: "Y", False: "N"}).fillna("N")

        df_cards_dec_pref = (
            df_join.drop_duplicates(subset=["decisionid"], keep="first")
            if "decisionid" in df_join.columns
            else df_join.copy()
        )

    if not df_inbox.empty:
        df_inbox = (
            df_inbox.sort_values(by=["decisionid_a", "eventtimestamp_a"], ascending=[True, False])
            .drop_duplicates(subset=["decisionid_a"], keep="first")
        )

        df_all = pd.merge(
            df_cards_dec_pref,
            df_inbox,
            how="left",
            left_on="decisionid",
            right_on="decisionid_a",
            suffixes=("", "_a"),
        )
    else:
        logging.warning("Inbox dataset empty. Proceeding without inbox join.")
        df_all = df_cards_dec_pref.copy()

    return df_all, out_dir


# ========= Stage 5: Aggregations, SLA Checks, and KPI Computations =========

def compute_alert_metrics(df_all: pd.DataFrame, out_dir: Path, report_dt: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_final = df_all.copy()

    # Timestamp parsing
    ts_cols = [
        "eventtimestamp",
        "eventtimestamp_p",
        "eventtimestamp_a",
        "ess_process_timestamp",
        "ess_src_event_timestamp",
        "ess_process_timestamp_p",
        "ess_src_event_timestamp_p",
        "ess_process_timestamp_a",
    ]
    for col in ts_cols:
        if col in df_final.columns:
            df_final[col] = pd.to_datetime(df_final[col], errors="coerce")

    # Date parts
    if "eventtimestamp" in df_final.columns:
        df_final["event_date"] = df_final["eventtimestamp"].dt.date
        df_final["decision_date"] = df_final["event_date"]
    if "ess_src_event_timestamp" in df_final.columns:
        df_final["src_event_date"] = df_final["ess_src_event_timestamp"].dt.date
    if "ess_process_timestamp" in df_final.columns:
        df_final["process_date"] = df_final["ess_process_timestamp"].dt.date

    # Numeric coercions
    for numcol in ["alertamount", "thresholdamount"]:
        if numcol in df_final.columns:
            df_final[numcol] = pd.to_numeric(df_final[numcol], errors="coerce")

    # Threshold limit check
    if {"alertamount", "thresholdamount"}.issubset(df_final.columns):
        df_final["threshold_limit_check"] = (
            df_final["alertamount"] < df_final["thresholdamount"]
        ).map({True: "Y", False: "N"})
    else:
        df_final["threshold_limit_check"] = "N"

    # Missing indicator
    df_final["found_missing"] = (
        df_final.get("eventtimestamp_a").isna()
        | df_final.get("eventtimestamp").isna()
    ).map({True: "Y", False: "N"})

    # SLA timing
    if {"eventtimestamp_a", "eventtimestamp"}.issubset(df_final.columns):
        df_final["time_diff"] = (
            df_final["eventtimestamp_a"] - df_final["eventtimestamp"]
        ).dt.total_seconds()
    else:
        df_final["time_diff"] = np.nan

    df_final["sla_ind"] = np.where(
        pd.notna(df_final["time_diff"]) & (df_final["time_diff"] <= 1800),
        "Y",
        "N",
    )

    # SLA Bucketing
    bins = [
        -float("inf"),
        0,
        1800,
        3600,
        7200,
        10800,
        14400,
        18000,
        21600,
        25200,
        28800,
        32400,
        36000,
        86400,
        172800,
        259200,
        float("inf"),
    ]
    labels = [
        "00 - <0s",
        "01 - ≤30m",
        "02 - 30–60m",
        "03 - 1–2h",
        "04 - 2–3h",
        "05 - 3–4h",
        "06 - 4–5h",
        "07 - 5–6h",
        "08 - 6–7h",
        "09 - 7–8h",
        "10 - 8–9h",
        "11 - 9–10h",
        "12 - 10–24h",
        "13 - 1–2d",
        "14 - 2–3d",
        "15 - >3d",
    ]
    df_final["alert_time"] = pd.cut(df_final["time_diff"], bins=bins, labels=labels, right=True)
    if hasattr(df_final["alert_time"], "cat"):
        df_final["alert_time"] = df_final["alert_time"].cat.add_categories(["99 - Missing"])
    df_final["alert_time"] = df_final["alert_time"].fillna("99 - Missing")

    # Snap date = next Wednesday from decision_date
    def _snapdate(d):
        if pd.isna(d):
            return pd.NaT
        if not isinstance(d, (pd.Timestamp, datetime, date)):
            return pd.NaT
        d2 = pd.to_datetime(d).date()
        weekday = d2.weekday()  # Wed=2
        days_to_wed = (2 - weekday) % 7
        return d2 + timedelta(days=days_to_wed)

    df_final["snap_date"] = df_final["decision_date"].apply(_snapdate)
    df_final["report_dt"] = report_dt

    # Weekly sampling for Accuracy section
    def sample_by_day(df: pd.DataFrame, size: int = 10, seed: int = 42) -> pd.DataFrame:
        out = []
        for _, g in df.groupby("decision_date"):
            if len(g) <= size:
                out.append(g.copy())
            else:
                out.append(g.sample(n=size, random_state=seed))
        return pd.concat(out, ignore_index=True) if out else df.head(0)

    sample_df = sample_by_day(df_final, size=10)

    # Accuracy KPI
    accuracy_df = (
        sample_df.assign(
            ControlRisk="Accuracy",
            TestType="Sample",
            ROE="Alert010_Accuracy_AvailableCredit",
            CommentCode=lambda d: d["threshold_limit_check"].map(lambda x: "COM6" if x == "Y" else "COM5"),
            SegmentID=lambda d: pd.to_datetime(d["decision_date"], errors="coerce").dt.strftime("%Y%m%d"),
            DateCompleted=report_dt,
        )
        .groupby(["ControlRisk", "TestType", "ROE", "CommentCode", "SegmentID", "DateCompleted", "snap_date"], dropna=False)
        .agg(Volume=("decisionid", "count"), AlertAmount=("alertamount", "sum"), ThresholdAmount=("thresholdamount", "sum"))
        .reset_index()
    )

    # Timeliness KPI
    timeliness_df = (
        df_final.assign(
            ControlRisk="Timeliness",
            TestType="Anomaly",
            ROE="Alert011_Timeliness_SLA",
            CommentCode=lambda d: d["sla_ind"].map(lambda x: "COM6" if x == "Y" else "COM5"),
            SegmentID=lambda d: pd.to_datetime(d["decision_date"], errors="coerce").dt.strftime("%Y%m%d"),
            DateCompleted=report_dt,
        )
        .groupby(["ControlRisk", "TestType", "ROE", "CommentCode", "SegmentID", "DateCompleted", "snap_date"], dropna=False)
        .agg(Volume=("decisionid", "count"), AlertAmount=("alertamount", "sum"), ThresholdAmount=("thresholdamount", "sum"))
        .reset_index()
    )

    # Completeness KPI
    completeness_df = (
        df_final.assign(
            ControlRisk="Completeness",
            TestType="Reconciliation",
            ROE="Alert012_Completeness_AllClients",
            CommentCode=lambda d: d["decisionid_a"].fillna("").map(lambda x: "CON9" if x else "CON8"),
            SegmentID=lambda d: pd.to_datetime(d["decision_date"], errors="coerce").dt.strftime("%Y%m%d"),
            DateCompleted=report_dt,
        )
        .groupby(["ControlRisk", "TestType", "ROE", "CommentCode", "SegmentID", "DateCompleted", "snap_date"], dropna=False)
        .agg(Volume=("decisionid", "count"), AlertAmount=("alertamount", "sum"), ThresholdAmount=("thresholdamount", "sum"))
        .reset_index()
    )

    # Combined weekly AC sheet
    alert_cards_ac_week = pd.concat([accuracy_df, timeliness_df, completeness_df], ignore_index=True)
    alert_cards_ac_week.insert(0, "RegulatoryName", "C86")
    alert_cards_ac_week.insert(1, "LOB", "Credit Cards")

    logging.info("Aggregations complete: %d total KPI rows", len(alert_cards_ac_week))
    return df_final, alert_cards_ac_week


# ========= Stage 6: Excel Output and Historical Append Handling =========

def export_outputs(
    df_final: pd.DataFrame,
    alert_cards_ac_week: pd.DataFrame,
    out_dir: Path,
    conf: dict,
    report_dt: date,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Excel outputs
    xlsx_main = out_dir / "cards_alert_final.xlsx"
    df_final.to_excel(xlsx_main, sheet_name="cards_alert_final", index=False)
    logging.info("Wrote %s", xlsx_main)

    xlsx_ac = out_dir / "Alert_Cards_AC_week.xlsx"
    alert_cards_ac_week.to_excel(xlsx_ac, sheet_name="Alert_Cards_AC_week", index=False)
    logging.info("Wrote %s", xlsx_ac)

    logging.info(
        "Final counts: rows=%d | unique decisionIds=%d",
        len(df_final),
        df_final["decisionid"].nunique() if "decisionid" in df_final.columns else -1,
    )

    # Historical append (XLSX)
    hist_path = Path(conf["out_root"]).parent / "alert_cards_ac.xlsx"
    hist_path.parent.mkdir(parents=True, exist_ok=True)

    df_new = alert_cards_ac_week.copy()
    df_new["SnapDate"] = pd.to_datetime(report_dt).strftime("%Y-%m-%d")

    if hist_path.exists():
        logging.info("Reading existing history: %s", hist_path)
        df_hist = pd.read_excel(hist_path, dtype=str)

        if "SnapDate" in df_hist.columns:
            df_hist["SnapDate"] = pd.to_datetime(df_hist["SnapDate"], errors="coerce").dt.strftime("%Y-%m-%d")

        df_hist = df_hist[~df_hist["SnapDate"].isin(df_new["SnapDate"])]
        df_combined = pd.concat([df_hist, df_new], ignore_index=True)
    else:
        logging.warning("No historical file found; creating new.")
        df_combined = df_new

    if "SnapDate" in df_combined.columns:
        df_combined["_sort"] = pd.to_datetime(df_combined["SnapDate"], errors="coerce")
        df_combined = df_combined.sort_values("_sort").drop(columns="_sort")

    df_combined.to_excel(hist_path, sheet_name="alert_cards_ac", index=False)
    logging.info("Updated historical file: %s", hist_path)


# ---------- Pipeline Runner ----------
def main_pipeline(config_path: Path):
    conf = _bootstrap(config_path)
    df_all, out_dir = run_pipeline(conf)
    rpt_dt = date.today()
    df_final, alert_cards_ac_week = compute_alert_metrics(df_all, out_dir, rpt_dt)
    export_outputs(df_final, alert_cards_ac_week, out_dir, conf, rpt_dt)
    logging.info("✅ Pipeline completed successfully.")


if __name__ == "__main__":
    main_pipeline(Path("config.ini"))
