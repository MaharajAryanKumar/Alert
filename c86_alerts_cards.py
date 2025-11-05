# c86_alerts_cards.py
# Port of c86_Alerts_Cards.sas to Python + Trino/Starburst
# Author: (you)
# Python 3.10+, pandas 2.x

import os
import sys
import math
import logging
import numpy as np  # <-- Added for vectorized operations
from pathlib import Path
from datetime import datetime, date, timedelta, timezone

import pandas as pd

# ---- Optional: if you have trino installed; otherwise 'pip install trino' ----
import trino
from trino.auth import BasicAuthentication
# from trino.auth import OAuth2Authentication  # if Starburst SSO/OIDC is used

# =============================================================================
# 0. CONFIG / LOGGING
# =============================================================================

def setup_logging(log_path: Path):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] - %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)],
    )

def load_config(config_path: Path = Path("config.ini")) -> dict:
    """
    Very small INI loader (no external deps). Or replace with configparser.
    Expects a section [trino].
    """
    import configparser
    cfg = configparser.ConfigParser()
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")
    cfg.read(config_path)
    c = cfg["trino"]
    # Optional schema/catalog defaults
    return {
        "host": c.get("host"),
        "port": c.getint("port", fallback=443),
        "user": c.get("user"),
        "password": c.get("password", fallback=None),
        "http_scheme": c.get("http_scheme", fallback="https"),
        "catalog": c.get("catalog", fallback="hive"),
        "schema": c.get("schema", fallback="default"),
        "source": c.get("source", fallback="c86_alerts_cards"),
        # Paths
        "regpath": c.get("regpath", fallback="/sas/RSD/REG"),      # used to mirror directory structure
        "out_root": c.get("out_root", fallback="./c86/output/alert/cards"),
        "log_root": c.get("log_root", fallback="./c86/log/alert/cards"),
        # Env override (DEV/PROD). SAS auto-derived; here pass explicitly if needed.
        "env": c.get("env", fallback="PROD"),
    }

def get_trino_conn(conf: dict):
    # Choose auth as applicable in your environment:
    auth = None
    if conf.get("password"):
        auth = BasicAuthentication(conf["user"], conf["password"])
    # For Starburst with OAuth2 / SSO:
    # auth = OAuth2Authentication(...)  # if required

    return trino.dbapi.connect(
        host=conf["host"],
        port=conf["port"],
        user=conf["user"],
        http_scheme=conf["http_scheme"],
        auth=auth,
        catalog=conf["catalog"],
        schema=conf["schema"],
        source=conf["source"],
    )

# =============================================================================
# 1. DATE LOGIC (parity with SAS)
# =============================================================================

def compute_dates(today: date | None = None) -> dict:
    """
    Recreates SAS date logic.
    - SAS 'week.4' interval starts on Friday (weekday 4).
    - intnx('week.4', report_dt, 0) finds the Friday of the week containing report_dt.
    - end_dt = Friday - 2 days = Wednesday.
    """
    if not today:
        today = date.today()

    # Constants from SAS
    start_dt_ini = date(2022, 6, 30)    # '30JUN2022'd
    week_end_dt_ini = date(2022, 7, 25)  # '25JUL2022'd

    # Find the Friday (weekday 4) that starts the current 'week.4' period
    weekday = today.weekday() # Monday=0 ... Friday=4 ... Sunday=6
    days_since_friday = (weekday - 4) % 7
    week_start_friday = today - timedelta(days=days_since_friday)

    # SAS: %let end_dt = %eval(%sysfunc(intnx(week.4, &report_dt, 0))-2);
    end_dt = week_start_friday - timedelta(days=2) # This is a Wednesday

    # --- Rest of the logic ---
    if end_dt < week_end_dt_ini:
        start_dt = start_dt_ini
    else:
        start_dt = end_dt - timedelta(days=6) # 7-day period ending on end_dt

    week_start_dt = start_dt
    week_end_dt = end_dt
    pardt = start_dt - timedelta(days=7)
    week_end_dt_p1 = end_dt + timedelta(days=1)

    label = today.strftime("%Y%m%d") # SAS yyyymmddn8.

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

# =============================================================================
# 2. SQL HELPERS (Trino dialect)
# =============================================================================

# In Trino:
# - Maps: element_at(eventAttributes, 'key')
# - JSON scalar from a JSON string: json_extract_scalar(json_col, '$.path')
# - From ISO8601 text -> timestamp: from_iso8601_timestamp('2025-03-01T12:34:56Z')
#   (yields TIMESTAMP WITH TIME ZONE; cast to TIMESTAMP if needed)
# We’ll wrap in try() to avoid hard failures on bad rows.

# Common projections for timestamp/text fields:
ESS_PROCESS_TS = "try(CAST(from_iso8601_timestamp(element_at(eventAttributes, 'ess_process_timestamp')) AS timestamp))"
ESS_SRC_TS     = "try(CAST(from_iso8601_timestamp(element_at(eventAttributes, 'ess_src_event_timestamp')) AS timestamp))"
SRC_HDR        = "try(json_parse(element_at(eventAttributes, 'SourceEventHeader')))"
EVT_PAYLOAD    = "try(json_parse(element_at(eventAttributes, 'eventPayload')))"
EVT_ID         = "try(json_extract_scalar(" + SRC_HDR + ", '$.eventId'))"
EVT_TS         = "try(CAST(from_iso8601_timestamp(json_extract_scalar(" + SRC_HDR + ", '$.eventTimestamp')) AS timestamp))"

def q_xb80_cards(schema: str, week_start: date, week_end_p1: date, pardt: date) -> str:
    return f"""
    SELECT
        event_activity_type,
        source_event_id,
        partition_date,
        {ESS_PROCESS_TS} AS ess_process_timestamp,
        {ESS_SRC_TS}     AS ess_src_event_timestamp,
        {EVT_ID}         AS eventId,
        {EVT_TS}         AS eventTimestamp,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.accountId'))         AS accountId,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.alertType'))         AS alertType,
        try(CAST(json_extract_scalar({EVT_PAYLOAD}, '$.thresholdAmount') AS DECIMAL(10,2))) AS thresholdAmount,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.customerID'))        AS customerID,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.accountCurrency'))   AS accountCurrency,
        try(CAST(json_extract_scalar({EVT_PAYLOAD}, '$.creditLimit') AS DECIMAL(10,2)))     AS creditLimit,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.maskedAccount'))     AS maskedAccount,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.decisionId'))        AS decisionId,
        try(CAST(json_extract_scalar({EVT_PAYLOAD}, '$.alertAmount') AS DECIMAL(10,2)))     AS alertAmount
    FROM {schema}.xb80_credit_card_system_interface
    WHERE
        partition_date > DATE '{pardt.isoformat()}'
        AND event_activity_type = 'Alert Decision Cards'
        AND try(json_extract_scalar({EVT_PAYLOAD}, '$.alertType')) = 'AVAIL_CREDIT_REMAINING'
        AND event_timestamp BETWEEN TIMESTAMP '{week_start.isoformat()} 00:00:00'
                              AND TIMESTAMP '{week_end_p1.isoformat()} 00:00:00'
    """

def q_cards_dec_pref(schema: str, week_end_p1: date) -> str:
    # initial load (fixed) UNION daily incremental (post week_end_dt_p1)
    a = f"""
        SELECT
            {EVT_TS} AS event_timestamp_p,
            event_channel_type AS event_channel_type_p,
            event_activity_type AS event_activity_type_p,
            partition_date AS partition_date_p,
            {ESS_PROCESS_TS} AS ess_process_timestamp_p,
            {ESS_SRC_TS} AS ess_src_event_timestamp_p,
            {EVT_TS} AS eventTimestamp_p,
            try(json_extract_scalar({SRC_HDR}, '$.eventActivityName')) AS eventActivityName_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.preferenceType')) AS preferenceType_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.clientID')) AS clientID_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.isBusiness')) AS isBusiness_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.sendAlertEligible')) AS sendAlertEligible_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.active')) AS active_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.threshold')) AS threshold_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.custID')) AS custID_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.account')) AS account_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.maskAccountNo')) AS maskedAccountNo_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.externalAccount')) AS externalAccount_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.productType')) AS productType_p
        FROM {schema}.ffs0_client_alert_preferences_dep_initial_load
        WHERE event_activity_type IN ('Create Account Preference','Update Account Preference')
          AND try(json_extract_scalar({EVT_PAYLOAD}, '$.preferenceType')) = 'AVAIL_CREDIT_REMAINING'
          AND try(json_extract_scalar({EVT_PAYLOAD}, '$.productType'))   = 'CREDIT_CARD'
          AND partition_date = DATE '2022-03-24'
    """
    b = f"""
        SELECT
            {EVT_TS} AS event_timestamp_p,
            event_channel_type AS event_channel_type_p,
            event_activity_type AS event_activity_type_p,
            partition_date AS partition_date_p,
            {ESS_PROCESS_TS} AS ess_process_timestamp_p,
            {ESS_SRC_TS} AS ess_src_event_timestamp_p,
            {EVT_TS} AS eventTimestamp_p,
            try(json_extract_scalar({SRC_HDR}, '$.eventActivityName')) AS eventActivityName_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.preferenceType')) AS preferenceType_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.clientID')) AS clientID_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.isBusiness')) AS isBusiness_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.sendAlertEligible')) AS sendAlertEligible_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.active')) AS active_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.threshold')) AS threshold_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.custID')) AS custID_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.account')) AS account_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.maskAccountNo')) AS maskedAccountNo_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.externalAccount')) AS externalAccount_p,
            try(json_extract_scalar({EVT_PAYLOAD}, '$.productType')) AS productType_p
        FROM {schema}.ffs0_client_alert_preferences_dep
        WHERE event_timestamp > TIMESTAMP '{week_end_p1.isoformat()} 00:00:00'
          AND event_activity_type IN ('Create Account Preference','Update Account Preference')
          AND try(json_extract_scalar({EVT_PAYLOAD}, '$.preferenceType')) = 'AVAIL_CREDIT_REMAINING'
          AND try(json_extract_scalar({EVT_PAYLOAD}, '$.productType'))   = 'CREDIT_CARD'
    """
    # <-- CORRECTED: Use UNION (not UNION ALL) to match SAS
    return f"SELECT * FROM ({a}) UNION SELECT * FROM ({b})"

def q_fft0_inbox(schema: str, week_start: date) -> str:
    return f"""
    SELECT
        {ESS_PROCESS_TS} AS ess_process_timestamp_a,
        event_activity_type AS event_activity_type_a,
        source_event_id AS source_event_id_a,
        partition_date AS partition_date_a,
        event_timestamp AS event_timestamp_a,
        {EVT_ID} AS eventId_a,
        {EVT_TS} AS eventTimestamp_a,
        try(json_extract_scalar({SRC_HDR}, '$.eventActivityName')) AS eventActivityName_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.alertSent')) AS alertSent_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.sendInbox')) AS sendInbox_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.alertType')) AS alertType_a,
        try(CAST(json_extract_scalar({EVT_PAYLOAD}, '$.thresholdAmount') AS DECIMAL(12,2))) AS thresholdAmount_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.sendSMS')) AS sendSMS_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.sendPush')) AS sendPush_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.maskedAccount')) AS maskedAccount_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.reasonCode')) AS reasonCode_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.decisionId')) AS decisionId_a,
        try(CAST(json_extract_scalar({EVT_PAYLOAD}, '$.alertAmount') AS DECIMAL(12,2))) AS alertAmount_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.accountID')) AS accountId_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.account')) AS account_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.accountProduct')) AS accountProduct_a,
        try(json_extract_scalar({EVT_PAYLOAD}, '$.sendEmail')) AS sendEmail_a
    FROM {schema}.fft0_alert_inbox_dep
    WHERE event_activity_type = 'Alert Delivery Audit'
      AND try(json_extract_scalar({EVT_PAYLOAD}, '$.alertType')) = 'AVAIL_CREDIT_REMAINING'
      AND event_timestamp > TIMESTAMP '{week_start.isoformat()} 00:00:00'
    """

def read_trino_df(conn, sql: str) -> pd.DataFrame:
    logging.info("Running SQL:\n%s", sql)
    return pd.read_sql(sql, conn)

# =============================================================================
# 3. DATA PROCESSING (pandas parity with SAS)
# =============================================================================

def to_date(x):
    if pd.isna(x):
        return pd.NaT
    if isinstance(x, pd.Timestamp):
        return x.normalize()
    try:
        return pd.to_datetime(x).normalize()
    except Exception:
        return pd.NaT

def run_pipeline(conf: dict):
    # Paths
    label = compute_dates()["label"]  # but we’ll recompute below to keep single source of truth
    dt = compute_dates()
    label = dt["label"]

    out_root = Path(conf["out_root"])
    log_root = Path(conf["log_root"])
    out_dir = out_root / label
    out_dir.mkdir(parents=True, exist_ok=True)
    (log_root).mkdir(parents=True, exist_ok=True)
    log_file = log_root / f"c8600J_Alerts_Cards_{label}.log"
    setup_logging(log_file)

    logging.info("Environment=%s  Catalog=%s Schema=%s", conf["env"], conf["catalog"], conf["schema"])
    logging.info("Output dir: %s", out_dir.resolve())

    # Connect
    with get_trino_conn(conf) as conn:
        schema = f"{conf['catalog']}.{conf['schema']}"

        # xb80_cards
        df_cards = read_trino_df(
            conn,
            q_xb80_cards(schema, dt["week_start_dt"], dt["week_end_dt_p1"], dt["pardt"])
        )

        # cards_dec_pref (initial + incremental)
        df_pref = read_trino_df(conn, q_cards_dec_pref(schema, dt["week_end_dt_p1"]))

        # fft0_inbox
        df_inbox = read_trino_df(conn, q_fft0_inbox(schema, dt["week_start_dt"]))

    # Join logic (as in SAS)
    # xb80_cards left join cards_dec_pref on (accountId == externalAccount_p and customerID == custID_p)
    # Then create dec_tm_ge_pref_tm = Y if eventTimestamp > eventTimestamp_p
    for ts_col in ["eventTimestamp", "event_timestamp_p", "eventTimestamp_p",
                   "ess_process_timestamp", "ess_src_event_timestamp",
                   "ess_process_timestamp_p", "ess_src_event_timestamp_p"]:
        if ts_col in df_cards.columns:
            df_cards[ts_col] = pd.to_datetime(df_cards[ts_col], errors="coerce")
        if ts_col in df_pref.columns:
            df_pref[ts_col] = pd.to_datetime(df_pref[ts_col], errors="coerce")

    # Lowercase key columns for reliable merge
    df_cards = df_cards.rename(columns={"accountid": "accountid_key", "customerid": "customerid_key"})
    df_pref = df_pref.rename(columns={"externalaccount_p": "externalaccount_p_key", "custid_p": "custid_p_key"})

    df_join = pd.merge(
        df_cards,
        df_pref,
        how="left",
        left_on=["accountid_key", "customerid_key"],
        right_on=["externalaccount_p_key", "custid_p_key"],
        suffixes=("", "_p2"),
    )

    df_join["dec_tm_ge_pref_tm"] = (
        (df_join["eventTimestamp"] > df_join["eventTimestamp_p"]).map({True: "Y", False: "N"})
    )
    df_join["dec_tm_ge_pref_tm"] = df_join["dec_tm_ge_pref_tm"].fillna("N")


    # <-- CORRECTED: De-dup to one row per decisionId matching SAS sort order
    df_join = df_join.sort_values(
        by=[
            "decisionid",
            "eventTimestamp",
            "accountid_key",
            "customerid_key",
            "dec_tm_ge_pref_tm",
            "eventTimestamp_p",
            "externalaccount_p_key"
        ],
        ascending=[
            True,  # decisionid
            True,  # eventTimestamp
            True,  # accountid
            True,  # customerid
            False, # descending dec_tm_ge_pref_tm
            False, # descending eventTimestamp_p
            False  # descending externalAccount_p
        ]
    )
    df_cards_dec_pref2 = df_join.drop_duplicates(subset=["decisionid"], keep="first")


    # Prepare inbox for left join on decisionId
    for ts_col in ["eventTimestamp_a", "ess_process_timestamp_a"]:
        if ts_col in df_inbox.columns:
            df_inbox[ts_col] = pd.to_datetime(df_inbox[ts_col], errors="coerce")

    # De-dup inbox by decisionId_a (keep first by latest timestamp)
    df_inbox = df_inbox.sort_values(by=["decisionid_a", "eventTimestamp_a"], ascending=[True, False]) \
                       .drop_duplicates(subset=["decisionid_a"], keep="first")

    # Join: a.decisionId = b.decisionId_a
    df_all = pd.merge(
        df_cards_dec_pref2,
        df_inbox,
        how="left",
        left_on="decisionid",
        right_on="decisionid_a",
        suffixes=("", "_a2"),
    )

    # === cards_alert_final ===
    # Filters: if isbusiness_p == 'true' and dec_tm_ge_pref_tm == 'Y' then delete
    # Build fields / time windows:
    def to_bool_str(x):
        return str(x).lower()

    df_final = df_all.copy()
    df_final = df_final[~((df_final["isbusiness_p"].map(to_bool_str) == "true") &
                          (df_final["dec_tm_ge_pref_tm"] == "Y"))]

    # Convert date parts & fields
    for col in ["ess_src_event_timestamp", "ess_process_timestamp", "eventTimestamp",
                "eventTimestamp_a"]:
        if col in df_final.columns:
            df_final[col] = pd.to_datetime(df_final[col], errors="coerce")

    df_final["src_event_date"] = df_final["ess_src_event_timestamp"].dt.date
    df_final["process_date"]   = df_final["ess_process_timestamp"].dt.date

    # decision_date = datepart(eventTimestamp); event_date = datepart(input(compress(eventTimestamp,'a'), VMDTTM26.));
    # In Trino we already parsed eventTimestamp. We'll keep decision_date = date(eventTimestamp)
    df_final["decision_date"] = df_final["eventTimestamp"].dt.date
    df_final["event_date"]    = df_final["eventTimestamp"].dt.date

    # threshold_limit_check = alertAmount < thresholdAmount
    # Convert to numeric first
    df_final["alertamount"] = pd.to_numeric(df_final["alertamount"], errors="coerce")
    df_final["thresholdamount"] = pd.to_numeric(df_final["thresholdamount"], errors="coerce")
    df_final["threshold_limit_check"] = (df_final["alertamount"] < df_final["thresholdamount"]).map({True: "Y", False: "N"})

    # Found_Missing if either eventTimestamp_a or eventTimestamp missing
    df_final["Found_Missing"] = ((df_final["eventTimestamp_a"].isna()) | (df_final["eventTimestamp"].isna())).map({True: "Y", False: "N"})

    # <-- CORRECTED: Vectorized Time_Diff in seconds (eventTimestamp_a - eventTimestamp)
    df_final["Time_Diff"] = (df_final["eventTimestamp_a"] - df_final["eventTimestamp"]).dt.total_seconds()

    # <-- CORRECTED: Vectorized SLA_Ind = 'Y' if Time_Diff <= 1800 and not missing
    df_final["SLA_Ind"] = np.where(
        (df_final["Time_Diff"].notna()) & (df_final["Time_Diff"] <= 1800),
        "Y",
        "N"
    )

    # <-- CORRECTED: Vectorized Alert_Time buckets
    bins = [
        -float('inf'), 0, 1800, 3600, 7200, 10800, 14400, 18000, 21600,
        25200, 28800, 32400, 36000, 86400, 172800, 259200, float('inf')
    ]
    labels = [
        "00 - Less than 0 seconds",
        "01 - Less than or equal to 30 minutes",
        "02 - Greater than 30 mins and less than or equal to 60 mins",
        "03 - Greater than 1 hour and less than or equal to 2 hours",
        "04 - Greater than 2 hours and less than or equal to 3 hours",
        "05 - Greater than 3 hours and less than or equal to 4 hours",
        "06 - Greater than 4 hours and less than or equal to 5 hours",
        "07 - Greater than 5 hours and less than or equal to 6 hours",
        "08 - Greater than 6 hours and less than or equal to 7 hours",
        "09 - Greater than 7 hours and less than or equal to 8 hours",
        "10 - Greater than 8 hours and less than or equal to 9 hours",
        "11 - Greater than 9 hours and less than or equal to 10 hours",
        "12 - Greater than 10 hours and less than or equal to 24 hours",
        "13 - Greater than 1 day and less than or equal to 2 days",
        "14 - Greater than 2 days and less than or equal to 3 days",
        "15 - Greater than 3 days"
    ]
    df_final["Alert_Time"] = pd.cut(df_final["Time_Diff"], bins=bins, labels=labels, right=True)
    df_final["Alert_Time"] = df_final["Alert_Time"].cat.add_categories("99 - Timestamp is missing")
    df_final["Alert_Time"] = df_final["Alert_Time"].fillna("99 - Timestamp is missing")


    # === Summaries (Accuracy, Timeliness, Completeness_Recon) ===

    # report_dt and SnapDate alignment
    dt_all = compute_dates()
    report_dt = dt_all["report_dt"]

    # helper: SnapDate = intnx('week.3', decision_date, 0, 'e')
    # week.3 = week ends on Wednesday; emulate “end” of that week
    def snapdate(d: date):
        if pd.isna(d): return pd.NaT
        # find Wednesday of that week (weekday=2), then use that date
        weekday = d.weekday()
        days_to_wed_end = (2 - weekday) % 7
        return d + timedelta(days=days_to_wed_end)

    # Accuracy (on sample). SAS drew a 10-sample stratified; here we’ll compute on df_final first,
    # then separately draw sample for “detail” like SAS.
    # SAS sample table is ‘dataout.alert_card_base_samples’; we’ll generate similarly:
    # create per-decision_date sample of size 10 (SRS)
    def sample_by_day(df, size=10, seed=42):
        out = []
        for k, g in df.groupby("decision_date"):
            if len(g) <= size:
                out.append(g.copy())
            else:
                out.append(g.sample(n=size, random_state=seed))
        return pd.concat(out, ignore_index=True) if out else df.head(0)

    sample_df = sample_by_day(df_final, size=10)

    # Accuracy summary
    acc = (
        sample_df.assign(
            ControlRisk="Accuracy",
            TestType="Sample",
            RDE="Alert010_Accuracy_Available_Credit",
            CommentCode=lambda d: d["threshold_limit_check"].map(lambda x: "COM16" if x == "Y" else "COM19"),
            Segment10=lambda d: pd.to_datetime(d["decision_date"]).dt.strftime("%Y%m%d"),
            DateCompleted=report_dt,
            SnapDate=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
        )
        .groupby(["ControlRisk","TestType","RDE","CommentCode","Segment10","DateCompleted","SnapDate"], dropna=False)
        .agg(Volume=("decisionid","count"),
             bal=("alertamount","sum"),
             Amount=("thresholdamount","sum"))
        .reset_index()
    )

    # Timeliness summary
    tml = (
        df_final.assign(
            ControlRisk="Timeliness",
            TestType="Anomaly",
            RDE="Alert011_Timeliness_SLA",
            CommentCode=lambda d: d["SLA_Ind"].map(lambda x: "COM16" if x == "Y" else "COM19"),
            Segment10=lambda d: pd.to_datetime(d["decision_date"]).dt.strftime("%Y%m%d"),
            DateCompleted=report_dt,
            SnapDate=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
        )
        .groupby(["ControlRisk","TestType","RDE","CommentCode","Segment10","DateCompleted","SnapDate"], dropna=False)
        .agg(Volume=("decisionid","count"),
             bal=("alertamount","sum"),
             Amount=("thresholdamount","sum"))
        .reset_index()
    )

    # Completeness summary (recon)
    cpl = (
        df_final.assign(
            ControlRisk="Completeness",
            TestType="Reconciliation",
            RDE="Alert012_Completeness_All_Clients",
            CommentCode=lambda d: d["decisionid_a"].fillna("").map(lambda x: "COM16" if x != "" else "COM19"),
            Segment10=lambda d: pd.to_datetime(d["decision_date"]).dt.strftime("%Y%m"),
            DateCompleted=report_dt,
            SnapDate=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
        )
        .groupby(["ControlRisk","TestType","RDE","CommentCode","Segment10","DateCompleted","SnapDate"], dropna=False)
        .agg(Volume=("decisionid","count"),
             bal=("alertamount","sum"),
             Amount=("thresholdamount","sum"))
        .reset_index()
    )

    alert_cards_ac_week = pd.concat([acc, tml, cpl], ignore_index=True)
    # Add fixed columns from SAS
    alert_cards_ac_week.insert(0, "RegulatoryName", "c86")
    alert_cards_ac_week.insert(1, "LOB", "Credit Cards")
    alert_cards_ac_week.insert(2, "ReportName", "c86 Alerts")
    alert_cards_ac_week.insert(5, "TestPeriod", "Portfolio")
    alert_cards_ac_week.insert(6, "ProductType", "Credit Cards")
    for col in ["SubDE","Segment","Segment2","Segment3","Segment4","Segment5","Segment6","Segment7","Segment8","Segment9","HoldoutFlag"]:
        alert_cards_ac_week[col] = "."
    alert_cards_ac_week["HoldoutFlag"] = "N"
    # Comments column: In SAS: put(CommentCode,$cmt.) ; if you have a mapping, add it; else leave blank or same as code
    alert_cards_ac_week["Comments"] = ""  # place-holder (no $cmt. format here)

    # === Detail tabs ===

    # Completeness_Fail: where decisionId_a = ''
    completeness_fail = (
        df_final[df_final["decisionid_a"].fillna("") == ""]
        .assign(
            event_month=lambda d: pd.to_datetime(d["decision_date"]).dt.strftime("%Y%m"),
            reporting_date=report_dt,
            event_week_ending=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
            LOB="Credit Cards",
            Product="Credit Cards",
            account_number=lambda d: d["accountid_key"],
            available_credit=lambda d: d["alertamount"],
            event_date=lambda d: pd.to_datetime(d["decision_date"]).dt.date,
            custid_mask=lambda d: d["customerid_key"].fillna("").str[-3:].radd("******"),
        )[[
            "event_month", "reporting_date", "event_week_ending",
            "LOB", "Product", "account_number",
            "thresholdamount", "available_credit", "decisionid", "event_date", "custid_mask"
        ]]
    )

    # Timeliness_Fail: where SLA_Ind != 'Y'
    timeliness_fail = (
        df_final[df_final["SLA_Ind"] != "Y"]
        .assign(
            event_month=lambda d: pd.to_datetime(d["decision_date"]).dt.strftime("%Y%m"),
            reporting_date=report_dt,
            event_week_ending=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
            LOB="Credit Cards",
            Product="Credit Cards",
            account_number=lambda d: d["accountid_key"],
            available_credit=lambda d: d["alertamount"],
            event_date=lambda d: pd.to_datetime(d["decision_date"]).dt.date,
            total_minutes=lambda d: (d["Time_Diff"] / 60.0).apply(lambda x: None if pd.isna(x) else math.ceil(x)),
            custid_mask=lambda d: d["customerid_key"].fillna("").str[-3:].radd("******"),
        )[[
            "event_month","reporting_date","event_week_ending","LOB","Product",
            "account_number","thresholdamount","available_credit","decisionid",
            "event_date","eventTimestamp","eventTimestamp_a","total_minutes","custid_mask"
        ]]
    )

    # Accuracy_Fail: from the sample set where threshold_limit_check != 'Y'
    accuracy_fail = (
        sample_df[sample_df["threshold_limit_check"] != "Y"]
        .assign(
            event_month=lambda d: pd.to_datetime(d["decision_date"]).dt.strftime("%Y%m"),
            reporting_date=report_dt,
            event_week_ending=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
            LOB="Credit Cards",
            Product="Credit Cards",
            account_number=lambda d: d["accountid_key"],
            decision="AlertDecision",
            available_credit=lambda d: d["alertamount"],
            event_date=lambda d: pd.to_datetime(d["decision_date"]).dt.date,
            custid_mask=lambda d: d["customerid_key"].fillna("").str[-3:].radd("******"),
        )[[
            "event_month","reporting_date","event_week_ending","LOB","Product",
            "account_number","decision","thresholdamount","available_credit","decisionid",
            "event_date","custid_mask"
        ]]
    )

    # === OUTPUT ===
    # Primary “final” table
    df_final_path = out_dir / "cards_alert_final.parquet"
    df_final.to_parquet(df_final_path, index=False)
    logging.info("Wrote %s", df_final_path)

    # Summary "autocomplete" weekly table
    ac_week_path = out_dir / "Alert_Cards_AC_week.parquet"
    alert_cards_ac_week.to_parquet(ac_week_path, index=False)
    logging.info("WWrote %s", ac_week_path)

    # Excel detail exports
    xlsx1 = out_dir / "Alert_Cards_Completeness_Detail.xlsx"
    completeness_fail.to_excel(xlsx1, sheet_name="Alert_Cards_Completeness_Detail", index=False)
    logging.info("Wrote %s", xlsx1)

    xlsx2 = out_dir / "Alert_Cards_Timeliness_Detail.xlsx"
    timeliness_fail.to_excel(xlsx2, sheet_name="Alert_Cards_Timeliness_Detail", index=False)
    logging.info("Wrote %s", xlsx2)

procs    
    xlsx3 = out_dir / "Alert_Cards_Accuracy_Detail.xlsx"
    accuracy_fail.to_excel(xlsx3, sheet_name="Alert_Cards_Accuracy_Detail", index=False)
    logging.info("Wrote %s", xlsx3)

    # Basic counts (SAS printed some PROC FREQ outputs)
    logging.info(
        "Final counts: rows=%d, distinct decisionId=%d",
        len(df_final),
        df_final["decisionid"].nunique()
    )

    # <-- CORRECTED: HISTORICAL APPEND LOGIC (REPLACES SAS PROC APPEND/SQL) ---
    logging.info("Updating historical autocomplete file...")

    # The SAS 'ac' libname pointed to the parent output directory
    ac_lib_path = Path(conf["out_root"]).parent # Assumes ac. is one level up
    ac_lib_path.mkdir(parents=True, exist_ok=True)
    historical_ac_file = ac_lib_path / "alert_cards_ac.parquet"

    df_new_week = alert_cards_ac_week.copy()
    df_new_week["SnapDate"] = pd.to_datetime(df_new_week["SnapDate"]).dt.date

    if historical_ac_file.exists():
        logging.info("Reading existing history file: %s", historical_ac_file)
        df_history = pd.read_parquet(historical_ac_file)
        df_history["SnapDate"] = pd.to_datetime(df_history["SnapDate"]).dt.date

        # Get list of SnapDates from the new data
        new_snapdates = df_new_week["SnapDate"].unique()

        # Filter history to remove any data that will be replaced
        df_history_filtered = df_history[~df_history["SnapDate"].isin(new_snapdates)]

        # Append new data to filtered history
        df_combined_history = pd.concat([df_history_filtered, df_new_week], ignore_index=True)
        logging.info("Appended new data, removing %d old snapdates.", len(new_snapdates))
    else:
        logging.warning("History file not found. Creating new one.")
        df_combined_history = df_new_week

    # Sort and save
    df_combined_history = df_combined_history.sort_values(by="SnapDate")
    df_combined_history.to_parquet(historical_ac_file, index=False)
    logging.info("Wrote updated historical file: %s (Total rows: %d)", historical_ac_file, len(df_combined_history))


# =============================================================================
# 4. MAIN
# =============================================================================

if __name__ == "__main__":
    cfg = load_config(Path("config.ini"))
    run_pipeline(cfg)
