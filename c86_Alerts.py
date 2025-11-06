# c86_alerts_cards.py
# Port of c86_Alerts_Cards.sas to Python + Trino/Starburst
# Python 3.10+, pandas 2.x

import sys
import math
import logging
from pathlib import Path
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd

import trino
from trino.auth import BasicAuthentication

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
    import configparser
    cfg = configparser.ConfigParser()
    if not config_path.exists():
        raise FileNotFoundError(f"Missing config file: {config_path}")
    cfg.read(config_path)
    c = cfg["trino"]
    return {
        "host": c.get("host"),
        "port": c.getint("port", fallback=443),
        "user": c.get("user"),
        "password": c.get("password", fallback=None),
        "http_scheme": c.get("http_scheme", fallback="https"),
        "catalog": c.get("catalog", fallback="hive"),
        "schema": c.get("schema", fallback="default"),
        "source": c.get("source", fallback="c86_alerts_cards"),
        "regpath": c.get("regpath", fallback="/sas/RSD/REG"),
        "out_root": c.get("out_root", fallback="./c86/output/alert/cards"),
        "log_root": c.get("log_root", fallback="./c86/log/alert/cards"),
        "env": c.get("env", fallback="PROD"),
    }

def get_trino_conn(conf: dict):
    auth = None
    if conf.get("password"):
        auth = BasicAuthentication(conf["user"], conf["password"])
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
# 1. DATE LOGIC (SAS parity)
# =============================================================================

def compute_dates(today: date | None = None) -> dict:
    """
    SAS 'week.4' starts on Friday; 'end_dt' is Friday-2 = Wednesday.
    """
    if not today:
        today = date.today()

    start_dt_ini = date(2022, 6, 30)     # 30JUN2022
    week_end_dt_ini = date(2022, 7, 25)  # 25JUL2022

    weekday = today.weekday()            # Mon=0 ... Fri=4 ... Sun=6
    days_since_friday = (weekday - 4) % 7
    week_start_friday = today - timedelta(days=days_since_friday)
    end_dt = week_start_friday - timedelta(days=2)  # Wednesday

    if end_dt < week_end_dt_ini:
        start_dt = start_dt_ini
    else:
        start_dt = end_dt - timedelta(days=6)

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

# =============================================================================
# 2. SQL HELPERS (Trino/Starburst)
# =============================================================================

# Hardened ISO8601 → timestamp (handles space/Z)
def _SAFE_TS(expr: str) -> str:
    return f"""
    TRY(
      CAST(
        from_iso8601_timestamp(
          regexp_replace(
            regexp_replace({expr}, ' ', 'T'),
            'Z$', '+00:00'
          )
        ) AS timestamp
      )
    )
    """.strip()

# Common projections
ESS_PROCESS_TS = _SAFE_TS("element_at(eventAttributes, 'ess_process_timestamp')")
ESS_SRC_TS     = _SAFE_TS("element_at(eventAttributes, 'ess_src_event_timestamp')")
SRC_HDR        = "TRY(json_parse(element_at(eventAttributes, 'SourceEventHeader')))"
EVT_PAYLOAD    = "TRY(json_parse(element_at(eventAttributes, 'eventPayload')))"
EVT_ID         = f"TRY(json_extract_scalar({SRC_HDR}, '$.eventId'))"
EVT_TS         = _SAFE_TS(f"json_extract_scalar({SRC_HDR}, '$.eventTimestamp')")

def q_xb80_cards(schema: str, week_start: date, week_end_p1: date, pardt: date) -> str:
    # Use CTE to parse partition_date (YYYYMMDD) safely
    return f"""
    WITH cards AS (
      SELECT
        event_activity_type,
        source_event_id,
        partition_date,
        TRY(date_parse(partition_date, '%Y%m%d')) AS partition_date_parsed,
        {ESS_PROCESS_TS} AS ess_process_timestamp,
        {ESS_SRC_TS}     AS ess_src_event_timestamp,
        {EVT_ID}         AS eventId,
        {EVT_TS}         AS eventTimestamp,
        {EVT_PAYLOAD}    AS eventPayload
      FROM {schema}.xb80_credit_card_system_interface
    )
    SELECT
      event_activity_type,
      source_event_id,
      partition_date,
      ess_process_timestamp,
      ess_src_event_timestamp,
      eventId,
      eventTimestamp,
      TRY(json_extract_scalar(eventPayload, '$.accountId'))       AS accountId,
      TRY(json_extract_scalar(eventPayload, '$.alertType'))       AS alertType,
      TRY(CAST(json_extract_scalar(eventPayload, '$.thresholdAmount') AS DECIMAL(12,2))) AS thresholdAmount,
      TRY(json_extract_scalar(eventPayload, '$.customerID'))      AS customerID,
      TRY(json_extract_scalar(eventPayload, '$.accountCurrency')) AS accountCurrency,
      TRY(CAST(json_extract_scalar(eventPayload, '$.creditLimit') AS DECIMAL(12,2)))     AS creditLimit,
      TRY(json_extract_scalar(eventPayload, '$.maskedAccount'))   AS maskedAccount,
      TRY(json_extract_scalar(eventPayload, '$.decisionId'))      AS decisionId,
      TRY(CAST(json_extract_scalar(eventPayload, '$.alertAmount') AS DECIMAL(12,2)))     AS alertAmount
    FROM cards
    WHERE
      partition_date_parsed > DATE '{pardt.isoformat()}'
      AND event_activity_type = 'Alert Decision Cards'
      AND TRY(json_extract_scalar(eventPayload, '$.alertType')) = 'AVAIL_CREDIT_REMAINING'
      AND { _SAFE_TS("CAST(eventTimestamp AS varchar)") }
          BETWEEN TIMESTAMP '{week_start.isoformat()} 00:00:00'
              AND TIMESTAMP '{week_end_p1.isoformat()} 00:00:00'
    """

def q_cards_dec_pref(schema: str, week_end_p1: date) -> str:
    # initial + incremental; emit externalaccount_p, custid_p (lowercase aliases)
    a = f"""
      SELECT
        {EVT_TS} AS event_timestamp_p,
        event_channel_type AS event_channel_type_p,
        event_activity_type AS event_activity_type_p,
        TRY(date_parse(partition_date, '%Y%m%d')) AS partition_date_p,
        {ESS_PROCESS_TS} AS ess_process_timestamp_p,
        {ESS_SRC_TS} AS ess_src_event_timestamp_p,
        {EVT_TS} AS eventtimestamp_p,
        TRY(json_extract_scalar({SRC_HDR}, '$.eventActivityName')) AS eventactivityname_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.preferenceType')) AS preferencetype_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.clientID')) AS clientid_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.isBusiness')) AS isbusiness_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.sendAlertEligible')) AS sendalerteligible_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.active')) AS active_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.threshold')) AS threshold_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.custID')) AS custid_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.account')) AS account_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.maskAccountNo')) AS maskedaccountno_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.externalAccount')) AS externalaccount_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.productType')) AS producttype_p
      FROM {schema}.ffs0_client_alert_preferences_dep_initial_load
      WHERE event_activity_type IN ('Create Account Preference','Update Account Preference')
        AND TRY(json_extract_scalar({EVT_PAYLOAD}, '$.preferenceType')) = 'AVAIL_CREDIT_REMAINING'
        AND TRY(json_extract_scalar({EVT_PAYLOAD}, '$.productType'))   = 'CREDIT_CARD'
        AND partition_date = '20220324'
    """
    b = f"""
      SELECT
        {EVT_TS} AS event_timestamp_p,
        event_channel_type AS event_channel_type_p,
        event_activity_type AS event_activity_type_p,
        TRY(date_parse(partition_date, '%Y%m%d')) AS partition_date_p,
        {ESS_PROCESS_TS} AS ess_process_timestamp_p,
        {ESS_SRC_TS} AS ess_src_event_timestamp_p,
        {EVT_TS} AS eventtimestamp_p,
        TRY(json_extract_scalar({SRC_HDR}, '$.eventActivityName')) AS eventactivityname_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.preferenceType')) AS preferencetype_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.clientID')) AS clientid_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.isBusiness')) AS isbusiness_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.sendAlertEligible')) AS sendalerteligible_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.active')) AS active_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.threshold')) AS threshold_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.custID')) AS custid_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.account')) AS account_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.maskAccountNo')) AS maskedaccountno_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.externalAccount')) AS externalaccount_p,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.productType')) AS producttype_p
      FROM {schema}.ffs0_client_alert_preferences_dep
      WHERE event_timestamp > TIMESTAMP '{week_end_p1.isoformat()} 00:00:00'
        AND event_activity_type IN ('Create Account Preference','Update Account Preference')
        AND TRY(json_extract_scalar({EVT_PAYLOAD}, '$.preferenceType')) = 'AVAIL_CREDIT_REMAINING'
        AND TRY(json_extract_scalar({EVT_PAYLOAD}, '$.productType'))   = 'CREDIT_CARD'
    """
    return f"SELECT * FROM ({a}) UNION SELECT * FROM ({b})"

def q_fft0_inbox(schema: str, week_start: date) -> str:
    return f"""
    WITH inbox AS (
      SELECT
        {ESS_PROCESS_TS} AS ess_process_timestamp_a,
        event_activity_type AS event_activity_type_a,
        source_event_id AS source_event_id_a,
        partition_date AS partition_date_a,
        event_timestamp AS event_timestamp_a,
        {EVT_ID} AS eventid_a,
        {EVT_TS} AS eventtimestamp_a,
        TRY(json_extract_scalar({SRC_HDR}, '$.eventActivityName')) AS eventactivityname_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.alertSent')) AS alertsent_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.sendInbox')) AS sendinbox_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.alertType')) AS alerttype_a,
        TRY(CAST(json_extract_scalar({EVT_PAYLOAD}, '$.thresholdAmount') AS DECIMAL(12,2))) AS thresholdamount_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.sendSMS')) AS sendsms_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.sendPush')) AS sendpush_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.maskedAccount')) AS maskedaccount_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.reasonCode')) AS reasoncode_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.decisionId')) AS decisionid_a,
        TRY(CAST(json_extract_scalar({EVT_PAYLOAD}, '$.alertAmount') AS DECIMAL(12,2))) AS alertamount_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.accountID')) AS accountid_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.account')) AS account_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.accountProduct')) AS accountproduct_a,
        TRY(json_extract_scalar({EVT_PAYLOAD}, '$.sendEmail')) AS sendemail_a
      FROM {schema}.fft0_alert_inbox_dep
      WHERE event_activity_type = 'Alert Delivery Audit'
        AND TRY(json_extract_scalar({EVT_PAYLOAD}, '$.alertType')) = 'AVAIL_CREDIT_REMAINING'
    )
    SELECT *
    FROM inbox
    WHERE { _SAFE_TS("CAST(eventtimestamp_a AS varchar)") } > TIMESTAMP '{week_start.isoformat()} 00:00:00'
    """

def read_trino_df(conn, sql: str) -> pd.DataFrame:
    logging.info("Running SQL:\n%s", sql)
    return pd.read_sql(sql, conn)

# =============================================================================
# 3. DATA PROCESSING (pandas)
# =============================================================================

def run_pipeline(conf: dict):
    # Dates & paths
    dt = compute_dates()
    label = dt["label"]

    out_root = Path(conf["out_root"])
    log_root = Path(conf["log_root"])
    out_dir = out_root / label
    out_dir.mkdir(parents=True, exist_ok=True)
    log_root.mkdir(parents=True, exist_ok=True)
    log_file = log_root / f"c8600J_Alerts_Cards_{label}.log"
    setup_logging(log_file)

    logging.info("Environment=%s  Catalog=%s Schema=%s", conf["env"], conf["catalog"], conf["schema"])
    logging.info("Output dir: %s", out_dir.resolve())

    # Connect & read
    with get_trino_conn(conf) as conn:
        schema = f"{conf['catalog']}.{conf['schema']}"

        df_cards = read_trino_df(
            conn, q_xb80_cards(schema, dt["week_start_dt"], dt["week_end_dt_p1"], dt["pardt"])
        )
        df_pref = read_trino_df(conn, q_cards_dec_pref(schema, dt["week_end_dt_p1"]))
        df_inbox = read_trino_df(conn, q_fft0_inbox(schema, dt["week_start_dt"]))

    # ---------- Robust join helper utilities ----------
    def _pick(colnames, candidates):
        for c in candidates:
            if c in colnames:
                return c
        return None

    def _to_lowercase_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [c.lower() for c in df.columns]
        return df

    def _clean_blanks_to_nan(df: pd.DataFrame, cols):
        for c in cols:
            if c and c in df.columns:
                df[c] = df[c].replace(r'^\s*$', pd.NA, regex=True)

    # Normalize all columns to lowercase
    df_cards = _to_lowercase_columns(df_cards)
    df_pref  = _to_lowercase_columns(df_pref)
    df_inbox = _to_lowercase_columns(df_inbox)

    # Pick join keys
    left_acc  = _pick(df_cards.columns, ['accountid_key', 'accountid'])
    left_cust = _pick(df_cards.columns, ['customerid_key', 'customerid'])
    right_acc = _pick(df_pref.columns,  ['externalaccount_p','externalaccount','account_p','account','externalaccountnumber','externalaccountno'])
    right_cust= _pick(df_pref.columns,  ['custid_p','custid','clientid_p','clientid'])

    _clean_blanks_to_nan(df_cards, [left_acc, left_cust])
    _clean_blanks_to_nan(df_pref,  [right_acc, right_cust])

    logging.info("Join keys -> left=(%s,%s) right=(%s,%s)", left_acc, left_cust, right_acc, right_cust)

    # Safe parse timestamps (idempotent)
    for ts_col in [
        "eventtimestamp","event_timestamp_p","eventtimestamp_p",
        "ess_process_timestamp","ess_src_event_timestamp",
        "ess_process_timestamp_p","ess_src_event_timestamp_p",
        "eventtimestamp_a","ess_process_timestamp_a"
    ]:
        if ts_col in df_cards.columns:
            df_cards[ts_col] = pd.to_datetime(df_cards[ts_col], errors="coerce")
        if ts_col in df_pref.columns:
            df_pref[ts_col] = pd.to_datetime(df_pref[ts_col], errors="coerce")
        if ts_col in df_inbox.columns:
            df_inbox[ts_col] = pd.to_datetime(df_inbox[ts_col], errors="coerce")

    # Preference join (skip if unusable)
    if df_pref.empty or not (left_acc and left_cust and right_acc and right_cust):
        logging.warning("Preference dataset not usable for join; skipping preference join")
        df_cards_dec_pref2 = df_cards.copy()
        df_cards_dec_pref2["dec_tm_ge_pref_tm"] = "N"
    else:
        df_join = pd.merge(
            df_cards,
            df_pref,
            how="left",
            left_on=[left_acc, left_cust],
            right_on=[right_acc, right_cust],
            suffixes=("", "_p2"),
        )

        if "eventtimestamp" in df_join.columns and "eventtimestamp_p" in df_join.columns:
            df_join["dec_tm_ge_pref_tm"] = (
                (df_join["eventtimestamp"] > df_join["eventtimestamp_p"])
                .map({True: "Y", False: "N"})
                .fillna("N")
            )
        else:
            df_join["dec_tm_ge_pref_tm"] = "N"

        sort_cols = [
            "decisionid","eventtimestamp", left_acc, left_cust,
            "dec_tm_ge_pref_tm","eventtimestamp_p", right_acc
        ]
        present = [c for c in sort_cols if c in df_join.columns]
        asc = [False if c in ["dec_tm_ge_pref_tm","eventtimestamp_p", right_acc] else True for c in present]
        df_join = df_join.sort_values(by=present, ascending=asc, na_position="last")

        if "decisionid" not in df_join.columns:
            df_join["decisionid"] = pd.NA

        df_cards_dec_pref2 = df_join.drop_duplicates(subset=["decisionid"], keep="first")

    # Inbox de-dup by decisionid_a, keep latest eventtimestamp_a
    if not df_inbox.empty:
        df_inbox = (
            df_inbox.sort_values(by=["decisionid_a", "eventtimestamp_a"], ascending=[True, False])
                    .drop_duplicates(subset=["decisionid_a"], keep="first")
        )

    # Join inbox: decisionid == decisionid_a
    df_all = pd.merge(
        df_cards_dec_pref2,
        df_inbox,
        how="left",
        left_on="decisionid",
        right_on="decisionid_a",
        suffixes=("", "_a2"),
    )

    # === cards_alert_final ===
    def to_bool_str(x): return str(x).lower()

    df_final = df_all.copy()
    # filter: isbusiness_p == 'true' and dec_tm_ge_pref_tm == 'Y' -> drop
    if "isbusiness_p" in df_final.columns:
        df_final = df_final[~((df_final["isbusiness_p"].map(to_bool_str) == "true") &
                              (df_final["dec_tm_ge_pref_tm"] == "Y"))]

    # Date parts
    for col in ["ess_src_event_timestamp","ess_process_timestamp","eventtimestamp","eventtimestamp_a"]:
        if col in df_final.columns:
            df_final[col] = pd.to_datetime(df_final[col], errors="coerce")

    df_final["src_event_date"] = df_final["ess_src_event_timestamp"].dt.date if "ess_src_event_timestamp" in df_final else pd.NaT
    df_final["process_date"]   = df_final["ess_process_timestamp"].dt.date    if "ess_process_timestamp" in df_final else pd.NaT
    df_final["decision_date"]  = df_final["eventtimestamp"].dt.date           if "eventtimestamp" in df_final else pd.NaT
    df_final["event_date"]     = df_final["eventtimestamp"].dt.date           if "eventtimestamp" in df_final else pd.NaT

    # Numerics and checks
    for numcol in ["alertamount","thresholdamount"]:
        if numcol in df_final.columns:
            df_final[numcol] = pd.to_numeric(df_final[numcol], errors="coerce")

    if set(["alertamount","thresholdamount"]).issubset(df_final.columns):
        df_final["threshold_limit_check"] = (df_final["alertamount"] < df_final["thresholdamount"]).map({True: "Y", False: "N"})
    else:
        df_final["threshold_limit_check"] = "N"

    df_final["Found_Missing"] = (
        (df_final.get("eventtimestamp_a").isna() if "eventtimestamp_a" in df_final else True) |
        (df_final.get("eventtimestamp").isna() if "eventtimestamp" in df_final else True)
    ).map({True: "Y", False: "N"})

    # Time diff & SLA
    df_final["Time_Diff"] = (
        (df_final["eventtimestamp_a"] - df_final["eventtimestamp"]).dt.total_seconds()
        if set(["eventtimestamp_a","eventtimestamp"]).issubset(df_final.columns) else pd.NA
    )

    df_final["SLA_Ind"] = np.where(
        (pd.notna(df_final["Time_Diff"])) & (df_final["Time_Diff"] <= 1800),
        "Y","N"
    )

    # Buckets
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
    if hasattr(df_final["Alert_Time"], "cat"):
        df_final["Alert_Time"] = df_final["Alert_Time"].cat.add_categories("99 - Timestamp is missing")
    df_final["Alert_Time"] = df_final["Alert_Time"].fillna("99 - Timestamp is missing")

    # === Summaries ===
    report_dt = compute_dates()["report_dt"]

    def snapdate(d):
        if pd.isna(d): return pd.NaT
        if not isinstance(d, (pd.Timestamp, datetime, date)): return pd.NaT
        d0 = pd.to_datetime(d).date()
        weekday = d0.weekday()      # Wed=2
        days_to_wed_end = (2 - weekday) % 7
        return d0 + timedelta(days=days_to_wed_end)

    def sample_by_day(df, size=10, seed=42):
        out = []
        for k, g in df.groupby("decision_date"):
            if len(g) <= size:
                out.append(g.copy())
            else:
                out.append(g.sample(n=size, random_state=seed))
        return pd.concat(out, ignore_index=True) if out else df.head(0)

    sample_df = sample_by_day(df_final, size=10)

    acc = (
        sample_df.assign(
            ControlRisk="Accuracy",
            TestType="Sample",
            RDE="Alert010_Accuracy_Available_Credit",
            CommentCode=lambda d: d["threshold_limit_check"].map(lambda x: "COM16" if x == "Y" else "COM19"),
            Segment10=lambda d: pd.to_datetime(d["decision_date"]).strftime("%Y%m%d"),
            DateCompleted=report_dt,
            SnapDate=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
        )
        .groupby(["ControlRisk","TestType","RDE","CommentCode","Segment10","DateCompleted","SnapDate"], dropna=False)
        .agg(Volume=("decisionid","count"),
             bal=("alertamount","sum"),
             Amount=("thresholdamount","sum"))
        .reset_index()
    )

    tml = (
        df_final.assign(
            ControlRisk="Timeliness",
            TestType="Anomaly",
            RDE="Alert011_Timeliness_SLA",
            CommentCode=lambda d: d["SLA_Ind"].map(lambda x: "COM16" if x == "Y" else "COM19"),
            Segment10=lambda d: pd.to_datetime(d["decision_date"]).strftime("%Y%m%d"),
            DateCompleted=report_dt,
            SnapDate=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
        )
        .groupby(["ControlRisk","TestType","RDE","CommentCode","Segment10","DateCompleted","SnapDate"], dropna=False)
        .agg(Volume=("decisionid","count"),
             bal=("alertamount","sum"),
             Amount=("thresholdamount","sum"))
        .reset_index()
    )

    cpl = (
        df_final.assign(
            ControlRisk="Completeness",
            TestType="Reconciliation",
            RDE="Alert012_Completeness_All_Clients",
            CommentCode=lambda d: d["decisionid_a"].fillna("").map(lambda x: "COM16" if x != "" else "COM19"),
            Segment10=lambda d: pd.to_datetime(d["decision_date"]).strftime("%Y%m"),
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
    alert_cards_ac_week.insert(0, "RegulatoryName", "c86")
    alert_cards_ac_week.insert(1, "LOB", "Credit Cards")
    alert_cards_ac_week.insert(2, "ReportName", "c86 Alerts")
    alert_cards_ac_week.insert(5, "TestPeriod", "Portfolio")
    alert_cards_ac_week.insert(6, "ProductType", "Credit Cards")
    for col in ["SubDE","Segment","Segment2","Segment3","Segment4","Segment5","Segment6","Segment7","Segment8","Segment9","HoldoutFlag"]:
        alert_cards_ac_week[col] = "."
    alert_cards_ac_week["HoldoutFlag"] = "N"
    alert_cards_ac_week["Comments"] = ""

    # === Detail tabs ===
    completeness_fail = (
        df_final[df_final["decisionid_a"].fillna("") == ""]
        .assign(
            event_month=lambda d: pd.to_datetime(d["decision_date"]).strftime("%Y%m"),
            reporting_date=report_dt,
            event_week_ending=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
            LOB="Credit Cards",
            Product="Credit Cards",
            account_number=lambda d: d.get("accountid", pd.Series([None]*len(d))),
            available_credit=lambda d: d["alertamount"],
            event_date=lambda d: pd.to_datetime(d["decision_date"]).dt.date,
            custid_mask=lambda d: d.get("customerid", pd.Series([""]*len(d))).fillna("").str[-3:].radd("******"),
        )[
            ["event_month","reporting_date","event_week_ending","LOB","Product",
             "account_number","thresholdamount","available_credit","decisionid","event_date","custid_mask"]
        ]
    )

    timeliness_fail = (
        df_final[df_final["SLA_Ind"] != "Y"]
        .assign(
            event_month=lambda d: pd.to_datetime(d["decision_date"]).strftime("%Y%m"),
            reporting_date=report_dt,
            event_week_ending=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
            LOB="Credit Cards",
            Product="Credit Cards",
            account_number=lambda d: d.get("accountid", pd.Series([None]*len(d))),
            available_credit=lambda d: d["alertamount"],
            event_date=lambda d: pd.to_datetime(d["decision_date"]).dt.date,
            total_minutes=lambda d: (d["Time_Diff"] / 60.0).apply(lambda x: None if pd.isna(x) else math.ceil(x)),
            custid_mask=lambda d: d.get("customerid", pd.Series([""]*len(d))).fillna("").str[-3:].radd("******"),
        )[
            ["event_month","reporting_date","event_week_ending","LOB","Product",
             "account_number","thresholdamount","available_credit","decisionid",
             "event_date","eventtimestamp","eventtimestamp_a","total_minutes","custid_mask"]
        ]
    )

    accuracy_fail = (
        sample_df[sample_df["threshold_limit_check"] != "Y"]
        .assign(
            event_month=lambda d: pd.to_datetime(d["decision_date"]).strftime("%Y%m"),
            reporting_date=report_dt,
            event_week_ending=lambda d: pd.to_datetime(d["decision_date"]).map(snapdate),
            LOB="Credit Cards",
            Product="Credit Cards",
            account_number=lambda d: d.get("accountid", pd.Series([None]*len(d))),
            decision="AlertDecision",
            available_credit=lambda d: d["alertamount"],
            event_date=lambda d: pd.to_datetime(d["decision_date"]).dt.date,
            custid_mask=lambda d: d.get("customerid", pd.Series([""]*len(d))).fillna("").str[-3:].radd("******"),
        )[
            ["event_month","reporting_date","event_week_ending","LOB","Product",
             "account_number","decision","thresholdamount","available_credit","decisionid",
             "event_date","custid_mask"]
        ]
    )

    # === OUTPUT ===
    df_final_path = out_dir / "cards_alert_final.parquet"
    df_final.to_parquet(df_final_path, index=False)
    logging.info("Wrote %s", df_final_path)

    ac_week_path = out_dir / "Alert_Cards_AC_week.parquet"
    alert_cards_ac_week.to_parquet(ac_week_path, index=False)
    logging.info("Wrote %s", ac_week_path)

    xlsx1 = out_dir / "Alert_Cards_Completeness_Detail.xlsx"
    completeness_fail.to_excel(xlsx1, sheet_name="Alert_Cards_Completeness_Detail", index=False)
    logging.info("Wrote %s", xlsx1)

    xlsx2 = out_dir / "Alert_Cards_Timeliness_Detail.xlsx"
    timeliness_fail.to_excel(xlsx2, sheet_name="Alert_Cards_Timeliness_Detail", index=False)
    logging.info("Wrote %s", xlsx2)

    xlsx3 = out_dir / "Alert_Cards_Accuracy_Detail.xlsx"
    accuracy_fail.to_excel(xlsx3, sheet_name="Alert_Cards_Accuracy_Detail", index=False)
    logging.info("Wrote %s", xlsx3)

    logging.info(
        "Final counts: rows=%d, distinct decisionId=%d",
        len(df_final),
        df_final["decisionid"].nunique() if "decisionid" in df_final else -1
    )

    # Historical append
    logging.info("Updating historical autocomplete file...")
    ac_lib_path = Path(conf["out_root"]).parent
    ac_lib_path.mkdir(parents=True, exist_ok=True)
    historical_ac_file = ac_lib_path / "alert_cards_ac.parquet"

    df_new_week = alert_cards_ac_week.copy()
    df_new_week["SnapDate"] = pd.to_datetime(df_new_week["SnapDate"]).dt.date

    if historical_ac_file.exists():
        logging.info("Reading existing history file: %s", historical_ac_file)
        df_history = pd.read_parquet(historical_ac_file)
        df_history["SnapDate"] = pd.to_datetime(df_history["SnapDate"]).dt.date
        new_snapdates = df_new_week["SnapDate"].unique()
        df_history_filtered = df_history[~df_history["SnapDate"].isin(new_snapdates)]
        df_combined_history = pd.concat([df_history_filtered, df_new_week], ignore_index=True)
        logging.info("Appended new data, replaced %d SnapDate(s).", len(new_snapdates))
    else:
        logging.warning("History file not found. Creating new one.")
        df_combined_history = df_new_week

    df_combined_history = df_combined_history.sort_values(by="SnapDate")
    df_combined_history.to_parquet(historical_ac_file, index=False)
    logging.info("Wrote updated historical file: %s (Total rows: %d)", historical_ac_file, len(df_combined_history))

# =============================================================================
# 4. MAIN
# =============================================================================

if __name__ == "__main__":
    cfg = load_config(Path("config.ini"))
    run_pipeline(cfg)
