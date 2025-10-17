from datetime import datetime
import logging
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

log = logging.getLogger(__name__)

def slack_fail_alert(context, **kwargs):
    ti = context["ti"]
    msg = (
        f":red_circle: *Airflow task failed*\n"
        f"*DAG:* `{ti.dag_id}`\n"
        f"*Task:* `{ti.task_id}`\n"
        f"*When:* `{context.get('ts')}`\n"
        f"*Error:* `{context.get('exception')}`\n"
        f"<{ti.log_url}|Ver logs>"
    )
    try:
        SlackWebhookHook(slack_webhook_conn_id="slack_notification_events").send(text=msg)
        log.info("Slack fail notification sent.")
    except Exception as e:
        log.exception("Slack fail notification error: %s", e)

def slack_success_alert(context, **kwargs):
    ti = context["ti"]
    msg = (
        f":white_check_mark: *Airflow task succeeded*\n"
        f"*DAG:* `{ti.dag_id}`\n"
        f"*Task:* `{ti.task_id}`\n"
        f"*When:* `{context.get('ts')}`\n"
        f"<{ti.log_url}|Ver logs>"
    )
    try:
        SlackWebhookHook(slack_webhook_conn_id="slack_notification_events").send(text=msg)
        log.info("Slack success notification sent.")
    except Exception as e:
        log.exception("Slack success notification error: %s", e)

MERGE_SQL = r"""
MERGE INTO MEETUP_DB.DATA_GLD.SBN_CALCULATED_EVENTS d
USING (
  WITH base AS (
    SELECT
      NULLIF(TRIM(e.event_url), '')                           AS event_url,
      e.event_time,
      NULLIF(TRIM(e.event_name), '')                          AS event_name,
      NULLIF(TRIM(g.group_name), '')                          AS group_name,
      COALESCE(NULLIF(TRIM(vc.city), ''), NULLIF(TRIM(gc.city), '')) AS city_name,
      NULLIF(TRIM(c.name_category), '')                       AS category_name,
      e.yes_rsvp_count,
      e.rsvp_limit,
      CASE WHEN e.rsvp_limit > 0
           THEN ROUND(100.0 * e.yes_rsvp_count / e.rsvp_limit, 2) END AS fill_rate_pct,
      CURRENT_TIMESTAMP() AS load_ts
    FROM MEETUP_DB.DATA_SLV.FACT_EVENTS e
    LEFT JOIN MEETUP_DB.DATA_SLV.DIM_GROUPS     g  ON g.id_group    = e.id_group
    LEFT JOIN MEETUP_DB.DATA_SLV.DIM_CATEGORIES c  ON c.id_category = g.id_category
    LEFT JOIN MEETUP_DB.DATA_SLV.DIM_CITIES     gc ON gc.id_city    = g.id_city
    LEFT JOIN MEETUP_DB.DATA_SLV.DIM_VENUES     v  ON v.venue_id    = e.id_venue
    LEFT JOIN MEETUP_DB.DATA_SLV.DIM_CITIES     vc ON vc.id_city    = v.id_city
    WHERE e.event_url IS NOT NULL
  )
  SELECT *
  FROM (
    SELECT b.*,
           ROW_NUMBER() OVER (PARTITION BY b.event_url, b.event_time ORDER BY b.load_ts DESC) AS rn
    FROM base b
  )
  WHERE rn = 1
) s
ON  d.event_url  = s.event_url
AND d.event_time = s.event_time
WHEN MATCHED THEN UPDATE SET
  d.event_name     = s.event_name,
  d.group_name     = s.group_name,
  d.city_name      = s.city_name,
  d.category_name  = s.category_name,
  d.yes_rsvp_count = s.yes_rsvp_count,
  d.rsvp_limit     = s.rsvp_limit,
  d.fill_rate_pct  = s.fill_rate_pct,
  d.load_ts        = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  event_url, event_time, event_name, group_name, city_name, category_name,
  yes_rsvp_count, rsvp_limit, fill_rate_pct, load_ts
) VALUES (
  s.event_url, s.event_time, s.event_name, s.group_name, s.city_name, s.category_name,
  s.yes_rsvp_count, s.rsvp_limit, s.fill_rate_pct, CURRENT_TIMESTAMP()
);
"""

with DAG(
    dag_id="sbn_calculated_events_15min",
    start_date=datetime(2025, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["gold", "sabana", "merge", "notifications"],
) as dag:

    merge_job = SQLExecuteQueryOperator(
        task_id="merge_sabana",
        conn_id="cnn_events_rappipay",
        sql=MERGE_SQL,
        on_failure_callback=slack_fail_alert,
        on_success_callback=slack_success_alert,
    )