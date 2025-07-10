from dagster import Definitions
from src.main.python.pipeline import kobis_daily_job, kobis_daily_schedule

defs = Definitions(
    jobs=[kobis_daily_job],
    schedules=[kobis_daily_schedule],
)