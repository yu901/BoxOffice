from dagster import Definitions
from src.main.python.kobis_pipeline import kobis_daily_job, kobis_daily_schedule
from src.main.python.goods_stock_pipeline import goods_events_job, goods_stock_check_job, goods_events_schedule, goods_stock_schedule

defs = Definitions(
    jobs=[kobis_daily_job, goods_events_job, goods_stock_check_job],
    schedules=[kobis_daily_schedule, goods_events_schedule, goods_stock_schedule],
)