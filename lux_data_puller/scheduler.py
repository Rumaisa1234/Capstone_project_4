from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from lux_microservice import send_to_producer

scheduler = BlockingScheduler()
scheduler.add_job(send_to_producer, "cron", second="0", next_run_time=datetime.now())
scheduler.start()
