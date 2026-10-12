import logging
from analytics_daily import run_daily_analytics
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
if __name__ == "__main__":
    print(run_daily_analytics())
