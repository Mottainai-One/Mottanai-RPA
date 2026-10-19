"""Run the daily operational-to-analytics batch manually."""

import logging

from analytics.load import run


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(run())
