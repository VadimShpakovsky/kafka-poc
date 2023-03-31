import logging


def setup_logger(level: int = logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)-8s %(filename)s:%(lineno)s - %(message)s",
    )