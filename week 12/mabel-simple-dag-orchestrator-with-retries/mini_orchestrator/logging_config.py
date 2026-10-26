import logging
import sys


def configure_logging(log_file: str = "logs/scheduler.log", level=logging.INFO) -> None:
    fmt = "%(asctime)s %(levelname)-8s %(name)s %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%dT%H:%M:%S%z")

    root = logging.getLogger("mini_orchestrator")
    root.setLevel(level)
    root.handlers.clear()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
