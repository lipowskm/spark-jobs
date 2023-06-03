import logging

logger = logging.getLogger(__name__)
_formatter = logging.Formatter("[%(levelname)s] %(name)s %(asctime)s - %(message)s")
_ch = logging.StreamHandler()
_ch.setFormatter(_formatter)
_ch.setLevel(logging.INFO)
logger.addHandler(_ch)
del logging
