import logging
import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from sodapy import Socrata

logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)
