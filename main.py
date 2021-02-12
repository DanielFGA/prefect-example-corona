import os

from prefect.executors import DaskExecutor

from config import GRAPHVIZ_PATH
from corona_prefect_example import flow

os.environ["PATH"] += os.pathsep + GRAPHVIZ_PATH

flow.executor = DaskExecutor()

flow.register(project_name="example")