import os

from prefect.executors import LocalDaskExecutor

from config import GRAPHVIZ_PATH
from corona_prefect_example import flow

os.environ["PATH"] += os.pathsep + GRAPHVIZ_PATH

flow.executor = LocalDaskExecutor()

flow.run()
flow.visualize(filename='prefact_flow', format='png')
