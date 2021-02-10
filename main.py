import os

from prefect.executors import LocalDaskExecutor

from corona_prefect_example import flow

os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin'

flow.executor = LocalDaskExecutor()

flow.run()
flow.visualize(filename='prefact_flow', format='png')
