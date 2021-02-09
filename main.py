from typing import List

from prefect import task, Flow
import pandas as pd
import sqlite3
import datetime

import os
os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin'

CORONA_CASES_PATH = './raw_data/corona_fallzahlen.csv'
CORONA_TESTS_PATH = './raw_data/corona_testzahlen.csv'
DATABASE_NAME = 'corona_database'


def remove_dot(x):
    return x.replace('.', '')


class StringConverter(dict):
    def __contains__(self, item):
        return True
    def __getitem__(self, item):
        return str
    def get(self, default=None):
        return str


@task(name='extract corona cases')
def extract_corona_cases() -> pd.DataFrame:
    corona_cases_df = pd.read_csv(CORONA_CASES_PATH, sep=';', converters=StringConverter())
    return corona_cases_df


@task(name='extract corona tests')
def extract_corona_tests() -> pd.DataFrame:
    corona_tests_df = pd.read_csv(CORONA_TESTS_PATH, sep=';', converters=StringConverter(), usecols=['Kalenderwoche', 'Anzahl Testungen'])
    return corona_tests_df[:-1]  # we dont need the last summary line


@task(name='transform corona cases')
def transform_corona_cases(corona_cases_df: pd.DataFrame) -> pd.DataFrame:
    corona_cases_df_transformed = corona_cases_df.applymap(remove_dot)

    start_date = datetime.date(year=2020, month=3, day=2)
    for i in range(len(corona_cases_df_transformed['Kalenderwoche'])):
        kw = corona_cases_df_transformed['Kalenderwoche'][i]
        kw_int = int(kw[3:])
        corona_cases_df_transformed['Kalenderwoche'][i] = "{}{}".format(start_date.year, kw_int)
        start_date += datetime.timedelta(days=7)

    corona_cases_df_transformed = corona_cases_df_transformed.astype('int32')
    corona_cases_df_transformed['Kalenderwoche'] = corona_cases_df_transformed['Kalenderwoche'].astype('str')

    return corona_cases_df_transformed


@task(name='transform corona tests')
def transform_corona_tests(corona_tests_df: pd.DataFrame) -> pd.DataFrame:
    def transform_kw(kw):
        kw = kw.replace('*', '')
        year = kw.split('/')[1]
        kw = kw.split('/')[0]
        return "{}{}".format(year, kw)
    corona_tests_df_transformed = corona_tests_df.applymap(remove_dot)
    corona_tests_df_transformed['Kalenderwoche'] = corona_tests_df_transformed['Kalenderwoche'].apply(transform_kw)
    corona_tests_df_transformed = corona_tests_df_transformed.astype('int32')
    corona_tests_df_transformed['Kalenderwoche'] = corona_tests_df_transformed['Kalenderwoche'].astype('str')
    return corona_tests_df_transformed


@task(name='unite corona cases & tests')
def unite_corona_cases_and_tests(corona_cases_df_transformed: pd.DataFrame,
                                 corona_tests_df_transformed: pd.DataFrame) -> pd.DataFrame:
    return corona_cases_df_transformed.join(corona_tests_df_transformed['Anzahl Testungen'])


@task(name='create database')
def create_database(database_name: str) -> sqlite3.Connection:
    conn = sqlite3.connect(database_name)
    return conn


@task(name='create table')
def create_table_from_df(df: pd.DataFrame, df_name: str, connection: sqlite3.Connection):
    connection.execute('DROP table IF EXISTS {}'.format(df_name))
    df.to_sql(name=df_name, con=connection)


@task(name='visualize')
def visualize_df(df: pd.DataFrame,
                        df_name: str,
                        x_axis: str,
                        y_axis: List[str]):
    fig = df.plot(title=df_name, kind='line', x=x_axis, y=y_axis).get_figure()
    fig.savefig(df_name)


with Flow('Corona cases in relation with tests flow') as flow:

    corona_cases_df = extract_corona_cases()
    corona_tests_df = extract_corona_tests()

    corona_cases_df_transformed = transform_corona_cases(corona_cases_df)
    corona_tests_df_transformed = transform_corona_tests(corona_tests_df)

    unite_corona_cases_and_tests_df = unite_corona_cases_and_tests(corona_cases_df_transformed, corona_tests_df_transformed)

    connection = create_database(DATABASE_NAME)
    create_table_from_df(corona_cases_df_transformed, 'corona_cases', connection)
    create_table_from_df(corona_tests_df_transformed, 'corona_tests', connection)
    create_table_from_df(unite_corona_cases_and_tests_df, 'unite_corona_cases_and_tests', connection)

    visualize_df(unite_corona_cases_and_tests_df, 'Corona cases with relation to tests',
                        x_axis='Kalenderwoche',
                        y_axis=["Gemeldete Infektionen", "Hospitalisierte Fälle", "Todesfälle", "Anzahl Testungen"]
                        )


flow.run()
flow.visualize(filename='prefact_flow', format='png')
