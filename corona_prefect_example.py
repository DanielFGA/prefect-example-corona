from typing import List

from prefect import task, Flow, Task
import pandas as pd
import sqlite3
import datetime

from util import *
from config import *


@task(name='extract corona cases')
def extract_corona_cases() -> pd.DataFrame:
    corona_cases_df = pd.read_csv(CORONA_CASES_PATH, sep=';', converters=StringConverter())
    sleep_random()
    return corona_cases_df


@task(name='extract corona tests')
def extract_corona_tests() -> pd.DataFrame:
    corona_tests_df = pd.read_csv(CORONA_TESTS_PATH, sep=';', converters=StringConverter(), usecols=['Kalenderwoche', 'Anzahl Testungen'])
    sleep_random()
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
    sleep_random()
    return corona_cases_df_transformed


@task(name='transform corona tests')
def transform_corona_tests(corona_tests_df: pd.DataFrame) -> pd.DataFrame:
    def transform_kw(kw_year):
        kw_year = kw_year.replace('*', '')
        year = kw_year.split('/')[1]
        kw = kw_year.split('/')[0]
        return "{}{}".format(year, kw)
    corona_tests_df_transformed = corona_tests_df.applymap(remove_dot)
    corona_tests_df_transformed['Kalenderwoche'] = corona_tests_df_transformed['Kalenderwoche'].apply(transform_kw)
    corona_tests_df_transformed = corona_tests_df_transformed.astype('int32')
    corona_tests_df_transformed['Kalenderwoche'] = corona_tests_df_transformed['Kalenderwoche'].astype('str')
    sleep_random()
    return corona_tests_df_transformed


@task(name='unite corona cases & tests')
def unite_corona_cases_and_tests(corona_cases_df_transformed: pd.DataFrame,
                                 corona_tests_df_transformed: pd.DataFrame) -> pd.DataFrame:
    sleep_random()
    return corona_cases_df_transformed.join(corona_tests_df_transformed['Anzahl Testungen'])


@task(name='create table')
def create_table_from_df(df: pd.DataFrame, df_name: str):
    connection = sqlite3.connect(DATABASE_NAME)
    connection.execute('DROP table IF EXISTS {}'.format(df_name))
    df.to_sql(name=df_name, con=connection)
    sleep_random()


@task(name='visualize')
def visualize_df(df: pd.DataFrame,df_name: str,
                        x_axis: str,y_axis: List[str]):
    fig = df.plot(title=df_name, kind='line', x=x_axis, y=y_axis).get_figure()
    fig.savefig(df_name)
    sleep_random()


with Flow('Corona cases in relation with tests flow') as flow:

    corona_cases_df = extract_corona_cases()
    corona_tests_df = extract_corona_tests()

    corona_cases_df_transformed = transform_corona_cases(corona_cases_df)
    corona_tests_df_transformed = transform_corona_tests(corona_tests_df)

    unite_corona_cases_and_tests_df = unite_corona_cases_and_tests(corona_cases_df_transformed,
                                                                   corona_tests_df_transformed)

    create_table_from_df(unite_corona_cases_and_tests_df, 'unite_corona_cases_and_tests')

    visualize_df(unite_corona_cases_and_tests_df,
                 'Corona cases with relation to tests',
                 x_axis='Kalenderwoche',
                 y_axis=["Gemeldete Infektionen", "Hospitalisierte Fälle", "Todesfälle", "Anzahl Testungen"]
                 )


