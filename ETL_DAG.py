from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow import DAG
import requests
import gzip
from pandas import pandas as pd
from glob import glob
import os

def ETL_Teco():

    #URL's containing datasets
    #title.basics.tsv.gz containing ID, titleType, primaryTitle,originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres
    #title.ratings.tsv.gz containing averageRating, numVotes
    #title.crew.tsv.gz containing directors, writers

    links=['https://datasets.imdbws.com/title.basics.tsv.gz','https://datasets.imdbws.com/title.ratings.tsv.gz','https://datasets.imdbws.com/title.crew.tsv.gz','https://datasets.imdbws.com/name.basics.tsv.gz']

    def retrieve(x):
        #Import dataset
        if 'title.' in x:
            dset=requests.get(x)
            open(x.split("title.")[1],'wb').write(dset.content)
            print('Saved {} as {}'.format(x,x.split("title.")[1]))
        else:
            dset=requests.get(x)
            open(x.split(".com/")[1],'wb').write(dset.content)
            print('Saved {} as {}'.format(x,x.split(".com/")[1]))
            
    #Save remote files locally for manipulation
    for i in range(len(links)):
        retrieve(links[i])

    #Load Dataframe with the .csv downloaded locally

    df_basics=pd.read_csv('basics.tsv.gz',compression='gzip',sep='\t',error_bad_lines=False)
    df_basics['startYear'].replace({'\\N':'0'},inplace=True)
    df_ratings=pd.read_csv('ratings.tsv.gz',compression='gzip',sep='\t',error_bad_lines=False)
    df_crew=pd.read_csv('crew.tsv.gz',compression='gzip',sep='\t',error_bad_lines=False)
    df_namebasics=pd.read_csv('name.basics.tsv.gz',compression='gzip',sep='\t',error_bad_lines=False)
    df_basics['startYear'].astype(int)

    #Filted df's to include data from 2015-2020 & Movies

    df_basics['startYear']=pd.to_numeric(df_basics['startYear'])
    df_basics_filtered=df_basics[(df_basics['startYear']>=int(2015))&(df_basics['startYear']<=int(2020))&(df_basics['titleType']=='movie')]
    df_basics_filtered_moredata=df_basics_filtered.join(df_ratings.set_index('tconst'),on='tconst')
    df_basics_filtered_moredata=df_basics_filtered_moredata.join(df_crew.set_index('tconst'),on='tconst')

    #Replace '\\N' with zero-values

    try:
        df_basics_filtered_moredata['runtimeMinutes'].replace({'\\N':'0'},inplace=True)
        df_basics_filtered_moredata['runtimeMinutes']=pd.to_numeric(df_basics_filtered_moredata['runtimeMinutes'])

    except TypeError:
        pass

    #Explode into rows based on genres column so data is at genre-year level
    df_basics_filtered_moredata_exploded=df_basics_filtered_moredata.drop('genres',axis=1).join(df_basics_filtered_moredata.genres.str.split(",",expand=True).stack().reset_index(drop=True,level=1).rename('generos'))

    #Table 1: ratings grouped by genre-year

    rating_generoyear=pd.DataFrame(df_basics_filtered_moredata_exploded.groupby(by=['startYear','generos'])['averageRating'].mean().round(2))
    rating_generoyear.index.set_names(['Year','Generos'])
    rating_generoyear.reset_index(inplace=True)

    #Table 2: runtimeMinutes grouped by genre-year

    runtime_generoyear=pd.DataFrame(df_basics_filtered_moredata_exploded.groupby(by=['startYear','generos'])['runtimeMinutes'].mean().round(2))
    runtime_generoyear.index.set_names(['Year','Generos'])
    runtime_generoyear.reset_index(inplace=True)

    #Table 3: numVotes grouped by genre-year

    num_votos_generoyear=pd.DataFrame(df_basics_filtered_moredata_exploded.groupby(by=['startYear','generos'])['numVotes'].sum().astype(int),columns=['numVotes'])
    num_votos_generoyear.index.set_names(['Year','Generos'])
    num_votos_generoyear.reset_index(inplace=True)

    #Merge Table 1, Table 2 & Table 3 into resultados_2

    resultados_1=pd.merge(rating_generoyear,num_votos_generoyear,on=['startYear','generos'],how='inner')
    resultados_2=pd.merge(resultados_1,runtime_generoyear,on=['startYear','generos'],how='inner')

    #Obtain a table with unique genres, writers and directors
    #Table 4
    df_basics_filtered_moredata_exploded_dir=df_basics_filtered_moredata_exploded.drop('directors',axis=1).join(df_basics_filtered_moredata.directors.str.split(",",expand=True).stack().reset_index(drop=True,level=1).rename('directors'))
    df_basics_filtered_moredata_exploded_dir_wr=df_basics_filtered_moredata_exploded_dir.drop('writers',axis=1).join(df_basics_filtered_moredata_exploded_dir.writers.str.split(",",expand=True).stack().reset_index(drop=True,level=1).rename('writers'))
    num_directors_generoyear=pd.DataFrame(df_basics_filtered_moredata_exploded_dir_wr.groupby(by=['startYear','generos'])['directors'].count().astype(int))
    num_directors_generoyear.index.set_names(['Year','Generos'])
    num_directors_generoyear.reset_index(inplace=True)
    #Table 5
    num_wr_generoyear=pd.DataFrame(df_basics_filtered_moredata_exploded_dir_wr.groupby(by=['startYear','generos'])['writers'].count().astype(int))
    num_wr_generoyear.index.set_names(['Year','Generos'])
    num_wr_generoyear.reset_index(inplace=True)

    #Merge resultados_2 & Table 4 & Table 5

    resultados_3=pd.merge(resultados_2,num_directors_generoyear,on=['startYear','generos'],how='inner')
    resultados_4=pd.merge(resultados_3,num_wr_generoyear,on=['startYear','generos'],how='inner')

    #Export .csv with final results (top directors not included)

    resultados_4.to_csv('results_final.csv')

    return print('proceso terminado')


dag = DAG('etl_teco', description='etl_teco', schedule_interval='0 0 * * *', start_date=datetime(2021, 7, 19), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries = 3, dag=dag)

etl_operator = PythonOperator(task_id='ETL', python_callable=ETL_Teco, dag=dag)


dummy_operator >> etl_operator
