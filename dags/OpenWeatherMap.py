from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime, time
from datetime import datetime
import requests, json
import os
import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from airflow.operators.python import get_current_context
from joblib import dump
import random

def compute_model_score(model, X, y):
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()
    return model_score
	
def train_and_save_model(model, X, y, path_to_model='./app/model.pckl'):
    # training the model
    model.fit(X, y)
    # saving model
    print(str(model), 'saved at ', path_to_model)
    dump(model, path_to_model)

def prepare_data(path_to_data='/app/clean_data/fulldata.csv'):
    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []
    for c in df['city'].unique():
        df_temp = df[df['city'] == c]
		# creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)
		# creating features
        for i in range(1, 10):
            df_temp.loc[:, 'temp_m-{}'.format(i)] = df_temp['temperature'].shift(-i)
    		# deleting null values
            df_temp = df_temp.dropna()
            dfs.append(df_temp)
	
	    # concatenating datasets
        df_final = pd.concat(dfs,axis=0,ignore_index=False)

	# deleting date variable
    df_final = df_final.drop(['date'], axis=1)
	# creating dummies for city variable
    df_final = pd.get_dummies(df_final)
    features = df_final.drop(['target'], axis=1)
    target = df_final['target']
    return features, target


#support functions
def collectWeather(cities, main_path="/app"):
    data=[]
    fname = datetime.strftime(datetime.today(), '%Y-%m-%d_%H:%M.json')
    print(f"Preparing {fname}")
    tgtPath=f"{main_path}/raw_files" #"/app/raw_files"
    
    for city in cities :
        req = f"https://api.openweathermap.org/data/2.5/weather?q={city},fr&APPID=961373402b6adffd1ce1e7d1e6ce1682"
        r = requests.get(req)
        data.append({"city": city, "infos":r.json()})
        print(city,r.json(),"\n")
    print(data)

    with open(f"{tgtPath}/{fname}","w", encoding="utf_8") as f: 
        json.dump(data,f,indent=4)

    f.close()   


def transform_data_into_csv(n_files=None, filename='data.csv', main_path="/app"):
     #./ in notebook resp /app in airflow dag
    parent_folder = f'{main_path}/raw_files'
    files = sorted(os.listdir(parent_folder), reverse=True)

    if n_files:
        files = files[:n_files]

    dfs = []
    for f in files:
        with open(os.path.join(parent_folder, f), 'r') as file:
            data_temp = json.load(file)

        for data_city in data_temp:
            print(data_city)
            dfs.append(
                {
                    'temperature': data_city['infos']['main']['temp'],
                    'city': data_city['city'],
                    'pression': data_city['infos']['main']['pressure'],
                    'date': f.split('.')[0].replace("_", " ")
                }
            )
    df = pd.DataFrame(dfs)
    print('\n', df.head(10))
    df.to_csv(os.path.join(f'{main_path}/clean_data', filename), index=False)


def ComputeScore(model, main_path="/app"):
    
    task_instance = get_current_context()['task_instance']
    value = random.uniform(a=0, b=1)
    
    X, y = prepare_data(f'{main_path}/clean_data/fulldata.csv')
    X=X.fillna(0)
    score=compute_model_score(model, X, y)

    #to translate into dags with XCom
    task_instance.xcom_push(key="score", value=score)
    return score

def readScoresandCompute(task4s, models, main_path="/app"):
    task_instance = get_current_context()['task_instance']
    scores = task_instance.xcom_pull(key='score',task_ids=task4s)

    bestModel=models[scores.index(min(scores))]
    # using neg_mean_square_error and find the min of scores
    X, y = prepare_data(f'{main_path}/clean_data/fulldata.csv')
    X=X.fillna(0)
    train_and_save_model(bestModel, X, y,f'{main_path}/clean_data/best_model.pickle' )


with DAG(
    dag_id='OpenWeatherMap',
    description='My first DAG created with DataScientest',
    tags=['exam', 'datascientest'],
    #schedule_interval=None,
    schedule_interval='* * * * *',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0),
    },
    catchup=False
) as my_dag:

    my_task1 = PythonOperator(
        task_id='my_very_first_task',
        python_callable=collectWeather,
        op_kwargs={'cities': ['paris, France', 'Paris, Kentucky ', 'london', 'Washington, District of Columbia']}
    )

    
    my_task2 = PythonOperator(
        task_id='my_second_task',
        python_callable=transform_data_into_csv,
        op_kwargs={'n_files': 20}
    )

    my_task3 = PythonOperator(
        task_id='my_third_task',
        python_callable=transform_data_into_csv,
    )

    tasks4,tasks4names = [], []
    models = [LinearRegression(),DecisionTreeRegressor(),RandomForestRegressor()]

    for m,model in enumerate(models):
        task4Name=f'my_forth_{m+1}'
        task4=PythonOperator(
            task_id=task4Name,
            python_callable=ComputeScore,
            op_kwargs={'model': model}
        )
        tasks4.append(task4)
        tasks4names.append(task4Name)

    my_task5 = PythonOperator(
            task_id='my_fith',
            python_callable=readScoresandCompute,
            op_kwargs={'task4s': tasks4names, 'models': models}
        )

    #and the DAG
    my_task1 >> [my_task2, my_task3]
    my_task3 >> tasks4 >> my_task5
