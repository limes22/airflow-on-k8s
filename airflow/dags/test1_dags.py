from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score
from airflow.models import Variable

default_args = {
    'owner': 'howdi2000',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

dag = DAG{
    'test2_dag',
    default_args==default_args,
    description='A simple test2 DAG',
    schedule_interval=timedelta(days=1),
}

def feature_engineering(**kwargs):
    from sklearn.datasets import load_iris
    import pandas as pd

    iris = load_iris()
    X = pd.DataFrame(iris.data, columns=iris.feature_names)
    y = pd.Series(iris.target, name='target')

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    ti = kwargs['ti']
    ti.xcom_push(key='X_train', value=X_train.to_json())
    ti.xcom_push(key='X_test', value=X_test.to_json())
    ti.xcom_push(key='y_train', value=y_train.to_json(orient='records'))
    ti.xcom_push(key='y_test', value=y_test.to_json(orient='records'))

def train_model(model_name, **kwargs):
    ti = kwargs['ti']
    x_train = pd.read_json(ti.xcom_pull(key='X_train', task_ids='feature_engineering'))
    x_train = pd.read_json(ti.xcom_pull(key='X_test', task_ids='feature_engineering'))
    y_train = pd.read_json(ti.xcom_pull(key='y_train', task_ids='feature_engineering'), typ='series')
    y_test = pd.read_json(ti.xcom_pull(key='y_test', task_ids='feature_engineering'), typ='series')

    if model_name == 'RandomForest':
        model = RandomForestClassifier()
    elif model_name == 'GradientBoosting':
        model = GradientBoostingClassifier()
    else:
        raise ValueError('Unsupported model:' + model_name)
    model.fit(x_train, y_train)
    prediced = model.predict(x_test)
    performance = accuracy_score(y_test, predicted)

    ti.xcom_push(key=f'performance_{model_name}', value=performance)


def selection_best_model(**kwargs):
    ti = kwargs['ti']
    rf_performance = ti.xcom_pull(key='performance_RandomForest', task_ids='train_rf')
    gb_performance = ti.xcom_pull(key='performance_GradientBoosting', task_ids='train_gb')

    best_model = 'RandomForest' if rf_performance > gb_performance else 'GradientBoosting'
    print(f"Best model is {best_model} with performance {max(rf_performance, gb_performance)}")

    return best_model

with dag:
    tr1 = PythonOperator(
        task_id='feature_engineering',
        python_callable=feature_engineering,
        provide_context=True
    )
    tr2 = PythonOperator(
        task_id='train_model_rf',
        python_callable=train_model,
        op_args=['model_name': 'RandomForest'],
        provide_context=True
    )
    tr3 = PythonOperator(
        task_id='train_model_gb',
        python_callable=train_model,
        op_args=['model_name': 'GradientBoosting'],
        provide_context=True
    )
    tr4 = PythonOperator(
        task_id='selection_best_model',
        python_callable=selection_best_model,
        provide_context=True
    )

    tr1 >> [tr2, tr3] >> tr4