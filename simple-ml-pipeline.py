from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from datetime import datetime

import os
from airflow.decorators import task
from airflow.decorators import dag, task



@dag(dag_id='simple_ml_pipeline', start_date=datetime(2024, 4, 4), schedule_interval=None,catchup=False)
## Sample DAG using python virtual env operator
## Important to note that virtual env doesnt support xcom push and pull
## https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/python.html#id1
def bitcoin_model():
    @task.virtualenv(
    task_id="fetch_data", requirements=["scikit-learn", "pandas"], system_site_packages=False
    )
    def fetch_data():
        import os
        import pandas as pd
        from sklearn.datasets import load_iris

        BASE_DIR = "/tmp/airflow_iris"

        os.makedirs(BASE_DIR, exist_ok=True)
        iris = load_iris()
        df = pd.DataFrame(data=iris.data, columns=iris.feature_names)
        df['target'] = iris.target

        data_path = os.path.join(BASE_DIR, "iris_data.csv")
        df.to_csv(data_path, index=False)

        # ti.xcom_push(key='data_path', value=data_path)
        # return {"data_path": data_path}
        return data_path

    @task.virtualenv(
    task_id="preprocess_data", requirements=["scikit-learn", "pandas"], system_site_packages=False
    )
    def preprocess_data(data_path):
        import os
        import pickle
        import pandas as pd
        from sklearn.preprocessing import StandardScaler
        from sklearn.model_selection import train_test_split
        # data_path = ti.xcom_pull(task_ids='fetch_data')['data_path']
        print(data_path)
        df = pd.read_csv(data_path)

        print(df.head())

        X = df.drop('target', axis=1)
        y = df['target']

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=0.2, random_state=42
        )

        BASE_DIR = "/tmp/airflow_iris"
        processed_path = os.path.join(BASE_DIR, "processed.pkl")
        with open(processed_path, 'wb') as f:
            pickle.dump((X_train, X_test, y_train, y_test), f)

        # ti.xcom_push(key='processed_path', value=processed_path)

        return {"processed_path": processed_path}



    dest_file = fetch_data()
    dest_file = preprocess_data(data_path=dest_file)


bitcoin_model()