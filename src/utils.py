
import pandas as pd

def sample_utility_function():
    """
    This is a sample utility function.
    It doesn't do anything useful, but it's here to demonstrate the structure.
    """
    return True


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

    return data_path


def preprocess_data(data_path):
    import os
    import pickle
    import pandas as pd
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    
    print(data_path)
    df = pd.read_csv(data_path)

    print(df.head())

    X = df.drop('target', axis=1)
    y = df['target']

    # scaler = StandardScaler()
    # X_scaled = scaler.fit_transform(X)

    x_scaled = log_transform_data(X)

    x_train, x_test, y_train, y_test = train_test_split(
        x_scaled, y, test_size=0.2, random_state=42
    )

    BASE_DIR = "/tmp/airflow_iris"
    processed_path = os.path.join(BASE_DIR, "processed.pkl")
    with open(processed_path, 'wb') as f:
        pickle.dump((x_train, x_test, y_train, y_test), f)


    return {"processed_path": processed_path}


def log_transform_data(df: pd.DataFrame) -> pd.DataFrame:

    import pandas as pd
    import numpy as np

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected input of type 'pandas.DataFrame', but got {type(df).__name__}.")

    log_df = np.log1p(df)
    return log_df
