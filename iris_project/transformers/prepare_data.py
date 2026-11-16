import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

if "transformer" not in globals():
    try:
        from mage_ai.data_preparation.decorators import transformer
    except ImportError:
        def transformer(func):
            return func
if "test" not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(func):
            return func


TEST_SIZE = 0.2  # change to 0.3 to deliberately trigger a red CI run (see A3)


def transform(df: pd.DataFrame, test_size: float = TEST_SIZE, random_state: int = 42) -> pd.DataFrame:
    """Shuffle, split into train/test and tag each row with its split."""
    df = df.copy()
    train_idx, test_idx = train_test_split(
        df.index, test_size=test_size, random_state=random_state, stratify=df["target"]
    )
    df["split"] = np.where(df.index.isin(train_idx), "train", "test")
    return df


@transformer
def prepare_data(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    test_size = kwargs.get("test_size", TEST_SIZE)
    return transform(df, test_size=test_size)


@test
def test_split_is_80_20(output, *args) -> None:
    assert (output["split"] == "train").sum() == 120
    assert (output["split"] == "test").sum() == 30


@test
def test_no_missing_values(output, *args) -> None:
    assert output.isnull().sum().sum() == 0
