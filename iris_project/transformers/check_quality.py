import pandas as pd

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


@transformer
def check_quality(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """C4: custom block between load_iris and prepare_data — pass-through if clean."""
    return df


@test
def test_no_missing_values(output, *args) -> None:
    assert output.isnull().sum().sum() == 0, "Found missing values in raw Iris data"


@test
def test_exactly_three_classes(output, *args) -> None:
    assert output["target"].nunique() == 3, "Expected exactly 3 Iris classes"
