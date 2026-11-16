import pandas as pd
from sklearn.datasets import load_iris as _load_iris

if "data_loader" not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_loader
    except ImportError:
        def data_loader(func):
            return func
if "test" not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(func):
            return func


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """Load the sklearn Iris dataset as a DataFrame with a 'target' column."""
    return _load_iris(as_frame=True).frame


@test
def test_output_shape(output, *args) -> None:
    assert output.shape[0] == 150, "Expected 150 Iris rows"
    assert "target" in output.columns, "Expected a target column"
