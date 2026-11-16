import pandas as pd
from sklearn.metrics import accuracy_score
from sklearn.tree import DecisionTreeClassifier

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


FEATURES = ["sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)"]
MIN_TEST_ACCURACY = 0.80  # the gate: save_results only runs if this test passes


@transformer
def train_model(df: pd.DataFrame, *args, **kwargs) -> dict:
    max_depth = kwargs.get("max_depth")
    train_df = df[df["split"] == "train"]
    test_df = df[df["split"] == "test"]

    model = DecisionTreeClassifier(max_depth=max_depth, random_state=42)
    model.fit(train_df[FEATURES], train_df["target"])

    predictions = model.predict(test_df[FEATURES])
    test_accuracy = accuracy_score(test_df["target"], predictions)

    results = test_df[FEATURES + ["target"]].copy()
    results["predicted"] = predictions

    return {
        "model": model,
        # mage-ai's variable serializer can't reconstruct a bare None inside a
        # stored dict (it looks up builtins.NoneType, which doesn't exist on
        # any Python version) -- so report the tree's actual depth instead of
        # None when max_depth wasn't constrained.
        "max_depth": max_depth if max_depth is not None else model.get_depth(),
        "test_accuracy": test_accuracy,
        "predictions": results,
    }


@test
def test_accuracy_above_threshold(output, *args) -> None:
    assert output["test_accuracy"] >= MIN_TEST_ACCURACY, (
        f"test_accuracy={output['test_accuracy']:.3f} is below the "
        f"{MIN_TEST_ACCURACY} gate — save_results will not run"
    )
