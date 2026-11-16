import pandas as pd
from sklearn.metrics import classification_report, confusion_matrix

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
def evaluate_model(train_output: dict, *args, **kwargs) -> dict:
    """Second downstream block from train_model (parallel to save_results)."""
    results = train_output["predictions"]
    y_true, y_pred = results["target"], results["predicted"]

    cm = confusion_matrix(y_true, y_pred)
    report = classification_report(y_true, y_pred, output_dict=True)
    metrics_table = pd.DataFrame(report).transpose()

    print("Confusion matrix:\n", cm)
    print("\nMetrics table:\n", metrics_table)

    # Plain lists/dicts round-trip through mage-ai's variable storage reliably;
    # a numpy array / DataFrame doesn't always survive serialize-then-test intact.
    return {"confusion_matrix": cm.tolist(), "metrics_table": metrics_table.to_dict(orient="index")}


@test
def test_metrics_shape(output, *args) -> None:
    assert len(output["confusion_matrix"]) == 3
    assert all(len(row) == 3 for row in output["confusion_matrix"])
    assert "accuracy" in output["metrics_table"]
