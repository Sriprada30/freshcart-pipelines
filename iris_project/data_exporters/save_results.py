import os

if "data_exporter" not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_exporter
    except ImportError:
        def data_exporter(func):
            return func
if "test" not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        def test(func):
            return func


OUTPUT_PATH = "iris_predictions.csv"


@data_exporter
def save_results(train_output: dict, *args, **kwargs) -> None:
    """Only reached if train_model's test_accuracy gate passed."""
    train_output["predictions"].to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {len(train_output['predictions'])} rows to {OUTPUT_PATH}")


@test
def test_file_written(*args) -> None:
    # save_results returns None, so Mage calls this test with zero output
    # args -- the file's existence on disk is the only thing worth checking.
    assert os.path.exists(OUTPUT_PATH)
