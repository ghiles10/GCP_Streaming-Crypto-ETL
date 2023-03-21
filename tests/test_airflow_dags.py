from airflow.models import DagBag


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    dag_bag = DagBag(include_examples=False)

    # If there are no errors, return a tuple of None
    return [v.strip() for v in dag_bag.import_errors.values()]


def test_file_imports():
    """Test for import errors on a file"""

    for error in get_import_errors():
        assert error is not None
