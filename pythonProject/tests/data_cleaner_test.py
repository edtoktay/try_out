import pytest
import pandas as pd
from io import StringIO
from classifier.data_cleaner import DataCleaner


@pytest.fixture
def mock_csv_content():
    return StringIO("""tag1,2021-01-01 00:00:00,100
tag1,2021-01-01 00:00:01,100
tag1,2021-01-01 00:00:02,200
tag2,2021-01-01 00:00:00,300
tag2,2021-01-01 00:00:01,300
tag2,2021-01-01 00:00:02,400
""")


def test_data_cleaner_initialization(mock_csv_content):
    cleaner = DataCleaner(mock_csv_content)
    assert not cleaner.data.empty
    assert list(cleaner.data.columns) == ['tagname', 'timestamp', 'value']


def test_clear_duplicates(mock_csv_content):
    cleaner = DataCleaner(mock_csv_content)
    df = cleaner.data
    cleaned_df = cleaner._DataCleaner__clear_duplicates(
        df, 'timestamp', ['timestamp', 'value'], is_keep_last=False)
    assert cleaned_df.duplicated(subset='timestamp').sum() == 0


def test_clean(mock_csv_content):
    cleaner = DataCleaner(mock_csv_content)
    df = cleaner.data
    cleaned_df = cleaner._DataCleaner__clean(df)
    assert cleaned_df.duplicated(subset='timestamp').sum() == 0
    assert cleaned_df.duplicated(subset='value').sum() == 0


def test_get_tag_data(mock_csv_content):
    cleaner = DataCleaner(mock_csv_content)
    tag_data = cleaner.get_tag_data()
    assert 'tag1' in tag_data
    assert 'tag2' in tag_data
    assert tag_data['tag1'].duplicated(subset='timestamp').sum() == 0
    assert tag_data['tag2'].duplicated(subset='timestamp').sum() == 0
    assert tag_data['tag1'].duplicated(subset='value').sum() == 0
    assert tag_data['tag2'].duplicated(subset='value').sum() == 0
