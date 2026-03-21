import json

import pytest

from app.nl_to_sql import convert_to_sql


with open("tests/fixtures/test_queries.json", encoding="utf-8") as fixture_file:
    test_cases = json.load(fixture_file)["test_cases"]


@pytest.mark.parametrize("tc", test_cases)
def test_sql_generation(tc):
    result = convert_to_sql(tc["question"])
    sql = result["sql"].upper()
    for expected_keyword in tc["expected_sql_contains"]:
        assert expected_keyword.upper() in sql, (
            f"[{tc['id']}] Expected '{expected_keyword}' in SQL for: {tc['question']}\nGot: {result['sql']}"
        )


def test_returns_source():
    result = convert_to_sql("top products by revenue")
    assert result["source"] in ["llm", "rule_based"]


def test_handles_empty_question():
    result = convert_to_sql("asdfghjkl random words with no meaning")
    assert result["sql"] is not None
