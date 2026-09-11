import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from app.chatbot import answer_question, UNGROUNDED


class FakeNeo4j:
    """Stands in for Neo4jClient.run_cypher against a tiny in-memory
    fixture, so chatbot logic is tested without a real graph database."""

    def __init__(self, rows):
        self.rows = rows  # list of dicts, one per "Row" node

    def run_cypher(self, cypher, params=None):
        params = params or {}
        if "count(r) AS row_count" in cypher and "WHERE" not in cypher:
            return [{"row_count": len(self.rows)}]
        if "WHERE" in cypher and "count(r)" in cypher:
            col = _extract_col(cypher)
            value = params.get("value")
            matched = [r for r in self.rows if str(r.get(col)) == str(value)]
            return [{"row_count": len(matched)}]
        if "DISTINCT" in cypher:
            col = _extract_col(cypher)
            seen = []
            for r in self.rows:
                v = r.get(col)
                if v not in seen:
                    seen.append(v)
            return [{"value": v} for v in seen]
        return []


def _extract_col(cypher: str) -> str:
    # crude but sufficient for these fixed test-fabricated templates
    start = cypher.index("r.`") + 3
    end = cypher.index("`", start)
    return cypher[start:end]


FIXTURE_ROWS = [
    {"group": "Billing"}, {"group": "Billing"}, {"group": "Support"},
    {"group": "Sales"}, {"group": "Billing"},
]
COLUMNS = ["customer_id", "name", "group", "order_id", "amount"]


def test_no_dataset_uploaded_is_ungrounded():
    result = answer_question("How many rows are there?", FakeNeo4j([]), dataset_id=None, columns=[])
    assert result.grounded is False
    assert "no csv" in result.answer.lower() or "uploaded" in result.answer.lower()


def test_total_row_count_is_grounded_and_correct():
    result = answer_question("How many rows are there?", FakeNeo4j(FIXTURE_ROWS), "ds1", COLUMNS)
    assert result.grounded is True
    assert "5" in result.answer
    assert result.cypher.strip() != ""
    assert result.result == [{"row_count": 5}]


def test_filtered_count_by_real_column_is_grounded():
    result = answer_question(
        "How many rows belong to the Billing group?", FakeNeo4j(FIXTURE_ROWS), "ds1", COLUMNS,
    )
    assert result.grounded is True
    assert "3" in result.answer


def test_distinct_values_of_real_column_is_grounded():
    result = answer_question(
        "What are the distinct values of group?", FakeNeo4j(FIXTURE_ROWS), "ds1", COLUMNS,
    )
    assert result.grounded is True
    assert "Billing" in result.answer and "Support" in result.answer


def test_question_about_nonexistent_column_is_never_fabricated():
    """The single most important behavior in the whole project: a
    question referencing a column that does NOT exist in this dataset
    must never be answered as if it does -- grounded must be False and
    the answer must say so plainly, per handout Part 1."""
    result = answer_question(
        "How many rows have status = 'Approved'?", FakeNeo4j(FIXTURE_ROWS), "ds1", COLUMNS,
    )
    assert result.grounded is False
    assert result.answer.startswith(UNGROUNDED)
    assert result.result == []


def test_columns_question_lists_real_columns_only():
    result = answer_question("What columns does this data have?", FakeNeo4j(FIXTURE_ROWS), "ds1", COLUMNS)
    assert result.grounded is True
    for col in COLUMNS:
        assert col in result.answer
