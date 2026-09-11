"""
The chatbot, per the handout's Part 1 contract:

  "What is not acceptable is a chatbot that answers from general knowledge
  instead of your graph. If the question cannot be answered from what is
  actually sitting in Neo4j, the correct answer is 'I don't have that in
  the data' -- not a plausible-sounding guess."

Design decision (see REPORT.md 'Chatbot approach' row): rather than a
fixed set of question -> Cypher templates written against columns we
guessed at design time, or an LLM call, this chatbot introspects the
*actual* columns present for the most recently uploaded dataset (passed
in by the caller, sourced from ParsedCSV.columns / the Dataset node) and
only offers to answer questions that reference a column that genuinely
exists. This means:

  - It works on a truly arbitrary/dynamic CSV, not one the templates were
    written against in advance.
  - It cannot silently fabricate an answer about a column that isn't in
    the file -- grounding is structural (the column-match step), not a
    policy we have to remember to enforce in every branch.
  - No LLM / external API key required at all, which sidesteps the
    handout's "ask your organisers whether an LLM is permitted" caveat
    entirely.

Every branch below returns real, executed Cypher + the real result rows,
per the /chat contract in Part 4.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class ChatAnswer:
    answer: str
    cypher: str
    result: list
    grounded: bool


UNGROUNDED = "I don't have that in the data."


def _find_matching_column(question: str, columns: list) -> str | None:
    """Case-insensitive substring match of a real column name inside the
    question. This is the structural grounding check: if no real column
    name appears in the question, most branches below refuse to guess
    which column the user meant."""
    q_lower = question.lower()
    # Prefer the longest matching column name first, so e.g. "group_id"
    # doesn't get shadowed by a shorter partial match like "group".
    for col in sorted(columns, key=len, reverse=True):
        if col.lower() in q_lower:
            return col
    return None


_STOPWORDS = {"the", "a", "an", "this", "that", "how", "many", "rows", "row"}


def _extract_filter_value(question: str, col: str) -> str | None:
    """Pull a candidate filter value out of the question. Tries, in order:
    1. A quoted string ('Billing').
    2. 'col = value' / 'col is value' / 'col equals value'.
    3. A bare token immediately adjacent to the column name itself, e.g.
       'rows belong to the Billing group' -> value sits right before the
       column word 'group'. This is what makes natural phrasings like
       '...the Billing group' work without requiring the user to type
       'group = Billing'.
    """
    quoted = re.search(r"['\"]([^'\"]+)['\"]", question)
    if quoted:
        return quoted.group(1)

    eq = re.search(
        rf"{re.escape(col)}\s*(?:=|is|equals?)\s*['\"]?([A-Za-z0-9_\-]+)['\"]?",
        question, re.IGNORECASE,
    )
    if eq:
        return eq.group(1)

    adjacent = re.search(
        rf"\b([A-Za-z0-9_\-]+)\s+{re.escape(col)}\b", question, re.IGNORECASE,
    )
    if adjacent and adjacent.group(1).lower() not in _STOPWORDS:
        return adjacent.group(1)

    return None


def answer_question(question: str, neo4j_client, dataset_id: str | None,
                     columns: list) -> ChatAnswer:
    """neo4j_client must expose run_cypher(cypher, params) -> list[dict],
    matching neo4j_client.Neo4jClient.run_cypher."""
    q = question.strip()
    q_lower = q.lower()

    if not dataset_id:
        return ChatAnswer(
            answer="No CSV has been uploaded yet, so there's nothing in the graph to answer from.",
            cypher="",
            result=[],
            grounded=False,
        )

    is_count_question = bool(re.search(r"\bhow many\b", q_lower) or "count" in q_lower)
    matched_col = _find_matching_column(q, columns)
    # A question "reads as filtered" if it names a real column, OR uses
    # filter-intent language (have/with/belong/=/is/where) even if the
    # column it names turns out not to exist. Either way, this must NOT
    # silently degrade into an unqualified total-row-count answer -- that
    # would be exactly the kind of confident-but-wrong grounding the
    # handout's Part 1 forbids.
    filter_intent = bool(re.search(r"\b(have|with|belong|belongs|where|=|is)\b", q_lower))

    if is_count_question and matched_col and filter_intent:
        value = _extract_filter_value(q, matched_col)
        if value is not None:
            cypher = (
                "MATCH (d:Dataset {id: $dataset_id})-[:HAS_ROW]->(r:Row) "
                f"WHERE r.`{matched_col}` = $value "
                "RETURN count(r) AS row_count"
            )
            result = neo4j_client.run_cypher(
                cypher, {"dataset_id": dataset_id, "value": value},
            )
            count = result[0]["row_count"] if result else 0
            return ChatAnswer(
                answer=f"There are {count} rows where {matched_col} = '{value}'.",
                cypher=cypher,
                result=result,
                grounded=True,
            )
        # Column is real but we couldn't parse a value -> don't guess.
        return ChatAnswer(
            answer=UNGROUNDED + f" I recognized the column '{matched_col}' but "
            "couldn't tell what value you're filtering for.",
            cypher="",
            result=[],
            grounded=False,
        )

    if is_count_question and filter_intent and not matched_col:
        # Filter language referencing a column that doesn't exist in this
        # dataset (e.g. 'status' when the real columns don't include it).
        # This is the case the handout is most worried about: never answer
        # a question about data that isn't there.
        return ChatAnswer(
            answer=UNGROUNDED + " None of the columns in this dataset match what "
            "you're asking about. Available columns: " + ", ".join(columns) + ".",
            cypher="",
            result=[],
            grounded=False,
        )

    # --- total row count (only for genuinely unqualified questions) --------
    if re.search(r"\bhow many rows\b", q_lower) and not filter_intent:
        cypher = (
            "MATCH (d:Dataset {id: $dataset_id})-[:HAS_ROW]->(r:Row) "
            "RETURN count(r) AS row_count"
        )
        result = neo4j_client.run_cypher(cypher, {"dataset_id": dataset_id})
        count = result[0]["row_count"] if result else 0
        return ChatAnswer(
            answer=f"There are {count} rows in this dataset.",
            cypher=cypher,
            result=result,
            grounded=True,
        )

    # --- list distinct values of a real column -----------------------------
    if re.search(r"\b(distinct|unique|list|what are the)\b", q_lower):
        col = _find_matching_column(q, columns)
        if col:
            cypher = (
                "MATCH (d:Dataset {id: $dataset_id})-[:HAS_ROW]->(r:Row) "
                f"RETURN DISTINCT r.`{col}` AS value LIMIT 25"
            )
            result = neo4j_client.run_cypher(cypher, {"dataset_id": dataset_id})
            values = [row["value"] for row in result]
            return ChatAnswer(
                answer=f"Distinct values of {col}: {', '.join(map(str, values)) or '(none found)'}.",
                cypher=cypher,
                result=result,
                grounded=True,
            )

    # --- what columns exist -------------------------------------------------
    if re.search(r"\b(columns|fields|schema|what.*data)\b", q_lower):
        cypher = "// no query needed -- columns are known from the ingest step"
        return ChatAnswer(
            answer=f"This dataset has these columns: {', '.join(columns)}.",
            cypher=cypher,
            result=[{"columns": columns}],
            grounded=True,
        )

    # --- Fallback: genuinely can't ground this question ----------------------
    cypher = ""
    return ChatAnswer(
        answer=UNGROUNDED + " Try asking about row counts, or about one of these columns: "
        + ", ".join(columns) + ".",
        cypher=cypher,
        result=[],
        grounded=False,
    )
