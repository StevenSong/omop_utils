import datetime
from pathlib import Path

import polars as pl

from omop_utils.to_meds import (
    DEFAULT_VISIT_CONCEPT_ID,
    ConceptMap,
    SourceFile,
    SourceTable,
    table_events,
)


def naive(*args) -> datetime.datetime:
    """A timezone-naive datetime, as MEDS uses."""
    return datetime.datetime(*args)  # noqa: DTZ001


def concept_map(mapping: dict[int, str]) -> ConceptMap:
    return ConceptMap(
        pl.Series(list(mapping), dtype=pl.Int64),
        pl.Series(list(mapping.values()), dtype=pl.Utf8),
    )


def test_visit_events():
    # Ported from meds_etl's tests/test_omop.py
    visit_occurrence = pl.DataFrame(
        {
            "visit_occurrence_id": [1, 2, 3],
            "person_id": [12345, 12345, 12345],
            "visit_concept_id": [9201, 9202, 9202],
            "visit_start_date": [
                datetime.date(2024, 10, 25),
                datetime.date(2024, 10, 30),
                datetime.date(2024, 11, 1),
            ],
            "visit_end_date": [
                datetime.date(2024, 10, 28),
                datetime.date(2024, 10, 30),
                datetime.date(2024, 11, 1),
            ],
            "visit_source_concept_id": [9201, 9202, 9202],
            "discharged_to_concept_id": [38004453, 0, None],
        }
    )
    concepts = concept_map(
        {9201: "Visit/IP", 9202: "Visit/OP", 38004453: "SNOMED/38004453"}
    )
    events = pl.concat(
        table_events(
            visit_occurrence.lazy(),
            table_name="visit",
            table_details=details,
            concepts=concepts,
        ).collect()
        for details in [
            {"fallback_concept_id": DEFAULT_VISIT_CONCEPT_ID},
            {
                "concept_id_field": "discharged_to_concept_id",
                "time_field_options": ["visit_end_datetime", "visit_end_date"],
            },
        ]
    )
    assert events.sort("time").select("code", "time").to_dicts() == [
        {"code": "Visit/IP", "time": naive(2024, 10, 25, 23, 59, 59)},
        {
            "code": "SNOMED/38004453",
            "time": naive(2024, 10, 28, 23, 59, 59),
        },
        {"code": "Visit/OP", "time": naive(2024, 10, 30, 23, 59, 59)},
        {"code": "Visit/OP", "time": naive(2024, 11, 1, 23, 59, 59)},
    ]


def test_measurement_values():
    measurement = pl.DataFrame(
        {
            "person_id": [1, 1, 1, 1, 1, 1],
            "measurement_concept_id": [10, 10, 10, 10, 0, 99],
            "measurement_source_concept_id": [0, 0, 0, 20, 0, 0],
            "measurement_datetime": [naive(2020, 1, 1, 12)] * 6,
            "value_as_number": [1.5, None, None, None, None, None],
            "value_source_value": ["1.5", " 7 ", "positive", None, None, None],
            "value_as_concept_id": [0, 0, 0, 30, 30, 0],
            "measurement_source_value": ["a", "b", "c", "d", "e", "f"],
            "unit_concept_id": [40, 40, None, None, None, None],
            "unit_source_value": [None, "mg", None, None, None, None],
        },
        schema_overrides={
            "measurement_concept_id": pl.Int32,
            "unit_concept_id": pl.Int32,
        },
    )
    concepts = concept_map({10: "LOINC/10", 20: "LOCAL/20", 40: "UCUM/mg/dL"})
    events = table_events(
        measurement.lazy(),
        table_name="measurement",
        table_details={
            "string_value_field": "value_source_value",
            "numeric_value_field": "value_as_number",
            "concept_id_value_field": "value_as_concept_id",
        },
        concepts=concepts,
    ).collect()
    # concept 0 and unknown concept 99 have no code, so those rows are dropped
    assert events.select("code", "numeric_value", "text_value", "unit").rows() == [
        ("LOINC/10", 1.5, None, "UCUM/mg/dL"),
        ("LOINC/10", 7.0, None, "mg"),  # unit_source_value wins over unit_concept_id
        ("LOINC/10", None, "positive", None),
        (
            "LOCAL/20",
            None,
            "OMOP_CONCEPT_ID/30",
            None,
        ),  # has a source concept, so the value concept is kept
    ]


def test_files_for_range():
    table = SourceTable(
        "measurement",
        [{}],
        "person_id",
        [
            SourceFile(Path("a"), (1, 10)),
            SourceFile(Path("b"), (11, 20)),
            SourceFile(Path("c"), None),
        ],
    )
    assert table.files_for(None, 11) == [Path("a"), Path("c")]
    assert table.files_for(11, 12) == [Path("b"), Path("c")]
    assert table.files_for(10, 11) == [Path("a"), Path("c")]
    assert table.files_for(21, None) == [Path("c")]
