# pyspark-roleplaying-dims

A reusable PySpark utility for the automated creation of role-playing dimensions, a common pattern in dimensional data modeling.

---

### Origin and Purpose

This project presents a polished, standalone adaptation of a utility that is actively used in a production Azure-based data warehouse. 

For this public demonstration, the core logic was extracted and refined to highlight key data engineering principles like testability and schema-aware design, independent of a larger project's specific infrastructure.

### Key Features

- **Schema-Aware Aliasing**: Automatically renames columns by prefixing them with the role's name (e.g., `is_weekend` -> `order_date_is_weekend`). This behavior is robust to schema changes in the source data and can be overridden with explicit mappings for full control.

- **Test-Driven & Validated**: The core transformation logic is verified with a suite of unit tests using `pytest` and `chispa`, ensuring reliability and correctness.

- **Production-Ready Patterns**: Utilizes Python's standard `logging` module for seamless integration into ETL pipelines and is decoupled from any specific Spark environment by accepting the Spark session as a parameter.

### Project Structure

```
pyspark-roleplaying-dims/
│
├── src/
│   └── roleplay.py           # The main, documented utility function
│
├── tests/
│   └── test_roleplay.py      # Pytest unit tests for the transformation logic
│
├── examples/
│   ├── quickstart.ipynb      # A runnable Databricks notebook with examples
│   └── dim_date_sample.csv   # Sample data for the notebook
│
├── Execute_Tests.ipynb       # A runnable notebook for running the tests
├── README.md                 # This file
├── requirements.txt
└── pytest.ini
```

### Running the Tests

The tests are designed to run in a Spark environment like Databricks or a correctly configured local setup.

```bash
# Install dependencies
# pip install -r requirements.txt

# Run tests
pytest -v

# OR use the Execute_Tests.ipynb notebook
```