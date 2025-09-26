# HR Data Governance & Quality System

This project provides a framework for implementing a data governance system for HR data. It automates data quality checks using Elementary, generates dynamic documentation, and provides a shared space for customer collaboration.

## Project Goals
- **Automated Data Quality:** Execute data quality tests defined in dbt and fetch results using Elementary.
- **Dynamic Documentation:** Generate Markdown reports describing data schemas and embedding latest data quality results.
- **Stakeholder Collaboration:** Use a shared Markdown file for customers to submit requests, questions, or suggest new rules.

## Project Structure
- `governance/rules/data_rules.yml`: Source of truth for table/column rules and documentation.
- `governance/customer_feedback/customer_requests.md`: Shared file for customer requests, questions, and issues.
- `governance/data_quality_checker.py`: Fetches Elementary test results.
- `governance/document_generator.py`: Generates Markdown report from rules and test results.
- `docs/data_governance_report.md`: Output report for stakeholders.

## How It Works
1. Define rules in `rules/data_rules.yml`.
2. Run dbt and Elementary tests via Airflow or CLI.
3. Use `data_quality_checker.py` to fetch results.
4. Generate documentation with `document_generator.py`.
5. Collaborate with customers via `customer_requests.md`.

## Usage
```sh
pip install -r requirements.txt
python run_governance.py
```
Review the output in `docs/data_governance_report.md`.