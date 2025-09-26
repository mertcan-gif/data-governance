# Data Governance & Quality System

This repository provides a framework for managing, documenting, and monitoring data quality for HR data assets. It integrates automated data quality checks, metadata cataloging, and collaborative rule management.

## Project Structure

- `governance/rules/data_rules.yml`  
  Central YAML file for table and column business rules and descriptions.

- `governance/customer_feedback/customer_requests.md`  
  Shared Markdown file for customer requests, questions, and reported issues.

- `governance/src/data_quality_checker.py`  
  Script to fetch and process Elementary data quality test results.

- `governance/src/document_generator.py`  
  Script to generate Markdown reports combining business rules and test results.

- `governance/src/datahub_metadata.py`  
  Script to emit table and column metadata to DataHub for cataloging and governance.

- `governance/data_catalog.md`  
  Documentation of metadata operations and cataloging strategy.

- `governance/data_rules_registry.csv`  
  (Optional) Exported spreadsheet for collaborative rule tracking and automation.

- `governance/docs/data_governance_report.md`  
  Auto-generated report summarizing data quality checks and rule compliance.

## Workflow

1. **Define Rules:**  
   Add or update table/column rules in `data_rules.yml` or collaboratively in the spreadsheet.

2. **Run Data Quality Checks:**  
   Use dbt and Elementary to execute tests and export results.

3. **Generate Documentation:**  
   Run `document_generator.py` to produce a Markdown report for stakeholders.

4. **Catalog Metadata:**  
   Use `datahub_metadata.py` to register tables, columns, ownership, and tags in DataHub.

5. **Customer Collaboration:**  
   Track requests, questions, and issues in `customer_requests.md` or the shared spreadsheet.

## Usage

1. **Set up your Python environment:**
   ```sh
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Run governance scripts:**
   ```sh
   python governance/src/data_quality_checker.py
   python governance/src/document_generator.py
   python governance/src/datahub_metadata.py
   ```

3. **Review outputs:**
   - Data quality report: `governance/docs/data_governance_report.md`
   - Metadata documentation: `governance/data_catalog.md`

## Benefits

- Centralized business rule management
- Automated data quality monitoring
- Collaborative customer feedback and rule requests
- Integrated metadata cataloging for discoverability and governance

---
