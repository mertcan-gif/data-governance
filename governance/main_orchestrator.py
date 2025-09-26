import yaml
import os
from governance.src.data_checker import fetch_elementary_results
from governance.src.document_generator import generate_report

# --- Configuration ---
# These paths are crucial for dbt and Elementary to find your project and credentials.
DBT_PROJECT_DIR = os.path.abspath('.') # Assumes you run this from the project root
DBT_PROFILES_DIR = os.path.expanduser('~/.dbt/') # Default dbt profiles location

RULES_FILE_PATH = 'rules/data_rules.yml'
REPORT_OUTPUT_PATH = 'docs/data_governance_report.md'

def main():
    """
    Main function to orchestrate the data governance process by fetching
    live results from Elementary.
    """
    print("Starting data governance process...")

    # --- 1. Load Documentation Configuration ---
    try:
        with open(RULES_FILE_PATH, 'r') as f:
            rules = yaml.safe_load(f)
        print(f"Successfully loaded documentation rules from {RULES_FILE_PATH}")
    except FileNotFoundError:
        print(f"Error: Rules file not found at {RULES_FILE_PATH}")
        return

    # --- 2. Fetch Live Test Results from Elementary ---
    # This is the key step: we call the function that uses the Elementary SDK
    # to connect to the warehouse and get the results of the last dbt test run.
    check_results = fetch_elementary_results(
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_profiles_dir=DBT_PROFILES_DIR,
        days_back=1 # Look for tests run in the last day
    )

    if not check_results:
        print("Halting process: No test results were fetched from Elementary.")
        return

    # --- 3. Generate Documentation Report ---
    os.makedirs(os.path.dirname(REPORT_OUTPUT_PATH), exist_ok=True)
    print("Generating governance report from live Elementary results...")
    generate_report(rules, check_results, REPORT_OUTPUT_PATH)
    
    print("\nData governance process finished successfully!")


if __name__ == "__main__":
    main()

