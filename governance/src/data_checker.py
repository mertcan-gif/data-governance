import os
from datetime import datetime, timedelta

# The elementary-data library provides a DataMonitoring object
# which is the main entry point for fetching test results.
from elementary.monitor.data_monitoring import DataMonitoring

def fetch_elementary_results(dbt_project_dir: str, dbt_profiles_dir: str, days_back: int = 1) -> list:
    """
    Connects to the data warehouse using dbt profiles and fetches the latest
    Elementary test results using the SDK.

    Args:
        dbt_project_dir: The path to your dbt project directory (containing dbt_project.yml).
        dbt_profiles_dir: The path to the directory containing your profiles.yml file.
        days_back: How many days back to look for test results.

    Returns:
        A list of dictionaries formatted for the documentation generator.
    """
    print("Fetching Elementary test results using the Python SDK...")
    
    # Initialize the DataMonitoring object. This object reads your dbt profile
    # to get the credentials for connecting to your data warehouse.
    # Make sure your `profiles.yml` is configured correctly!
    try:
        monitor = DataMonitoring(
            project_dir=dbt_project_dir,
            profiles_dir=dbt_profiles_dir
        )
        
        # Fetch the results of dbt test runs from the last `days_back` days.
        # This queries the tables that Elementary creates in your warehouse.
        test_results = monitor.get_test_results(
             days_back=days_back
        )

        if not test_results:
             print("No test results found in the specified time frame.")
             return []

    except Exception as e:
        print(f"\n--- ERROR ---")
        print(f"Failed to connect to data warehouse and fetch Elementary results: {e}")
        print("Please ensure that your `profiles.yml` is correctly configured at:")
        print(f"'{os.path.join(dbt_profiles_dir, 'profiles.yml')}' and that `dbt test` has been run.")
        print("--------------\n")
        return []

    # The SDK returns a rich object. We need to parse it into the simple
    # list of dictionaries that our documentation generator expects.
    formatted_results = []
    for result in test_results:
        # The Elementary SDK provides results for both tests and model/source freshness.
        # We are only interested in dbt tests for this report.
        if result.test_type == 'dbt_test':
            formatted_results.append({
                'table': result.table_name,
                'column': result.column_name,
                'test_type': result.test_name,
                'status': 'PASS' if result.status == 'pass' else 'FAIL',
                'details': result.test_results_description
            })
            
    print(f"Successfully fetched and formatted {len(formatted_results)} test results from Elementary.")
    return formatted_results

