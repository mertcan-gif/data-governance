from datetime import datetime

def generate_report(rules_config: dict, test_results: list, output_path: str):
    """
    Generates a Markdown report from the rules configuration and test results.

    Args:
        rules_config: The dictionary loaded from data_rules.yml.
        test_results: A list of dictionaries, where each represents a test result.
        output_path: The file path to save the generated Markdown report.
    """
    
    report_content = []
    
    # --- Report Header ---
    report_content.append("# Data Governance and Quality Report")
    report_content.append(f"**Report Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    report_content.append("This report provides a summary of the data quality checks performed on the HR data warehouse.")
    
    # --- Summary Section ---
    total_tests = len(test_results)
    failed_tests = sum(1 for r in test_results if r['status'] == 'FAIL')
    passed_tests = total_tests - failed_tests

    report_content.append("\n## Test Summary")
    report_content.append(f"- **Total Tests Run:** {total_tests}")
    report_content.append(f"- **Passed:** <span style='color:green;'>{passed_tests}</span>")
    report_content.append(f"- **Failed:** <span style='color:red;'>{failed_tests}</span>")

    # --- Detailed Results by Table ---
    for table in rules_config['tables']:
        table_name = table['name']
        report_content.append(f"\n## Table: `{table_name}`")
        report_content.append(f"_{table.get('description', '')}_\n")
        
        report_content.append("| Column | Description | Business Rules | Test Status | Details |")
        report_content.append("|---|---|---|---|---|")

        for column in table['columns']:
            col_name = column['name']
            description = column.get('description', '')
            rules = "<ul>" + "".join([f"<li>{r}</li>" for r in column.get('rules', [])]) + "</ul>"
            
            # Find the corresponding test results for this column
            col_results = [r for r in test_results if r.get('table') == table_name and r.get('column') == col_name]

            if not col_results:
                status_md = "⚪ N/A"
                details_md = "No tests found for this column."
                report_content.append(f"| `{col_name}` | {description} | {rules} | {status_md} | {details_md} |")
            else:
                for i, result in enumerate(col_results):
                    status_emoji = "✅ PASS" if result['status'] == 'PASS' else "❌ FAIL"
                    details = result.get('details', 'No details provided.')
                    
                    # For the first row of a multi-test column, print all info.
                    if i == 0:
                        report_content.append(f"| `{col_name}` | {description} | {rules} | {status_emoji} ({result['test_type']}) | {details} |")
                    # For subsequent rows, only print the test-specific info.
                    else:
                        report_content.append(f"| | | | {status_emoji} ({result['test_type']}) | {details} |")

    # --- Write to File ---
    try:
        with open(output_path, 'w') as f:
            f.write("\n".join(report_content))
        print(f"Successfully generated report at {output_path}")
    except IOError as e:
        print(f"Error writing report to file: {e}")

