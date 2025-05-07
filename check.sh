FAILED test_infra.py::TestAthenaQueries::test_hospital_beds_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_us_states_aggregation_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_table_existence[covid_19-enigma_aggregation_us_states] - Failed: Table covid_19.enigma_aggregation_us_states does not exist or cannot be queried: Query failed with state FAILED: Unknown error

=========================== short test summary info ============================
FAILED test_infra.py::TestAthenaQueries::test_hospital_beds_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_us_states_aggregation_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_moderna_vaccine_distribution_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_table_existence[covid_19-hospital_beds] - Failed: Table covid_19.hospital_beds does not exist or cannot be queried: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_table_existence[covid_19-enigma_aggregation_us_states] - Failed: Table covid_19.enigma_aggregation_us_states does not exist or cannot be queried: Query failed with state FAILED: Unknown error
========================= 5 failed, 2 passed in 11.64s =========================

=========================== short test summary info ============================
FAILED test_infra.py::TestAthenaQueries::test_hospital_beds_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_us_states_aggregation_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_moderna_vaccine_distribution_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_table_existence[covid_19-hospital_beds] - Failed: Table covid_19.hospital_beds does not exist or cannot be queried: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_table_existence[covid_19-enigma_aggregation_us_states] - Failed: Table covid_19.enigma_aggregation_us_states does not exist or cannot be queried: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_table_existence[covid_19-cdc_moderna_vaccine_distribution] - Failed: Table covid_19.cdc_moderna_vaccine_distribution does not exist or cannot be queried: Query failed with state FAILED: Unknown error
========================= 6 failed, 1 passed in 9.43s ==========================

test_infra.py:225: Failed
=========================== short test summary info ============================
FAILED test_infra.py::TestAthenaQueries::test_hospital_beds_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_us_states_aggregation_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_table_existence[covid_19-hospital_beds] - Failed: Table covid_19.hospital_beds does not exist or cannot be queried: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_table_existence[covid_19-enigma_aggregation_us_states] - Failed: Table covid_19.enigma_aggregation_us_states does not exist or cannot be queried: Query failed with state FAILED: Unknown error
========================= 4 failed, 3 passed in 11.89s =========================

============================= test session starts ==============================
platform linux -- Python 3.10.17, pytest-8.3.5, pluggy-1.5.0
rootdir: /home/runner/work/sample-query-data-s3-athena-glue/sample-query-data-s3-athena-glue
collected 7 items
test_infra.py FFF.FFF                                                    [100%]
=================================== FAILURES ===================================
__________________ TestAthenaQueries.test_hospital_beds_query __________________
self = <test_infra.TestAthenaQueries object at 0x7fb66c6880a0>
athena_client = <botocore.client.Athena object at 0x7fb66b8db5b0>
output_location = 's3://athena-query-results/query-results/'
    def test_hospital_beds_query(self, athena_client, output_location):
        """Test querying the covid_19.hospital_beds table."""
        query = "SELECT * FROM covid_19.hospital_beds LIMIT 10"
>       results_df = self.execute_query_and_get_results(athena_client, query, output_location)
test_infra.py:112: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
self = <test_infra.TestAthenaQueries object at 0x7fb66c6880a0>
athena_client = <botocore.client.Athena object at 0x7fb66b8db5b0>
query = 'SELECT * FROM covid_19.hospital_beds LIMIT 10'
output_location = 's3://athena-query-results/query-results/'
    def execute_query_and_get_results(self, athena_client, query, output_location):
        """Execute an Athena query and return the results as a pandas DataFrame."""
        # Start the query
        response = athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': output_location
            }
        )
        query_execution_id = response['QueryExecutionId']
    
        # Wait for query completion
        state = self.wait_for_query_completion(athena_client, query_execution_id)
    
        if state == 'SUCCEEDED':
            # Get query results
            results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    
            # Convert to DataFrame
            columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            rows = []
    
            # Skip the header row
            for row in results['ResultSet']['Rows'][1:]:
                data = [item.get('VarCharValue', '') if 'VarCharValue' in item else None for item in row['Data']]
                rows.append(data)
    
            return pd.DataFrame(rows, columns=columns)
        else:
            # Get error details if query failed
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
>           raise Exception(f"Query failed with state {state}: {error_message}")
E           Exception: Query failed with state FAILED: Unknown error
test_infra.py:107: Exception
______________ TestAthenaQueries.test_us_states_aggregation_query ______________
self = <test_infra.TestAthenaQueries object at 0x7fb66c6882e0>
athena_client = <botocore.client.Athena object at 0x7fb66b8db5b0>
output_location = 's3://athena-query-results/query-results/'
    def test_us_states_aggregation_query(self, athena_client, output_location):
        """Test querying the covid_19.enigma_aggregation_us_states table."""
        query = "SELECT * FROM covid_19.enigma_aggregation_us_states WHERE state_name = 'Vermont' LIMIT 10"
>       results_df = self.execute_query_and_get_results(athena_client, query, output_location)
test_infra.py:129: 
_ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 
self = <test_infra.TestAthenaQueries object at 0x7fb66c6882e0>
athena_client = <botocore.client.Athena object at 0x7fb66b8db5b0>
query = "SELECT * FROM covid_19.enigma_aggregation_us_states WHERE state_name = 'Vermont' LIMIT 10"
output_location = 's3://athena-query-results/query-results/'
    def execute_query_and_get_results(self, athena_client, query, output_location):
        """Execute an Athena query and return the results as a pandas DataFrame."""
        # Start the query
        response = athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': output_location
            }
        )
        query_execution_id = response['QueryExecutionId']
    
        # Wait for query completion
        state = self.wait_for_query_completion(athena_client, query_execution_id)
    
        if state == 'SUCCEEDED':
            # Get query results
            results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    
            # Convert to DataFrame
            columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            rows = []
    
            # Skip the header row
            for row in results['ResultSet']['Rows'][1:]:
                data = [item.get('VarCharValue', '') if 'VarCharValue' in item else None for item in row['Data']]
                rows.append(data)
    
            return pd.DataFrame(rows, columns=columns)
        else:
            # Get error details if query failed
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
>           raise Exception(f"Query failed with state {state}: {error_message}")
E           Exception: Query failed with state FAILED: Unknown error
test_infra.py:107: Exception

E           Failed: Table covid_19.enigma_aggregation_us_states does not exist or cannot be queried: Query failed with state FAILED: Unknown error

test_infra.py:225: Failed
=============================================================================================== short test summary info ================================================================================================
FAILED test_infra.py::TestAthenaQueries::test_hospital_beds_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_us_states_aggregation_query - Exception: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_table_existence[covid_19-hospital_beds] - Failed: Table covid_19.hospital_beds does not exist or cannot be queried: Query failed with state FAILED: Unknown error
FAILED test_infra.py::TestAthenaQueries::test_table_existence[covid_19-enigma_aggregation_us_states] - Failed: Table covid_19.enigma_aggregation_us_states does not exist or cannot be queried: Query failed with state FAILED: Unknown error
============================================================================================= 4 failed, 3 passed in 8.25s =============================================================================================
