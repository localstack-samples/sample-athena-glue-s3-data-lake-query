import boto3
import pytest
import time
import pandas as pd
from botocore.exceptions import ClientError

class TestAthenaQueries:
    """Test suite for validating Athena SQL queries against Glue Catalog tables."""

    DATABASE_NAME = 'covid-19'
    # Pick a table that is created by CloudFormation and is crucial for tests.
    # 'hospital_beds' is a good candidate as it's directly tested.
    KEY_TABLE_NAME = 'hospital_beds'
    # Or, if 'hospital_beds' is very large/complex to check quickly, pick a simpler one:
    # KEY_TABLE_NAME = 'us_state_abbreviations'

    @pytest.fixture(scope="class")
    def aws_credentials(self):
        """Fixture for AWS credentials (when using localstack)."""
        return {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "region_name": "us-east-1"
        }

    @pytest.fixture(scope="class")
    def endpoint_url(self):
        """Fixture for localstack endpoint URL."""
        return "http://localhost:4566"

    @pytest.fixture(scope="class")
    def athena_client(self, aws_credentials, endpoint_url):
        """Create an Athena client for testing."""
        return boto3.client(
            'athena',
            endpoint_url=endpoint_url,
            **aws_credentials
        )

    @pytest.fixture(scope="class")
    def s3_client(self, aws_credentials, endpoint_url):
        """Create an S3 client for testing."""
        return boto3.client(
            's3',
            endpoint_url=endpoint_url,
            **aws_credentials
        )

    @pytest.fixture(scope="class")
    def glue_client(self, aws_credentials, endpoint_url):
        """Create a Glue client for debugging and readiness checks."""
        return boto3.client(
            'glue',
            endpoint_url=endpoint_url,
            **aws_credentials
        )

    @pytest.fixture(scope="class")
    def output_bucket(self):
        """S3 bucket for Athena query results."""
        return "athena-query-results"

    @pytest.fixture(scope="class")
    def output_location(self, output_bucket):
        """S3 location for Athena query results."""
        return f"s3://{output_bucket}/query-results/"

    @pytest.fixture(scope="class", autouse=True)
    def setup_output_bucket(self, s3_client, output_bucket):
        """Create the output bucket for Athena query results if it doesn't exist."""
        try:
            s3_client.head_bucket(Bucket=output_bucket)
        except ClientError:
            s3_client.create_bucket(Bucket=output_bucket)

    @pytest.fixture(scope="class", autouse=True)
    def glue_resources_are_ready(self, glue_client):
        """
        Fixture to wait until the Glue database and a key table are confirmed to exist.
        This runs automatically for the TestAthenaQueries class due to autouse=True.
        """
        MAX_ATTEMPTS_GLUE = 20  # Roughly 20 * 5s = 100 seconds timeout for Glue checks
        SLEEP_INTERVAL_GLUE = 5  # Seconds

        print(f"Waiting for Glue database '{self.DATABASE_NAME}' to be ready...")
        for attempt in range(MAX_ATTEMPTS_GLUE):
            try:
                glue_client.get_database(Name=self.DATABASE_NAME)
                print(f"Glue database '{self.DATABASE_NAME}' is ready.")
                break
            except glue_client.exceptions.EntityNotFoundException:
                if attempt < MAX_ATTEMPTS_GLUE - 1:
                    print(f"Database '{self.DATABASE_NAME}' not found yet (attempt {attempt + 1}/{MAX_ATTEMPTS_GLUE}). Retrying in {SLEEP_INTERVAL_GLUE}s...")
                    time.sleep(SLEEP_INTERVAL_GLUE)
                else:
                    pytest.fail(f"Glue database '{self.DATABASE_NAME}' not found after {MAX_ATTEMPTS_GLUE} attempts. Aborting tests.")
            except Exception as e:
                pytest.fail(f"An unexpected error occurred while checking for Glue database '{self.DATABASE_NAME}': {str(e)}")
        
        print(f"Waiting for Glue table '{self.DATABASE_NAME}.{self.KEY_TABLE_NAME}' to be ready...")
        for attempt in range(MAX_ATTEMPTS_GLUE):
            try:
                glue_client.get_table(DatabaseName=self.DATABASE_NAME, Name=self.KEY_TABLE_NAME)
                print(f"Glue table '{self.DATABASE_NAME}.{self.KEY_TABLE_NAME}' is ready.")
                break
            except glue_client.exceptions.EntityNotFoundException:
                if attempt < MAX_ATTEMPTS_GLUE - 1:
                    print(f"Table '{self.DATABASE_NAME}.{self.KEY_TABLE_NAME}' not found yet (attempt {attempt + 1}/{MAX_ATTEMPTS_GLUE}). Retrying in {SLEEP_INTERVAL_GLUE}s...")
                    time.sleep(SLEEP_INTERVAL_GLUE)
                else:
                    pytest.fail(f"Glue table '{self.DATABASE_NAME}.{self.KEY_TABLE_NAME}' not found after {MAX_ATTEMPTS_GLUE} attempts. Aborting tests.")
            except Exception as e:
                pytest.fail(f"An unexpected error occurred while checking for Glue table '{self.DATABASE_NAME}.{self.KEY_TABLE_NAME}': {str(e)}")
        
        print("Glue resources confirmed ready.")

    def wait_for_query_completion(self, athena_client, query_execution_id):
        """Wait for Athena query to complete and return the state."""
        state = 'RUNNING'
        max_attempts = 45  # Increased for more buffer (45 * 2s = 90s)
        sleep_duration = 2 

        while max_attempts > 0 and state in ['RUNNING', 'QUEUED']:
            max_attempts -= 1
            try:
                response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                state = response['QueryExecution']['Status']['State']
            except Exception as e:
                print(f"Warning: Error polling query {query_execution_id}: {str(e)}. Will retry.")
                # Potentially add a more specific exception catch if needed
            
            if state in ['RUNNING', 'QUEUED']:
                time.sleep(sleep_duration)
        
        return state

    def execute_query_and_get_results(self, athena_client, query, output_location):
        """Execute an Athena query and return the results as a pandas DataFrame with retries."""
        max_retries = 3
        last_exception = None

        for attempt in range(max_retries):
            try:
                response = athena_client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={'Database': self.DATABASE_NAME}, # Specify database context
                    ResultConfiguration={'OutputLocation': output_location}
                )
                query_execution_id = response['QueryExecutionId']
                print(f"Attempt {attempt + 1}/{max_retries}: Started query execution ID: {query_execution_id} for query: {query}")
                
                state = self.wait_for_query_completion(athena_client, query_execution_id)
                print(f"Query {query_execution_id} completed with state: {state}")

                if state == 'SUCCEEDED':
                    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
                    if len(results['ResultSet']['Rows']) <= 1 and not results['ResultSet']['Rows'][0]['Data'][0].get('VarCharValue','').strip(): # Check if header is empty too
                        print(f"Query {query_execution_id} returned no data rows.")
                        return pd.DataFrame()
                    
                    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
                    rows = [[item.get('VarCharValue', None) for item in row['Data']] for row in results['ResultSet']['Rows'][1:]]
                    return pd.DataFrame(rows, columns=columns)
                else:
                    query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']
                    error_message = query_status.get('StateChangeReason', f"Unknown error, Athena State: {state}")
                    # Athena sometimes puts more detailed errors in AthenaError
                    athena_error = query_status.get('AthenaError', {}).get('ErrorMessage')
                    if athena_error:
                        error_message = f"{error_message} | AthenaError: {athena_error}"
                    
                    print(f"Query {query_execution_id} failed. State: {state}, Reason: {error_message}")
                    last_exception = Exception(f"Query {query_execution_id} failed after attempt {attempt + 1}. State: {state}, Reason: {error_message}")
                    if attempt < max_retries - 1:
                        print("Retrying query...")
                        time.sleep(3 * (attempt + 1)) # Exponential backoff for retries
                    else:
                        raise last_exception # Raise after final attempt

            except Exception as e:
                print(f"Exception on attempt {attempt + 1}/{max_retries} for query '{query}': {str(e)}")
                last_exception = e
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1)) # Exponential backoff for retries
                else:
                    # Ensure we re-raise the exception if all retries fail
                    if isinstance(last_exception, Exception): # If it's already an exception instance
                        raise last_exception
                    else: # If it's some other throwable, wrap it
                        raise Exception(f"Query execution failed after all retries for '{query}': {str(last_exception)}")
        
        # This part should ideally not be reached if exceptions are handled correctly above.
        # If it is, it means all retries failed without raising the final exception properly.
        if last_exception:
             raise Exception(f"Query '{query}' failed after all retries. Last error: {str(last_exception)}")
        return pd.DataFrame() # Should not happen if logic is correct

    # ----- Test Cases -----
    # The `glue_resources_are_ready` fixture will run before these.

    def test_database_existence(self, athena_client, output_location):
        """Test that the covid_19 database exists in the catalog (via Athena)."""
        query = f"SHOW DATABASES LIKE '{self.DATABASE_NAME}'" # Use class variable
        try:
            results_df = self.execute_query_and_get_results(athena_client, query, output_location)
            assert not results_df.empty, f"Database '{self.DATABASE_NAME}' not found by Athena."
            databases = results_df.iloc[:, 0].values.tolist()
            assert self.DATABASE_NAME in databases, f"Database '{self.DATABASE_NAME}' not found in Athena catalog. Available: {databases}"
            print(f"Athena confirmed database '{self.DATABASE_NAME}' exists.")
        except Exception as e:
            pytest.fail(f"Failed to list databases via Athena: {str(e)}")

    def test_list_tables(self, athena_client, output_location):
        """Test listing tables in the covid_19 database via Athena."""
        # No need to specify database in query if using QueryExecutionContext
        query = "SHOW TABLES"
        try:
            results_df = self.execute_query_and_get_results(athena_client, query, output_location)
            tables = results_df.iloc[:, 0].values.tolist() if not results_df.empty else []
            print(f"Available tables in {self.DATABASE_NAME} (via Athena): {tables}")
            assert len(tables) > 0, f"No tables found in {self.DATABASE_NAME} database via Athena."
            # Check if our key table is listed by Athena
            assert self.KEY_TABLE_NAME in tables, f"Key table '{self.KEY_TABLE_NAME}' not found by Athena's SHOW TABLES."
        except Exception as e:
            pytest.fail(f"Failed to list tables in {self.DATABASE_NAME} database via Athena: {str(e)}")

    def test_hospital_beds_query(self, athena_client, output_location): # Removed glue_client as readiness is handled by fixture
        """Test querying the covid_19.hospital_beds table."""
        # The glue_resources_are_ready fixture (if KEY_TABLE_NAME is 'hospital_beds')
        # has already confirmed this table exists in Glue.
        # Now, we test if Athena can query it.
        table_to_query = 'hospital_beds'
        query = f"SELECT * FROM {table_to_query} LIMIT 10"
        try:
            results_df = self.execute_query_and_get_results(athena_client, query, output_location)
            assert not results_df.empty, f"Query on {table_to_query} returned no results."
            expected_columns = ['objectid', 'hospital_name', 'hospital_type'] # Case sensitive, check your CFN
            for col in expected_columns:
                # Adjust column name checks to be case-insensitive for flexibility with Athena results
                assert any(c.lower() == col.lower() for c in results_df.columns), f"Column '{col}' not found in results. Available: {results_df.columns.tolist()}"
            
            # Check that results include VA Hospitals (as seen in screenshot)
            # Ensure the column name used here matches exactly what pandas DataFrame has
            hospital_type_col_name = next(c for c in results_df.columns if c.lower() == 'hospital_type')
            hospital_types = results_df[hospital_type_col_name].unique()
            assert 'VA Hospital' in hospital_types, f"Expected 'VA Hospital' in hospital types but found: {hospital_types}"
        except Exception as e:
            pytest.fail(f"Could not query {self.DATABASE_NAME}.{table_to_query} table: {str(e)}")


    def test_us_states_aggregation_query(self, athena_client, output_location):
        """Test querying the covid_19.enigma_aggregation_us_states table."""
        table_to_query = 'enigma_aggregation_us_states'
        query = f"SELECT * FROM {table_to_query} WHERE state_name = 'Vermont' LIMIT 10"
        try:
            results_df = self.execute_query_and_get_results(athena_client, query, output_location)
            assert not results_df.empty, f"Query on {table_to_query} returned no results for Vermont."
            
            expected_columns = ['state_fips', 'state_name', 'lat', 'long', 'date', 'cases', 'deaths', 'tests']
            for col in expected_columns:
                assert any(c.lower() == col.lower() for c in results_df.columns), f"Column '{col}' not found in results. Available: {results_df.columns.tolist()}"

            state_name_col = next(c for c in results_df.columns if c.lower() == 'state_name')
            lat_col = next(c for c in results_df.columns if c.lower() == 'lat')
            long_col = next(c for c in results_df.columns if c.lower() == 'long')

            assert all(results_df[state_name_col] == 'Vermont'), "Not all results are for Vermont."
            if not results_df.empty:
                lat_value = results_df[lat_col].iloc[0]
                long_value = results_df[long_col].iloc[0]
                # Convert to float for comparison, handling potential None or empty strings
                lat_value_float = float(lat_value) if lat_value is not None and str(lat_value).strip() else None
                long_value_float = float(long_value) if long_value is not None and str(long_value).strip() else None

                assert lat_value_float is not None, "Latitude value is None or empty"
                assert long_value_float is not None, "Longitude value is None or empty"
                assert lat_value_float == pytest.approx(44.0685773, abs=0.001), f"Unexpected latitude value: {lat_value}"
                assert long_value_float == pytest.approx(-72.6691839, abs=0.001), f"Unexpected longitude value: {long_value}"
        except Exception as e:
            pytest.fail(f"Test {table_to_query} failed: {str(e)}")

    def test_moderna_vaccine_distribution_query(self, athena_client, output_location):
        """Test querying the covid_19.cdc_moderna_vaccine_distribution table."""
        table_to_query = 'cdc_moderna_vaccine_distribution'
        query = f"SELECT * FROM {table_to_query} WHERE jurisdiction IN ('Vermont', 'New Jersey')"
        try:
            results_df = self.execute_query_and_get_results(athena_client, query, output_location)
            assert not results_df.empty, f"Query on {table_to_query} returned no results for Vermont or New Jersey."
            
            expected_columns = ['jurisdiction', 'week_of_allocations', 'first_dose_allocations', 'second_dose_allocations']
            for col in expected_columns:
                 assert any(c.lower() == col.lower() for c in results_df.columns), f"Column '{col}' not found in results. Available: {results_df.columns.tolist()}"

            jurisdiction_col = next(c for c in results_df.columns if c.lower() == 'jurisdiction')
            first_dose_col = next(c for c in results_df.columns if c.lower() == 'first_dose_allocations')
            second_dose_col = next(c for c in results_df.columns if c.lower() == 'second_dose_allocations')

            vermont_data = results_df[results_df[jurisdiction_col] == 'Vermont']
            if not vermont_data.empty:
                vermont_row = vermont_data.iloc[0]
                assert str(vermont_row[first_dose_col]) == '7480', f"Unexpected first dose allocations for Vermont: {vermont_row[first_dose_col]}"
                assert str(vermont_row[second_dose_col]) == '7480', f"Unexpected second dose allocations for Vermont: {vermont_row[second_dose_col]}"
            
            nj_data = results_df[results_df[jurisdiction_col] == 'New Jersey']
            if not nj_data.empty:
                nj_row = nj_data.iloc[0]
                assert str(nj_row[first_dose_col]) == '100620', f"Unexpected first dose allocations for NJ: {nj_row[first_dose_col]}"
                assert str(nj_row[second_dose_col]) == '100620', f"Unexpected second dose allocations for NJ: {nj_row[second_dose_col]}"
        except Exception as e:
            pytest.fail(f"Test {table_to_query} failed: {str(e)}")

    def test_cross_table_query(self, athena_client, output_location):
        """Test a more complex query joining multiple tables."""
        query = """
        SELECT 
            s.state_name, 
            s.cases, 
            s.deaths,
            v.first_dose_allocations
        FROM 
            enigma_aggregation_us_states s
        JOIN 
            cdc_moderna_vaccine_distribution v
        ON 
            s.state_name = v.jurisdiction
        WHERE 
            s.date = '2020-06-26' 
        LIMIT 5 
        """ 
        # Note: Removed database prefix from table names as it's handled by QueryExecutionContext
        
        try:
            results_df = self.execute_query_and_get_results(athena_client, query, output_location)
            if not results_df.empty:
                assert len(results_df) <= 5, f"Expected at most 5 results, got {len(results_df)}"
                expected_columns = ['state_name', 'cases', 'deaths', 'first_dose_allocations']
                for col in expected_columns:
                    assert any(c.lower() == col.lower() for c in results_df.columns), f"Column '{col}' not found in results. Available: {results_df.columns.tolist()}"
            else:
                print("Cross-table query returned no results, which might be expected if join conditions are not met for the specific date.")
                # Depending on expectations, you might assert results_df.empty is False
                # For now, allow empty if the join simply yields nothing for that date.
        except Exception as e:
            # If the query itself is expected to sometimes fail due to data (e.g., no matching join keys on that date)
            # but not due to infra issues, this might be a skip.
            # But given the new readiness checks, an infra-related failure here is less likely.
            pytest.fail(f"Cross-table query failed unexpectedly: {str(e)}")

    @pytest.mark.parametrize("table_name_param", [
        "hospital_beds",
        "enigma_aggregation_us_states",
        "cdc_moderna_vaccine_distribution"
    ])
    def test_table_existence_and_schema_via_athena(self, athena_client, output_location, table_name_param, glue_client):
        """Test that tables exist and their schema can be retrieved via Athena."""
        # The glue_resources_are_ready fixture has run.
        # First, a quick sanity check with Glue client directly (should pass if fixture worked for this table).
        try:
            glue_client.get_table(DatabaseName=self.DATABASE_NAME, Name=table_name_param)
            print(f"Glue confirmed table {self.DATABASE_NAME}.{table_name_param} exists before Athena check.")
        except Exception as e:
            # This would be unexpected if glue_resources_are_ready checked this specific table and passed.
            pytest.fail(f"Glue check failed for {self.DATABASE_NAME}.{table_name_param} even after readiness fixture: {str(e)}")

        # Now, test with Athena's SHOW COLUMNS
        query = f"SHOW COLUMNS IN {table_name_param}"
        try:
            results_df = self.execute_query_and_get_results(athena_client, query, output_location)
            assert not results_df.empty, f"No columns found by Athena for table {self.DATABASE_NAME}.{table_name_param}"
            print(f"Athena SHOW COLUMNS for {self.DATABASE_NAME}.{table_name_param}: {results_df.iloc[:, 0].tolist()}")
        except Exception as e:
            pytest.fail(f"Athena SHOW COLUMNS for {self.DATABASE_NAME}.{table_name_param} failed: {str(e)}")
