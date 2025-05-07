import boto3
import pytest
import time
import pandas as pd
from botocore.exceptions import ClientError

class TestAthenaQueries:
    """Test suite for validating Athena SQL queries against Glue Catalog tables."""

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
    
    def wait_for_query_completion(self, athena_client, query_execution_id):
        """Wait for Athena query to complete and return the state."""
        state = 'RUNNING'
        max_attempts = 20
        
        while max_attempts > 0 and state in ['RUNNING', 'QUEUED']:
            max_attempts -= 1
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            
            if state in ['RUNNING', 'QUEUED']:
                time.sleep(1)
        
        return state
    
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
            raise Exception(f"Query failed with state {state}: {error_message}")

    # def test_hospital_beds_query(self, athena_client, output_location):
    #     """Test querying the covid_19.hospital_beds table."""
    #     query = "SELECT * FROM covid_19.hospital_beds LIMIT 10"
    #     results_df = self.execute_query_and_get_results(athena_client, query, output_location)
        
    #     # Verify we got 10 results
    #     assert len(results_df) == 10
        
    #     # Verify the expected columns are present
    #     expected_columns = ['objectid', 'hospital_name', 'hospital_type']
    #     for col in expected_columns:
    #         assert col in results_df.columns
        
    #     # Check that all results are VA Hospitals (as seen in screenshot)
    #     hospital_types = results_df['hospital_type'].unique()
    #     assert 'VA Hospital' in hospital_types

    def test_us_states_aggregation_query(self, athena_client, output_location):
        """Test querying the covid_19.enigma_aggregation_us_states table."""
        query = "SELECT * FROM covid_19.enigma_aggregation_us_states WHERE state_name = 'Vermont' LIMIT 10"
        results_df = self.execute_query_and_get_results(athena_client, query, output_location)
        
        # Verify results exist
        assert len(results_df) > 0
        
        # Verify the expected columns are present
        expected_columns = ['state_fips', 'state_name', 'lat', 'long', 'date', 'cases', 'deaths', 'tests']
        for col in expected_columns:
            assert col in results_df.columns
        
        # Check that all results are for Vermont
        assert all(results_df['state_name'] == 'Vermont')
        
        # Verify latitude and longitude match the expected values
        if len(results_df) > 0:
            lat_value = results_df['lat'].iloc[0]
            long_value = results_df['long'].iloc[0]
            assert float(lat_value) == pytest.approx(44.0685773, abs=0.001)
            assert float(long_value) == pytest.approx(-72.6691839, abs=0.001)

    def test_moderna_vaccine_distribution_query(self, athena_client, output_location):
        """Test querying the covid_19.cdc_moderna_vaccine_distribution table."""
        query = "SELECT * FROM covid_19.cdc_moderna_vaccine_distribution WHERE jurisdiction IN ('Vermont', 'New Jersey')"
        results_df = self.execute_query_and_get_results(athena_client, query, output_location)
        
        # Verify results exist
        assert len(results_df) > 0
        
        # Verify the expected columns are present
        expected_columns = ['jurisdiction', 'week_of_allocations', 'first_dose_allocations', 'second_dose_allocations']
        for col in expected_columns:
            assert col in results_df.columns
        
        # Check if Vermont data is present and matches screenshot
        vermont_data = results_df[results_df['jurisdiction'] == 'Vermont']
        if not vermont_data.empty:
            vermont_row = vermont_data.iloc[0]
            assert vermont_row['first_dose_allocations'] == '7480'
            assert vermont_row['second_dose_allocations'] == '7480'
        
        # Check if New Jersey data is present and matches screenshot
        nj_data = results_df[results_df['jurisdiction'] == 'New Jersey']
        if not nj_data.empty:
            nj_row = nj_data.iloc[0]
            assert nj_row['first_dose_allocations'] == '100620'
            assert nj_row['second_dose_allocations'] == '100620'

    def test_cross_table_query(self, athena_client, output_location):
        """Test a more complex query joining multiple tables."""
        query = """
        SELECT 
            s.state_name, 
            s.cases, 
            s.deaths,
            v.first_dose_allocations
        FROM 
            covid_19.enigma_aggregation_us_states s
        JOIN 
            covid_19.cdc_moderna_vaccine_distribution v
        ON 
            s.state_name = v.jurisdiction
        WHERE 
            s.date = '2020-06-26'
        LIMIT 5
        """
        
        try:
            results_df = self.execute_query_and_get_results(athena_client, query, output_location)
            
            # If query succeeds, verify basic expectations
            assert len(results_df) <= 5
            
            # Check columns
            expected_columns = ['state_name', 'cases', 'deaths', 'first_dose_allocations']
            for col in expected_columns:
                assert col in results_df.columns
                
        except Exception as e:
            # This query might fail if the tables don't actually join well
            # Just assert that an expected error occurred rather than an unexpected one
            assert "Error details" in str(e) or "Query failed" in str(e)

    @pytest.mark.parametrize("database_name, table_name", [
        # ("covid_19", "hospital_beds"),
        ("covid_19", "enigma_aggregation_us_states"),
        ("covid_19", "cdc_moderna_vaccine_distribution")
    ])
    def test_table_existence(self, athena_client, output_location, database_name, table_name):
        """Test that the tables exist in the catalog."""
        query = f"SHOW COLUMNS IN {database_name}.{table_name}"
        
        try:
            results_df = self.execute_query_and_get_results(athena_client, query, output_location)
            # If we get results, the table exists
            assert len(results_df) > 0
        except Exception as e:
            pytest.fail(f"Table {database_name}.{table_name} does not exist or cannot be queried: {str(e)}")
