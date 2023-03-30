# How to use SQL to query data in S3 Bucket with Amazon Athena and AWS SDK for .NET

This Project provides a sample implementation that will show how to leverage [Amazon Athena](https://aws.amazon.com/athena/) from .NET Core Application using [AWS SDK for .NET](https://docs.aws.amazon.com/sdk-for-net/v3/developer-guide/welcome.html) to run standard SQL to analyze a large amount of data in [Amazon S3](https://aws.amazon.com/s3/).
To showcase a more realistic use-case, it includes a WebApp UI developed using [ReactJs](https://reactjs.org/). this WebApp contains components to demonstrate fetching COVID-19 data from API Server that uses AWS SDK for .NET to connect to Amazon Athena and run SQL Standard query from datasets on Amazon S3 files from a Data Lake account. This Data Lake account is the [aws-covid19-lake](https://registry.opendata.aws/aws-covid19-lake/) account, made available on [Registry of Open Data on AWS](https://registry.opendata.aws/)

Those ReatJs Components call .NET Core API that runs Amazon Athena Query, check the execution status, and list results. Each menu presents different views.

**Menu option _Testing By Date_**: Shows a filter by Date that presents a table with the following data: Date, State, Positive, Negative, Pending, Hospitalized, Death, Positive Increase

**Menu option _Testing By State_**: Shows a filter by State that presents a table with the following data: Date, State, Positive, Negative, Pending, Hospitalized, Death Positive Increase

**Menu option _Hospitals (Run&Go)_**: Run a request to the API server, get 200 with the Query ID, check the status of the execution; when the execution it's completed, it presents a table with the following data: Name, State, Type, ZipCode, Licensed Beds, Staffed Beds, Potential Increase in Beds

**Menu option _Hospitals (Run&Go)_**: Run request to the API server, wait for the result and presents a table with the following data: Name, State, Type, Zip Code, Licensed Beds, Staffed Beds, Potential Increase in Beds

# Steps

To run this project follow the instructions bellow:

## 1) Deploy Glue Catalog & Athena Database/Tables

Follow these basic steps to deploy the sample on LocalStack:
```
$ awslocal s3 mb s3://covid19-lake
$ awslocal s3 cp CovidLakeStack.template.json s3://covid19-lake/cfn/CovidLakeStack.template.json
$ awslocal s3 sync ./covid19-lake-data/ s3://covid19-lake/
$ awslocal cloudformation create-stack --stack-name covid-lake-stack --template-url https://covid19-lake.s3.us-east-2.amazonaws.com/cfn/CovidLakeStack.template.json --region us-west-2
```

## 2) COVID-19 Analysis

### Running queries in LocalStack

Once the stack has been deployed in LocalStack, you can run queries against the data, for example using the Athena SQL viewer in the LocalStack Web app (https://app.localstack.cloud/resources/athena/sql)

Some example queries are listed below.

To query the list of Moderna vaccine allocations:
```
SELECT * FROM covid_19.cdc_moderna_vaccine_distribution
---
| jurisdiction | week_of_allocations | first_dose_allocations | second_dose_allocations |
...
```

To query the list of Pfizer vaccine allocations:
```
SELECT * FROM covid_19.cdc_pfizer_vaccine_distribution
---
| jurisdiction | week_of_allocations | first_dose_allocations | second_dose_allocations |
...
```

To query agreggated COVID test data and cases:
```
SELECT * FROM covid_19.enigma_aggregation_us_states
---
| state_name | date | cases | tests | deaths | ...
...
```

To query hospital beds per US state:
```
SELECT * FROM covid_19.hospital_beds LIMIT 10
---
| state_name | num_licensed_beds | num_icu_beds | bed_utilization | ...
...
```

More queries following soon...

### Running queries in AWS

Some SQL Query that you can try on your own using [Amazon Athena Console UI]((https://us-west-2.console.aws.amazon.com/athena/home?region=us-west-2#query/)). This step is optional for this demo, but it helps you explore and learn more about Amazon Athena using Console UI

```sql
-- The following query returns the growth of confirmed cases for the past 7 days joined side-by-side with hospital bed availability, broken down by US county:
SELECT
  cases.fips,
  admin2 as county,
  province_state,
  confirmed,
  growth_count,
  sum(num_licensed_beds) as num_licensed_beds,
  sum(num_staffed_beds) as num_staffed_beds,
  sum(num_icu_beds) as num_icu_beds
FROM
  "covid-19"."hospital_beds" beds,
  ( SELECT
      fips,
      admin2,
      province_state,
      confirmed,
      last_value(confirmed) over (partition by fips order by last_update) - first_value(confirmed) over (partition by fips order by last_update) as growth_count,
      first_value(last_update) over (partition by fips order by last_update desc) as most_recent,
      last_update
    FROM
      "covid-19"."enigma_jhu"
    WHERE
      from_iso8601_timestamp(last_update) > now() - interval '200' day AND country_region = 'US') cases
WHERE
  beds.fips = cases.fips AND last_update = most_recent
GROUP BY cases.fips, confirmed, growth_count, admin2, province_state
ORDER BY growth_count desc

--Last 10 records regarding Testing and deaths
SELECT * FROM "covid-19"."world_cases_deaths_testing" order by "date" desc limit 10;

-- Last 10 records regarding Testing and deaths with JOIN on us_state_abbreviations to list State name
SELECT
   date,
   positive,
   negative,
   pending,
   hospitalized,
   death,
   total,
   deathincrease,
   hospitalizedincrease,
   negativeincrease,
   positiveincrease,
   sta.state AS state_abbreviation,
   abb.state

FROM "covid-19"."covid_testing_states_daily" sta
JOIN "covid-19"."us_state_abbreviations" abb ON sta.state = abb.abbreviation
limit 500;
```

## 4) Build & Run .NET Web Application

1) Go to the app root dir

```bash
cd ./src/app/AthenaNetCore/
```

2) Create AWS Credential file, **_for security precaution the file extension *.env is added to .gitignore to avoid accidental commit_**

```bash
code aws-credentials-do-not-commit.env #You can use any text editor eg: vi -> vi aws-credentials-do-not-commit.env
```

Below example of env file content, replace the XXXX... with your real AWS Credential, and add to S3_RESULT the output result you got from steep 2)

```ini
AWS_DEFAULT_REGION=us-west-2
AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
AWS_SESSION_TOKEN=XXXXX #(Optional, used only in case of temporary token, you'll need to remove this comment on the .env file)
S3_RESULT_BUCKET_NAME=s3://athena-results-netcore-s3bucket-xxxxxxxxxxxx/athena/results/ #paste the bucket name you've copied on the step 2, you'll need to remove this comment on the .env file)

```

3) Build .NET APP using docker-compose

```bash
docker-compose -f ./docker-compose.yml build
```

4) Run .NET APP docker-compose

```bash
docker-compose -f ./docker-compose.yml up
```

5) Test .NET APP via URL <http://localhost:8089/>

6) Clean up
```bash
# 1) Clean local resources
docker-compose down -v

# 2) Clean s3 objects created by Athena to store Results metadata
 aws s3 rm --recursive s3://athena-results-netcore-s3bucket-xxxxxxxxxxxx/athena/results/

# 3) Delete S3 bucket
aws cloudformation delete-stack --stack-name athena-results-netcore --region us-west-2

# 4) Delete Athena Tables
aws cloudformation delete-stack --stack-name covid-lake-stack



```
# References

<https://aws.amazon.com/blogs/big-data/a-public-data-lake-for-analysis-of-covid-19-data/>

<https://docs.aws.amazon.com/athena/latest/ug/code-samples.html>

<https://aws.amazon.com/blogs/apn/using-athena-express-to-simplify-sql-queries-on-amazon-athena/>

<https://docs.aws.amazon.com/sdk-for-net/v3/developer-guide/net-dg-config-creds.html>

<https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html>

<https://docs.aws.amazon.com/sdk-for-net/latest/developer-guide/creds-assign.html>

<https://github.com/awsdocs/aws-cloud9-user-guide/blob/master/LICENSE-SAMPLECODE>
