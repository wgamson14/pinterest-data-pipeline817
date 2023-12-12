# pinterest-data-pipeline817

# Contents
- Description and Process
- Installation
- Usage
- File Structure
- License Ibformation

# Description and Process
## Description

## Process
### Configuring the EC2 Kafka Client
- Initially we had to create a .pem locally using the AWS account that was provided.
- We connected to the EC2 instance using the ssh client, before setting up kafka within the within the client EC2 machine and the IAM MSK authentication package was installed.
- The trust policy was edited to add a principal for our specific user ID to provide permissions the authenticate to the MSK cluster and further the *client.properties* file was modify accordingly.
- Once the Kafka client was set up we had to return information regarding the *Bootstrap servers string* and the *Plaintext Apache Zookeeper connection string*, which could not be done from the CLI so had to be directly found in the MSK console.
- Before running the Kafka commands we had to set the *CLASSPATH* environment variable first by adding the respective path to our *.bashrc* file, this allows it to be in-place each time a new terminal is opened up.
- Finally, three topis were created using the kafka create topic command.

### Connecting MSK CLuster to S3 Bucket
- For convenience an S3 bucket was already provided for us, within the bucket we had to download the *Confluent.io Amazon S3 Connector* and copy it into our respective bucket. Then a custom plugin was created that would link to the already created S3 bucket.
- Further a connector was created, with cnfigurations for the correct S3 bucket and user, which would mean a pluin-connectr pair will now automatically store all data passing through the cluster, in the designated S3 bucket.

### Configuring the API
- Utilising the AP gateway console on aws, we created a resource that allowed us to build a proxy integration for our API. Once created a HTTP ANY method was created inside using the endpoint urk from ouor EC2 instance.
- API was deployed and the confluent package was installed in our local EC2 machine, modifying the *kafka-rest.properties* file allow the REST proxy to perform IAM authentication.
- starting the REST proxy combined with modifyng the *user_posting_emulation.py* file our data was successfully sent to the cluster and stored within the repsective topics within.

### Databricks
- For convenice purposes an access key and secret access key were already provided for us and stored in a csv file. Our specific S3 bucket was mounted to databrick and confirmed by running the script, *display(dbutils.fs.ls('/mnt/s3_bucket'))*.
- Once mounted each batch of data was processed into its respective dataframe. 3 were created one for each topic in the cluster, *df_pin*, *df_geo* and *df_user*.

### Spark on Databricks
- Initially we had to clean our newly formualted dataframes, the dataframes were read into Databricks using Spark.
- Necessary transformations were performed to clean the data including chanigng datatypes, removng nulls, changing the column order and converting numeric dta into the same format.
- The cleaned dataframes were then saved to a parquet file, to aallow us to read them into a separate notebook, for convenince puroposes, for querying.
- The data was read into the *querying_data* notebook from the parquet files and then queried using SQL in the notebook.

### AWS MWAA
- An API token and *requirements.txt* file were already provided for us.
- An Airflow DAG was created in a python file that will trigger a Databricks notebook to run and subsequently uploaded into the *dags* folder in the *mwaa-dags-bucket*.
- Once successfully upload, the file was manually triggered on the Airflow UI to check that everything runs successfully.
