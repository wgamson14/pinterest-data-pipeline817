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


