# Step-by-Step Execution Guide

## Prerequisites
- Docker Desktop
- AWS CLI configured with credentials
- Python 3.9+
- PyCharm (or any IDE)

---

## Step 1: Project Setup
### 1.1 Clone the Repository
```
git clone https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data.git
```


### 1.2 Project Structure
```
├── jobs/
│   ├── main.py                # Spark Streaming logic
│   ├── udf_utils.py           # Custom UDFs
│   └── config/
│       └── config.py          # AWS credentials
├── docker/
│   ├── Dockerfile             # Spark image setup
│   └── docker-compose.yml     # Cluster configuration
├── input/
│   ├── input_text/            # Sample text job postings
│   └── input_json/            # Sample JSON data
```
## Step 2: Configure AWS
### 2.1 Create S3 Bucket
![S3 main](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Images/S3%20main.png)

### 2.2 Update config.py
jobs/config/config.py
```
configuration = {
'AWS_ACCESS_Key': 'YOUR_AWS_ACCESS_KEY',
'AWS_SECRET_Key': 'YOUR_AWS_SECRET_KEY',
'S3_BUCKET': 'bucket_name',
'REGION': 'us-east-1'
}
```
Use the provided config.py script [Config File](Jobs/Config/config.py).

## Step 3: Build Docker Image
### 3.1 Dockerfile Setup
docker/Dockerfile
```
FROM bitnami/spark:3.5.5
USER root
RUN pip install pyspark boto3 pandas
RUN mkdir -p /jobs && chmod -R 777 /jobs
USER 1001
```

### 3.2 Build Image
```
docker-compose -f docker/docker-compose.yml build
```
![Docker Build](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Images/Docker%20Terminal.png)


## Step 4: Start Spark Cluster
### 4.1 Start Containers
```
docker-compose -f docker/docker-compose.yml up -d --scale spark-worker=4
```
![Docker Container](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Images/docker%20container.png)

### 4.2 Verify Containers
```
docker-compose ps
```

**Expected Output:**
```
NAME STATUS PORTS
spark-master Up 0.0.0.0:9092->8080/tcp
spark-worker-1 Up 8081/tcp
... (3 more workers)
```
![Spark cluster](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Images/Spark.png)

## Step 5: Prepare Input Data
### 5.1 Add Sample Files
- Place `ACCOUNTING-CLERK.txt` in `input/input_text/`
- Place `JobPostings.json` in `input/input_json/`

### 5.2 Validate Paths in main.py
```
text_input_dir = 'file:///jobs/input/input_text/'
json_input_dir = 'file:///jobs/input/input_json/'
```

---

## Step 6: Run Spark Streaming Job
```
docker exec spark-master /opt/bitnami/spark/bin/spark-submit
--master spark://spark-master:7077
--conf spark.hadoop.fs.s3a.access.key=YOUR_AWS_KEY
--conf spark.hadoop.fs.s3a.secret.key=YOUR_AWS_SECRET
/jobs/main.py
```
![Terminal](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Images/terminal%201.png)


## Step 7: Verify AWS Output
### 7.1 Check S3 Bucket
![S3 Checkpoint](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Images/S3%20C1.1.png)

### 7.2 Output Files
![S3 Data](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Images/S3%20d1.1.png)

## Step 8: Create Glue Crawler
### 8.1 Run Crawler
![Crawler](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Images/aws%20crawler.png)

### 8.2 Verify Results in Athenna
![Athenna](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Images/Athenna%20result.png)

### Step 9: Stop Cluster
```
docker-compose -f docker/docker-compose.yml down -v
```

### Note:
1. Replace YOUR_AWS_KEY and YOUR_AWS_SECRET with actual credentials (use environment variables in production).
2. For errors, check logs using:
   ```
   docker-compose logs -f spark-master
   ```
   
