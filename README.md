# AWS-Spark Streaming on Unstructured Data

## 1. Brief Introduction
This project implements a real-time data processing pipeline for analyzing unstructured job posting data. It leverages Apache Spark Streaming to process diverse input formats such as text and JSON, applies custom transformations using Python, and stores the structured output in AWS S3. The system runs in a Dockerized Spark cluster with scalable nodes to ensure efficient distributed processing.

### **Why We Need This Project**
Unstructured data, such as job descriptions, requirements, and qualifications, is rich in information but challenging to process due to its non-standardized format. This project addresses these challenges by:
- Extracting structured insights from unstructured data.
- Providing scalable real-time processing capabilities.
- Storing processed data in a cloud-based environment for further analysis.

---

## 2. Architecture Diagram
![Architecture Diagram](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Images/Architecture%20Diagram.png)



---

## 3. Technologies Used
| **Category**           | **Tools/Technologies**                          |
|-------------------------|------------------------------------------------|
| **Programming Language** | Python 3.9, Java 11                           |
| **Big Data Tools**       | Apache Spark (PySpark), Docker |
| **AWS Services**         | S3, IAM, Glue Crawler, Athenna  |
| **Infrastructure**       | Docker Compose, Bitnami Spark Image           |

---

## 4. Execution Guide
For detailed step-by-step instructions on setting up and executing this project, refer to the [Execution Guide](EXECUTION_GUIDE.md).

---

## 5. Datasets
The project uses sample datasets for job postings:
- [ACCOUNTING CLERK.txt](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Datasets/ACCOUNTING%20CLERK.txt)
- [JobPostings.json](https://github.com/SahilPatil2103/AWS-Spark-Streaming-on-Unstructured-Data/blob/main/Datasets/JobPostings.json)

These datasets are located in the `/Datasets` folder of the repository.
