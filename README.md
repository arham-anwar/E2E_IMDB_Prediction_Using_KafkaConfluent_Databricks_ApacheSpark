# IMDb Movie Rating Prediction using Apache Spark, Confluent Kafka, and Databricks

## Overview

Embarking on this project has been a pivotal journey in my data engineering and machine learning engineering endeavors. Through this hands-on experience, I have honed my skills in processing large-scale data with Apache Spark, orchestrating real-time data streams with Confluent Kafka, and building predictive models on Databricks.

## Technologies Used

- **Apache Spark**: Distributed computing framework for large-scale data processing. Apache Spark provides APIs in Python, Scala, and Java, making it highly versatile and suitable for big data processing tasks.
  
- **Confluent Kafka**: Distributed event streaming platform for real-time data pipelines. Kafka provides high-throughput, fault-tolerant, and scalable messaging capabilities, essential for real-time data streaming applications.

- **Databricks**: Unified analytics platform based on Apache Spark. Databricks simplifies the process of building big data and AI solutions by providing an integrated environment for data engineering, data science, and analytics.

## Project Workflow

### 1. Data Reading and Preprocessing

- Loaded IMDb movie data from a CSV file into a Spark DataFrame.
- Selected relevant numerical features and the target variable (`imdb_score`).
- Dropped rows with missing values and converted selected features and target to `DoubleType`.

### 2. Data Splitting

- Partitioned the preprocessed data into training, batch, and stream DataFrames using an 80-10-10 ratio.

### 3. Batch Data to Kafka

- Converted 10% of the batch DataFrame to JSON.
- Pushed the JSON data to the Kafka topic "a1" using Confluent Kafka configurations.

### 4. Kafka Data Reading

- Read the data from the Kafka topic "a1" into a Spark DataFrame using Confluent Kafka configurations.

### 5. Model Training

- Trained a RandomForestRegressor model on the training DataFrame.
- Assembled features into a vector and predicted `imdb_score`.

### 6. Stream Data to Kafka and Prediction

- Converted the stream DataFrame to JSON.
- Pushed the JSON data to the Kafka topic "a1".
- Read the stream data back from Kafka and made predictions using the trained model.

## Conclusion

The integration of Confluent Kafka with Databricks and Spark enabled seamless real-time data streaming, processing, and predictive modeling on the IMDb movie dataset, facilitating enhanced data-driven insights and predictions for IMDb movie ratings.

## A Note About Learning

Embarking on this project provided an invaluable learning experience, delving deep into the realms of distributed computing, real-time data streaming, and machine learning. Working with Apache Spark, Confluent Kafka, and Databricks not only enhanced technical proficiency but also fostered a deeper appreciation for the power of integrated big data solutions in driving actionable insights and innovative solutions.

