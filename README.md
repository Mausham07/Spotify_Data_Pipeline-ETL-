# Spotify ETL Pipeline

This project automates the extraction, transformation, and loading (ETL) of data from Spotify playlists into an AWS S3 bucket. 
It leverages Python, AWS Lambda, Amazon CloudWatch, Crawler, AWS Glue, Amazon Athena and other AWS services to streamline data processing tasks.

## Project Overview

- **Data Extraction**: Extracts playlist data from Spotify using the Spotipy library.
- **CloudWatch Integration**: Utilizes AWS CloudWatch for daily triggers to automate the data extraction process in AWS.
- **Data Transformation Trigger**: Implements triggers to initiate data transformation when new extraction is completed.
- **Data Transformation**: Transforms the extracted data into structured data frames for albums, artists, and songs.
- **Organized Data Storage**: Stores transformed data in separate folders within the S3 bucket, facilitating automatic detection by crawlers.
- **Athena Querying**: Enables querying of the transformed data using Athena for analytics and insights.


## Key Features

- **Automated Pipeline**: Automatically extracts data from Spotify playlists, transforms it, and loads it into S3.
- **Scalable Solution**: Utilizes AWS Lambda for scalable and cost-effective data processing.
- **Efficient Data Storage**: Stores structured data in an S3 bucket for easy access and analysis.
- **Maintains Data Integrity**: Archives processed data to maintain data integrity and organization.

## Prerequisites

- Python 3.x
- AWS account with access to S3 and Lambda
- Spotify Developer account for API credentials
- Git for version control

