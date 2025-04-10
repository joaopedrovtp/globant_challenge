# Globant's Data Engineer Challenge

Architecture

![architecture_img](/diagram.png)

My architectural intention for this challenge was to implement an ETL process using well-known and widely adopted tools in the market. To develop a robust REST API, I used the FastAPI framework written in Python, which communicates with AWS S3 to retrieve CSV files, performs transformations and batch processing, and ingests the data into a PostgreSQL database. The application runs using Docker Compose to manage both the application and the database services.

My idea was to implement authentication, security configurations, and tests for the application's methods.

Originally, I planned to deploy it on Azure using Azure Container Apps, but I wasn't able to get it running and synchronize the containers with the proper configurations in time.

![azure_img](/azure_dpl.png)
