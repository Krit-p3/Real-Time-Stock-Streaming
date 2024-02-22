# Real time stock streaming

## Description

This project utilizes Azure Databricks, Azure Data Lake Storage Gen2 (ADLS2), and Azure Event Hubs for data processing and streaming, following the Medallion architecture. The primary goal of this project is to demonstrate how to set up these Azure services and integrate them into a data pipeline, adhering to the principles of the Medallion architecture.


## Prerequisites

Before getting started, ensure you have the following:

- An Azure subscription
- Access to Azure Databricks, Azure Data Lake Gen 2, and Azure Event Hubs
- Docker installed on your local machine

## Setup

1. **Create Azure Resources:**
   - Create an Azure Databricks workspace.
   - Set up an ADLS2 storage account.
   - Create an Azure Event Hubs namespace.

2. **Environment Configuration:**
   - Set up an `.env`. on main directory
   ```env
        FINNHUB_API=<finnhub_api>
   ```
   - Set up an `.env` in streaming_quote directory 
   ```env 
        FINNHUB_API=<finnhub_api_key>
        EVENTHUBS_NAME=<eventhubs_name>
        EVENTHUB_CONNECTION_STRING=<eventhubs_connection_string>
   ```
3. **Databricks Cluster Setup:**
   - Log in to the Azure Databricks workspace.
   - Create a new cluster with the desired configurations.

4. **Databricks CLI Setup:**
   - Install the Databricks CLI on your local machine if you haven't already.
   - Authenticate the CLI with your Databricks workspace:
     ```bash
     databricks configure --token
     ```
5. **Databricks secrets scope:**
    - Set up databricks secrets scope 
    ```bash 
        databricks secrets create-scope SCOPE_NAME
    ```
    - add secrets in scope following this storage_account_name, storage_account_key, container_name, finnhub_api
    ```bash 
    databricks secrets put --scope SCOPE_NAME --key KEY --string-value VALUE

    ```

6. **Mounting ADLS2 with Databricks Notebook:**
   - In the Azure Databricks workspace, create a new notebook.
   - Use the notebook `setting.ipynb`

7. **Event Hubs Producer Setup:**
   - Navigate to the `streaming_quote` directory.
   - Build the Docker container:
     ```
     docker build -t streaming_quote .
     ```
   - Run the Docker container:
     ```
     docker run streaming_quote
     ```
8. **Continuous Integration and Continuous Deployment (CI/CD) with GitHub Actions:**
   - Two GitHub Actions workflows are included for CI/CD: `dev.yml` and `prod.yml`.
   - `dev.yml` is triggered on pushes to the `dev` branch and performs linting, formatting, testing, and deployment to the development environment.
   - `prod.yml` is triggered on pushes to the `main` branch and performs the same steps but deploys to the production environment.
   - Customize the workflows according to your project's requirements and environment.




