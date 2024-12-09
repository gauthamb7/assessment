# Tech Stack:
 - Airflow
 - Postgres
 - Jupyter Notebook
 - Pandas
 - Matplotlib
 - Seaborn
 - Papermil
   
# Airflow Data Pipeline

This project demonstrates an end-to-end data pipeline using Apache Airflow for data ingestion, transformation, and visualization.

---
## Steps to Set Up and Run the Pipeline

1. Create airflow_local directory
``` 
mkdir airflow_local
chmod -R 777 ./airflow_local/
```

2. Navigate to the Local Directory:

``` 
cd airflow_local
```
3. Update the .env File: Add environment-specific configurations, such as database connection details, into the .env file located in the airflow_local directory.

### Generate the AIRFLOW_UID: Run the following command to derive the AIRFLOW_UID:

```
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

4. Build and Start the Airflow Environment: 
```
docker-compose up --build -d
```

5. Access the Airflow UI:
Once the services are started, you can access the Airflow UI through the following link:

   - **URL**: [http://localhost:8080/](http://localhost:8080/)
   - **Username**: `airflow`
   - **Password**: `airflow`

6. Add New Postgres Connection to access 
   - Admin -> Connections -> "+"
   - Choose Connections type as Postgres 
   - Use host as **host.docker.internal**

7. DAGs (Data Pipeline Files)
All the pipeline files will be here, and you can view or manage them through the Airflow UI.

8. Dataset Folder
The dataset used for this project is stored in the local dataset folder. This folder contains input datasets and Jupyter notebooks that are used for data transformation and visualization.

9. Pipeline Overview:

- **e2e_pipeline.py**: This pipeline ingests data into the messages and statuses tables. After ingestion, the `transformation_visualisation` notebook is executed for data transformation and visualization.
  
- **cleanup_messages.ipynb**: Used to remove duplicate records from the Messages sheet to ensure unique IDs.
  
- **cleanup_statuses.ipynb**: Removes duplicate records from the Statuses sheet to ensure unique IDs.
  
- **output_cleanup_messages.ipynb**: The output notebook showing the processed data after cleaning the Messages data.
  
- **output_cleanup_statuses.ipynb**: The output notebook showing the processed data after cleaning the Statuses data.
  
- **transformation_visualisation.ipynb**: After the data is ingested, this notebook performs all the necessary transformations and visualizations. The results are saved in the `output_transformation_visualisation` notebook.

10. Logs Folder:
All the logs related to the Airflow tasks will be available in the logs folder.

11. Resources Folder:
The resources folder contains utility functions to execute Jupyter notebooks, load Excel data to the Postgres database, and handle other pipeline tasks.

---
