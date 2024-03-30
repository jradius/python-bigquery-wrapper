from google.cloud import bigquery
import logging
from google.api_core.exceptions import NotFound
from google.auth.exceptions import DefaultCredentialsError
import pandas as pd

class BigQueryWrapper:
    """A wrapper for the Google Cloud BigQuery service."""

    def __init__(self, project_id: str, dataset_id: str = None):
        """
        Initialize the BigQueryWrapper.

        Args:
            project_id (str): The Google Cloud project ID.
            dataset_id (str, optional): The dataset ID within the project. Defaults to None.
        """
        self.client = self.authenticate_client(project_id)
        self.dataset_id = dataset_id
        if dataset_id:
            self.dataset_ref = self.client.dataset(self.dataset_id)
            self._fetch_dataset_and_tables()

    @staticmethod
    def authenticate_client(project_id: str) -> bigquery.Client:
        """
        Ensure authentication to create a Google Cloud BigQuery client.
        
        Args:
            project_id (str): The Google Cloud project ID.
            
        Returns:
            bigquery.Client: An authenticated Google Cloud BigQuery client instance.
            
        Raises:
            DefaultCredentialsError: If no default credentials could be found, or if the credentials are not sufficient to authenticate the user with Google Cloud BigQuery. 
        """
        try:
            # Attempt to create a BigQuery client. If the user is not authenticated, this will raise an error.
            client = bigquery.Client(project=project_id)
            logging.info("Authentication successful.")
            return client
        except DefaultCredentialsError:
            # If the user is not authenticated, raise an error and instruct them to authenticate.
            raise DefaultCredentialsError("No authentication credentials found. Please authenticate using the Google Cloud SDK with the command 'gcloud auth application-default login'.")

    def _fetch_dataset_and_tables(self):
        """Fetch the dataset and tables."""
        try:
            self.dataset = self.client.get_dataset(self.dataset_ref)
            self.tables = list(self.client.list_tables(self.dataset))
        except NotFound:
            logging.error(f"Dataset '{self.dataset_id}' does not exist.")
            raise

    def select_project(self, project_id: str):
        """
        Select a project or change currently selected project.

        Args:
            project_id (str): The project ID to select.
        """
        self.client.project = project_id

    def select_dataset(self, dataset_id: str):
        """
        Select a dataset or change currently selected dataset.

        Args:
            dataset_id (str): The dataset ID to select.
        """
        self.dataset_id = dataset_id
        self.dataset_ref = self.client.dataset(dataset_id)
        self._fetch_dataset_and_tables()

    def print_dataset_info(self, dataset_id: str = None):
        """
        Print information about the dataset and .

        Args:
            dataset_id (str): The dataset ID to select.
        """
        if dataset_id is None:
            if self.dataset_id is None:
                raise ValueError("No dataset ID provided.")
            else:
                dataset_id = self.dataset_id

        print(f"Dataset ID:               {dataset_id}")
        print(f"Friendly Name:            {self.dataset.friendly_name}")
        print(f"Created:                  {self.dataset.created}")
        print(f"Last Modified:            {self.dataset.modified}")
        print(f"Default Table Expiration: {self.dataset.default_table_expiration_ms}")
        print(f"Location:                 {self.dataset.location}")
        print(f"\nDataset Description: {self.dataset.description}")
        if self.tables:
            print(f"\nTables found in {dataset_id}: {len(self.tables)} \n")
            for table in self.tables:
                print(f"\t- {table.table_id}")
        else:
            print(f"Dataset '{dataset_id}' does not have any tables.")

    def get_datasets(self) -> pd.DataFrame:
        """
        Get a DataFrame of datasets in the project and their properties.
        
        This function may take a while to run if there are many datasets in the project.
        
        Returns:
            pd.DataFrame: A DataFrame where each row represents a dataset and columns represent properties.
        """
        logging.info("Starting to fetch datasets. This may take a while...")
        datasets_info = []
        try:
            datasets = list(self.client.list_datasets())
            if datasets:
                for dataset in datasets:
                    dataset_ref = self.client.dataset(dataset.dataset_id)
                    dataset = self.client.get_dataset(dataset_ref)
                    datasets_info.append({
                        'dataset_id': dataset.dataset_id,
                        'friendly_name': dataset.friendly_name,
                        'created': dataset.created,
                        'last_modified': dataset.modified,
                        'default_table_expiration': dataset.default_table_expiration_ms,
                        'data_location': dataset.location,
                        'description': dataset.description
                    })
                logging.info("Finished fetching datasets.")
                return pd.DataFrame(datasets_info)
            else:
                logging.warning(f"Project {self.client.project} does not contain any datasets.")
                return pd.DataFrame()
        except Exception as e:
            logging.error(f"An error occurred while fetching datasets: {str(e)}")
            return pd.DataFrame()

    def get_table_details(self, table_id: str):
        """
        Print the properties of a table.
    
        Args:
            table_id (str): The ID of the table.
        """
        try:
            # Get the table
            table_ref = self.dataset_ref.table(table_id)
            table = self.client.get_table(table_ref)
    
            # Print the properties of the table
            print(f"\nTable ID:         {table.table_id}")
            print(f"Friendly Name:    {table.friendly_name}")
            print(f"Created:          {table.created}")
            print(f"Last Modified:    {table.modified}")
            print(f"Table Expiration: {table.expires}")
            print(f"Number of Rows:   {table.num_rows}")
            print(f"Number of Bytes:  {table.num_bytes}")
            print(f"\nTable Description: {table.description}")
        except NotFound:
            raise ValueError(f"Table '{table_id}' does not exist.")

    def get_table_schema(self, table_id):
        """
        Get a DataFrame of the schema of a table.
    
        Args:
            table_id (str): The ID of the table.
    
        Returns:
            pd.DataFrame: A DataFrame where each row represents a field (column) and columns represent field properties.
        """
        # Get the table
        table_ref = self.dataset_ref.table(table_id)
        table = self.client.get_table(table_ref)
    
        # Get the schema of the table
        schema = table.schema
    
        # Initialize a list to store each field's properties
        fields_info = []
    
        # For each field in the schema, get its properties and append them to the list
        for field in schema:
            fields_info.append({
                'field_name': field.name,
                'type': field.field_type,
                'mode': field.mode,
                'description': field.description,
                'fields': field.fields
            })
    
        # Convert the list of dictionaries to a DataFrame
        return pd.DataFrame(fields_info)