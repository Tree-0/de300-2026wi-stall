# put this file into airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import pathlib

"""
Set up a DAG to produce a recommendation .

Combine the updated observations with the previous ones.
Generate five movie recommendations for each of the following user types:
Cold user: a user for whom the system has no prior interaction data.
Top user: a randomly selected user whose number of interactions is in the top 5% of users.
Save the recommendations in a separate file and upload to your S3 bucket, under directory recommendations. Each output file should include:
User_Type
Last_Interaction_Time
other user summary fields you choose (e.g., number of ratings observed so far)
a list of the recommended movies
Important: Use a file naming scheme that prevents overwriting outputs from earlier iterations.
"""

# Constants, resource names and paths, etc.
OUTPUT_BUCKET = "stall-munezero-mwaa"
MOVIELENS_DIR = pathlib.Path("ml-1m")

default_args = {
    "owner": "de300",
    "depends_on_past": False,
    "start_date": pendulum.today("UTC").add(days=0),
    "retries": 1,
}



