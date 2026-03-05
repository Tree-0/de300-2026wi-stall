# HW 3 README

## Running the code
- put `sm_hw3_dag.py` in `s3://stall-munezero-final-project/dags/`. mwaa `MyAirflowEnvironment` will pick it up, and once activated, the environment will be scheduled to run.

Here is a visual of the expected outputs into the s3 bucket upon running the dag:

```
Expected S3 layout after DAG runs on MWAA:
s3://stall-munezero-final-project/
  hw3/ml-1m/
    count.json                  # run counter incremented each execution
    full_embedding.npy
    movies.dat
    rating_1.csv ... rating_4.csv
    tmp/
      merged_rating_{run_count}.csv
    output/
      recs_{run_count}_file.csv
```

## source code:
`sm_hw3_dag.py` (already in our airflow)
the `requirements.txt` already in our bucket

## Generative AI

After compiling our functions from homework 2, we had copilot generate the dag file.
```
"I need to take this pipeline to generate user recommendations from the ml-1m dataset from #{hw2_file} and construct a dag that we will then put into airflow. Here are the specifications for the dag, and remember to reference #{hw2_file} to understand the structure.

Combine the updated observations with the previous ones.
- Generate five movie recommendations for each of the following user types:
    - Cold user: a user for whom the system has no prior interaction data.
    - Top user: a randomly selected user whose number of interactions is in the top 5% of users.
- Save the recommendations in a separate file and upload to your S3 bucket, under directory recommendations. Each output file should include:
    - User_Type
    - Last_Interaction_Time
    - number of ratings observed so far
    - a list of the recommended movies
- Use a file naming scheme that prevents overwriting outputs from earlier iterations.
- Schedule all of your DAGs to be active for 48 hours in total.
"
``` 

We had an absurd amount of problems with our requirements.txt, eventually requiring us to completely scrap dags and rebuild them with a different set of requirements. total waste of time. 

```
Please rebuild this pipeline without the need for faiss as one of the module requirements. 
```