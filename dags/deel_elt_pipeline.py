from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import datetime
import time
import requests
import pandas as pd
import os

# Default arguments

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['sayedibra624@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 4,
    'retry_delay': datetime.timedelta(minutes=5)
}

# Initialize the DAG

dag = DAG('nyt_books_data_pipeline',
          catchup=False,
          default_args=default_args,
          schedule_interval='0 22 * * *',
          start_date=datetime.datetime(2024, 2, 6),
          dagrun_timeout=datetime.timedelta(minutes=30)
          )

# Fetch the API key from Airflow variables
API_KEY = Variable.get("nyt_api_key")
pg_conn_id = 'postgres_conn'


# Function to execute SQL from file
def execute_sql_from_file(file_path):
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    with open(file_path, 'r') as file:
        sql = file.read()
        cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()


# Create staging and ODS tables task
create_staging_tables_task = PythonOperator(
    task_id='create_staging_tables',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/ddls/create_staging_tables.sql'],
    dag=dag,
)

create_ods_tables_task = PythonOperator(
    task_id='create_ods_tables',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/ddls/create_ods_tables.sql'],
    dag=dag,
)

create_dim_and_fact_tables_task = PythonOperator(
    task_id='create_dim_and_fact_tables',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/ddls/create_dim_and_fact_tables.sql'],
    dag=dag,
)


# Fetch and store data
def fetch_and_store_data(year):
    url = f'https://api.nytimes.com/svc/books/v3/lists/overview.json'
    params = {
        'api-key': API_KEY,
        'published_date': f'{year}-01-01'
    }

    response = requests.get(url, params=params)
    data = response.json()

    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Extracting and inserting book list data
    date = data['results']['bestsellers_date']
    for list_info in data['results']['lists']:
        cursor.execute('''
            INSERT INTO staging.book_list (book_list_date, list_name, display_name, list_name_encoded, updated)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
        date, list_info['list_name'], list_info['display_name'], list_info['list_name_encoded'], list_info['updated']))

        # Extracting and inserting book data
        for book in list_info['books']:
            cursor.execute('''
                INSERT INTO staging.book (book_date, list_name, rank, rank_last_week, weeks_on_list, asterisk, dagger, primary_isbn10, primary_isbn13,
                                          publisher, description, title, author, contributor, contributor_note, book_image, book_image_width,
                                          book_image_height, amazon_product_url, age_group, book_review_link, first_chapter_link, sunday_review_link,
                                          article_chapter_link)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (date, list_info['list_name'], book['rank'], book.get('rank_last_week', None), book['weeks_on_list'],
                  book.get('asterisk', 0),
                  book.get('dagger', 0), book['primary_isbn10'], book['primary_isbn13'], book['publisher'],
                  book['description'], book['title'],
                  book['author'], book['contributor'], book['contributor_note'], book['book_image'],
                  book['book_image_width'], book['book_image_height'],
                  book['amazon_product_url'], book['age_group'], book['book_review_link'], book['first_chapter_link'],
                  book['sunday_review_link'],
                  book['article_chapter_link']))

    conn.commit()
    cursor.close()
    conn.close()


# Populate ODS tables task
upsert_to_ods_task = PythonOperator(
    task_id='upsert_to_ods',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/ddls/upsert_to_ods.sql'],
    dag=dag,
)

# Truncate staging tables task
truncate_staging_tables_task = PythonOperator(
    task_id='truncate_staging_tables',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/ddls/truncate_staging_tables.sql'],
    dag=dag,
)

# Populate Dimension and Fact Tables tasks
populate_dim_tables_task = PythonOperator(
    task_id='populate_dim_tables',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/ddls/populate_dim_tables.sql'],
    dag=dag,
)

populate_fact_table_task = PythonOperator(
    task_id='populate_fact_table',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/ddls/populate_fct_tables.sql'],
    dag=dag,
)

def execute_sql_from_file(file_path, output_dir):
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    with open(file_path, 'r') as file:
        sql = file.read()
        cursor.execute(sql)
        # Fetch data from the executed query
        data = cursor.fetchall()
        # Convert data to DataFrame for easier manipulation
        df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
        # Export DataFrame to CSV
        output_file = os.path.join(output_dir, os.path.basename(file_path).replace(".sql", ".csv"))
        df.to_csv(output_file, index=False)
    conn.commit()
    cursor.close()
    conn.close()

# Define tasks to execute SQL queries from files and export to CSV
execute_query_1 = PythonOperator(
    task_id='execute_query_1',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/sql_analytical_queries/book_top_3_longest_2022.sql', '/opt/airflow/config/'],
    dag=dag,
)

execute_query_2 = PythonOperator(
    task_id='execute_query_2',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/sql_analytical_queries/lists_least_unique_books.sql', '/opt/airflow/config/'],
    dag=dag,
)

execute_query_3 = PythonOperator(
    task_id='execute_query_3',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/sql_analytical_queries/top_publishers_quarterly_rank.sql', '/opt/airflow/config/'],
    dag=dag,
)

execute_query_4 = PythonOperator(
    task_id='execute_query_4',
    python_callable=execute_sql_from_file,
    op_args=['/opt/airflow/plugins/sql_analytical_queries/book_purchases_by_team_2023.sql', '/opt/airflow/config/'],
    dag=dag,
)

# Fetch and store data tasks
fetch_and_store_data_tasks = [
    PythonOperator(
        task_id=f'fetch_and_store_data_{year}',
        python_callable=fetch_and_store_data,
        op_args=[year],
        dag=dag,
    ) for year in range(2021, 2024)
]

# Set task dependencies
create_staging_tables_task >> create_ods_tables_task >> create_dim_and_fact_tables_task >> fetch_and_store_data_tasks >> upsert_to_ods_task >> truncate_staging_tables_task >> populate_dim_tables_task >> populate_fact_table_task >> execute_query_1 >> execute_query_2 >> execute_query_3 >> execute_query_4
