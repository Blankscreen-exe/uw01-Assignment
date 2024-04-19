# This Python file is the central module for a FastAPI application that specializes in processing CSV files and managing related tasks. 
# The application's endpoints enable users to upload CSV files, which are then validated for format correctness before being processed. 
# The processed data is subsequently stored in a PostgreSQL database, utilizing SQLAlchemy ORM for database interactions.
# The application's feature set includes:
# - An endpoint for CSV file uploads, which invokes a validation function to ensure the file meets the required CSV format before proceeding.
# - Asynchronous file operations and database transactions, orchestrated by FastAPI and SQLAlchemy's async capabilities, to enhance throughput and non-blocking I/O operations.
# - The scheduling of recurring tasks, such as data processing and cleanup routines, using Celery with Redis as the message broker to queue and execute these tasks at specified intervals or times.
# - An API endpoint designed to interrupt and stop active processing tasks, which interacts with Celery to revoke tasks based on their identifiers.
# - A logging mechanism that captures application events and errors, aiding in the real-time monitoring and troubleshooting of the application.
# - The loading of configuration settings from environment variables, which is handled at startup to parameterize connection strings, service endpoints, and other operational variables.
# - Integration with Redis for the purpose of tracking and updating the state of tasks, ensuring that state transitions are handled atomically and reliably.
# - A scheduled cleanup task, triggered by Celery's periodic task scheduler, to perform routine maintenance on the database and file system, thus preserving the application's integrity and performance.
#
# Functionally, the application is structured to receive CSV uploads through a specific endpoint, which then triggers the `validate_csv_format` function. 
# Upon successful validation, the `process_csv` function is called to handle the file asynchronously, storing the data in the database. 
# The `schedule_periodic_tasks` function sets up Celery tasks for regular data processing and cleanup. 
# The `stop_processing_task` endpoint allows for the cancellation of specific tasks using the `revoke_task` function. 
# The application's flow of logic is designed to ensure that each step, from file upload to data storage and task management, is executed in a seamless and orderly fashion.
# The codebase is segmented into logical blocks with extensive comments that guide developers through the application's flow, 
# detailing how endpoints trigger specific functions and how tasks are queued and managed.

from fastapi import FastAPI, UploadFile, File, HTTPException
from uuid import uuid4
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, func, and_, delete, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.future import select
import logging
import os
import redis
from dotenv import load_dotenv
import pandas as pd
import random
from celery import Celery
from celery.schedules import crontab
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
import asyncio
import json
from datetime import datetime, timedelta
from pprint import pprint as pp

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Initialize FastAPI app
app = FastAPI()

# SQLAlchemy setup
Base = declarative_base()
DATABASE_URI = os.getenv(
    # "DATABASE_URI", "sqlite+aiosqlite:///database.db"
    "DATABASE_URI", "postgresql+asyncpg://postgres:postgres@127.0.0.1/test001"
)

# Redis server configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)
REDIS_DB = os.getenv('REDIS_DB', 0)
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

# Create an asynchronous engine instance
engine = create_async_engine(DATABASE_URI, echo=True)
# Create an asynchronous session factory bound to the async engine. Create a sessionmaker factory that is configured to return asynchronous sessions. 
# This factory will be used to create new AsyncSession objects when needed
AsyncSessionLocal = sessionmaker(
    bind=engine,              # The engine instance to which the session is bound
    class_=AsyncSession,      # The class of the session to use (AsyncSession for async operations)
    expire_on_commit=False    # Prevents attributes from being expired after commit
)


# Retrieve the Celery broker URL from environment variables or use the defaul Redis port
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://127.0.0.1:6379")
# Initialize a new Celery application with the given broker URL
celery_app = Celery("main", broker=CELERY_BROKER_URL)

# Configure the Celery beat schedule to process CSV files every hour
celery_app.conf.beat_schedule = {
    'process-csv-every-hour': {  # Name of the scheduled job
        'task': 'main.process_csv_task',  # The task to run
        'schedule': crontab(minute=0, hour='*'),  # The schedule: at minute 0 of every hour
        'args': (None, None, None),  # Arguments for the task; placeholders for future use
    },
}

# Define a function to create a new table class for storing CSV data
def create_csv_table_class(unique_id):
    """
    Creates a new SQLAlchemy table class dynamically based on a unique identifier.

    This function dynamically constructs a new SQLAlchemy table class with a name and table structure
    based on the provided unique identifier. The table includes columns for storing CSV data and a relationship
    to a processed data table.

    Parameters:
    unique_id (str): A unique identifier used to construct the table name and reference it in relationships.

    Returns:
    class: A dynamically created SQLAlchemy table class for storing CSV data.
    """
    # TODO: find cause of random-ish None ids, this should never be raised
    if unique_id is None:
        raise
    # Dynamically create a class name based on the unique_id
    class_name = f"CSVTable_{unique_id}"
    
    # Define the attributes of the class, including SQLAlchemy columns
    attributes = {
        '__tablename__': f'csv_{unique_id}',
        'id': Column(Integer, primary_key=True),
        'col1': Column(String(256)),
        'col2': Column(String(256)),
        'col3': Column(String(256)),
        'col4': Column(String(256)),
        'processed_data': relationship(f"ProcessedData_{unique_id}", backref="original_data"),
    }
    
    # Use the type function to create the class dynamically
    return type(class_name, (Base,), attributes)

# Define a dynamic table creation function for processed data
def create_processed_data_table_class(unique_id):
    """
    Dynamically creates a new SQLAlchemy table class for storing processed data.

    Parameters:
    - unique_id (str): A unique identifier used to construct the table name and reference it in relationships.

    Returns:
    - class: A dynamically created SQLAlchemy table class for storing processed data.
    """
    # Dynamically create a class name based on the unique_id
    class_name = f"ProcessedData_{unique_id}"
    
    # Define the attributes of the class, including SQLAlchemy columns
    attributes = {
        '__tablename__': f'processed_{unique_id}',
        # 'extend_existing': True,
        'id': Column(Integer, primary_key=True),
        'original_data_id': Column(Integer, ForeignKey(f'csv_{unique_id}.id')),
        'status': Column(String(100)),
        'processed_date': Column(DateTime)
    }
    
    # Use the type function to create the class dynamically
    return type(class_name, (Base,), attributes)




# Helper functions for simulating user interactions in prototype - replacing later with actual user data
def open_function():
    """
    Simulates a user interaction that has a 10% chance of returning True.

    Returns:
    - bool: True with a 10% chance, otherwise False.
    """
    # 10% chance to return True and 90% chance to return False
    return random.random() < 0.1
def click_function():
    """
    Simulates a user interaction that has a 1% chance of returning True.

    Returns:
    - bool: True with a 1% chance, otherwise False.
    """
    # 1% chance to return True and 99% chance to return False
    return random.random() < 0.01




# Function to read and store CSV data asynchronously
async def read_and_store_csv(file_path, CSVTable, ProcessedDataTable, batch_size=50):
    """
    Asynchronously reads a CSV file and stores its data in the database.

    Parameters:
    - file_path (str): The path to the CSV file.
    - CSVTable (class): The SQLAlchemy table class for storing original CSV data.
    - ProcessedDataTable (class): The SQLAlchemy table class for storing processed data.
    - batch_size (int, optional): The number of records to process in each batch. Defaults to 50.

    Returns:
    - int: The total number of rows read from the CSV file.
    """
    # Read CSV file into a pandas DataFrame with predefined column names
    df = pd.read_csv(file_path, names=["col1", "col2", "col3", "col4"])
    
    # Convert DataFrame to a list of dictionaries for bulk insert
    data_dicts = df.to_dict(orient='records')
    
    async with AsyncSessionLocal() as session:
        # Process the DataFrame in batches
        for i in range(0, len(data_dicts), batch_size):
            batch = data_dicts[i:i+batch_size]
            # Create instances of CSVTable for each record in the batch
            csv_objects = []
            for record in batch:
                csv_objects.append(CSVTable(**record))
            
            await session.begin()
            session.add_all(csv_objects)
            await session.commit()

            

            # TODO: decide what values "status" should have
            processed_objects = [
                ProcessedDataTable(
                    original_data_id=csv_obj.id, status="pending", processed_date=datetime.now()
                ) for csv_obj in csv_objects
            ]

            # Add ProcessedDataTable instances to the session
            await session.begin()
            session.add_all(processed_objects)
            await session.commit()
    # Return the total number of rows read from the CSV
    return df.shape[0]


@celery_app.task(bind=True, autoretry_for=(Exception,), retry_kwargs={'max_retries': 5, 'countdown': 60})
def process_csv_task(self, original_table_name, processed_table_name, R, start_row=0, total_rows=None):
    """
    Celery task for processing CSV data in batches.

    Parameters:
    - self: Reference to the Celery task instance.
    - original_table_name (str): The name of the original CSV table.
    - processed_table_name (str): The name of the processed data table.
    - R (int): The number of rows to process in each batch.
    - start_row (int, optional): The starting row index for processing. Defaults to 0.
    - total_rows (int, optional): The total number of rows to process. If None, it will be determined dynamically.
    """
    # Create a new event loop for the task
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # Dynamically create a class for the original CSV table
    OriginalCSVTableClass = create_csv_table_class(original_table_name)
    # Dynamically create a class for the processed data table
    ProcessedDataTableClass = create_processed_data_table_class(processed_table_name)
    # If total_rows is not provided, fetch the total number of rows from the original table
    if total_rows is None:
        total_rows = loop.run_until_complete(get_total_rows(OriginalCSVTableClass))
    # Initialize processed_rows_count
    processed_rows_count = 0
    # Process rows in a loop until all rows are processed or an error occurs
    while start_row < total_rows:
        try:
            # Process a batch of R rows and get the count of processed rows
            batch_processed_count = loop.run_until_complete(
                process_csv_row(OriginalCSVTableClass, ProcessedDataTableClass, R, start_row)
            )
            # Increment the start_row by the number of processed rows
            start_row += batch_processed_count
            # Increment the total processed rows count
            processed_rows_count += batch_processed_count
            # If fewer than R rows are processed, break the loop
            if batch_processed_count < R:
                break
        except Exception as e:
            # Log the error and skip the current batch
            logging.error(f"Error processing rows starting at {start_row}: {e}")
            # Increment start_row to skip the problematic batch
            start_row += R
            # If the task has reached the maximum number of retries, re-raise the exception
            if self.request.retries >= self.max_retries:
                raise
            else:
                # Retry the task with the next batch
                self.retry(countdown=self.default_retry_delay, exc=e)
    # Save the current state of the task for future reference
    store_task_state(original_table_name, self.request.id, start_row, total_rows)
    # Close the event loop to ensure a clean state
    loop.close()


# Define an asynchronous function to get the total number of rows in a table
async def get_total_rows(OriginalCSVTableClass):
    """
    Asynchronously gets the total number of rows in a table.

    Parameters:
    - OriginalCSVTableClass (class): The SQLAlchemy table class for the original CSV data.

    Returns:
    - int: The total number of rows in the table.
    """
    # Use an asynchronous context manager to create a session
    async with AsyncSessionLocal() as session:
        try:
            # Asynchronously execute a query to count rows
            result = await session.execute(select(func.count()).select_from(OriginalCSVTableClass))
            total_rows = result.scalar()
            # Return the total number of rows found
            return total_rows
        except Exception as e:
            # If an exception occurs, rollback the session to a clean state
            await session.rollback()
            # Log the error with the exception message
            logging.error(f"Error counting rows: {e}")




# Define an asynchronous function to process a batch of CSV rows
async def process_csv_row(original_table_class, processed_table_class, R, start_row):
    """
    Asynchronously processes a batch of CSV rows.

    Parameters:
    - original_table_class (class): The SQLAlchemy table class for the original CSV data.
    - processed_table_class (class): The SQLAlchemy table class for the processed data.
    - R (int): The number of rows to process in the batch.
    - start_row (int): The starting row index for the batch.

    Returns:
    - int: The count of processed rows in the batch.
    """
    # Initialize the count of processed rows to zero
    processed_rows_count = 0
    # Start an asynchronous context manager with a session from the session factory
    async with AsyncSessionLocal() as session:
        try:
            # Get the current time to timestamp the processed data
            current_time = datetime.now()
            # Asynchronously query the database for a batch of rows starting from start_row, limited by R
            rows_to_process = await session.run_sync(lambda session: session.query(original_table_class).offset(start_row).limit(R).all())
            # Count the number of rows to process
            processed_rows_count = len(rows_to_process)
            # Initialize a list to hold processed data objects for bulk insertion
            processed_data_list = []
            # Iterate over each row to process
            for row in rows_to_process:
                # Default status is 'Delivered'
                status = "Delivered"
                # Change status to 'Opened' if the open_function returns True
                if open_function():
                    status = "Opened"
                # Change status to 'Clicked' if the click_function returns True
                if click_function():
                    status = "Clicked"
                # Create a new processed data object with the current status and timestamp
                processed_data = processed_table_class(
                    original_data_id=row.id,
                    status=status,
                    processed_date=current_time
                )
                # Append the new processed data object to the list for bulk insertion
                processed_data_list.append(processed_data)
            # Asynchronously perform a bulk insert of the processed data objects
            await session.run_sync(lambda session: session.bulk_save_objects(processed_data_list))
            # Commit the transaction to save changes to the database
            await session.commit()
        except Exception as e:
            # If an exception occurs, rollback the session to revert all changes
            await session.rollback()
            # Log the error with the exception message
            logging.error(f"Error processing rows: {e}")
    # Return the count of processed rows
    return processed_rows_count




# Function to store the task state along with the Celery task ID
def store_task_state(unique_id, task_id, next_start_row, total_rows):
    """
    Stores the state of a processing task in Redis.

    Parameters:
    - unique_id (str): The unique identifier for the CSV upload session.
    - task_id (str): The Celery task ID.
    - next_start_row (int): The next starting row index for processing.
    - total_rows (int): The total number of rows to process.
    """
    # Using Redis as a key-value store for atomic state updates
    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
    # Check if Redis server is running and accessible
    if not redis_client.ping():
        raise ConnectionError("Cannot connect to Redis server. Please ensure that Redis is running and accessible.")
    # Use a transaction to ensure atomicity of the update
    with redis_client.pipeline() as pipe:
        try:
            # Watch the unique_id key for changes
            pipe.watch(unique_id)
            # Start a transaction
            pipe.multi()
            # Store the task_id, next_start_row, and total_rows as a hash using hset
            pipe.hset(unique_id, mapping={'task_id': task_id, 'next_start_row': next_start_row, 'total_rows': total_rows})
            # Execute the transaction
            pipe.execute()
        except redis.WatchError:
            # Log an error if the transaction failed due to changes to the unique_id key
            logging.error(f"Transaction failed: The task state for unique_id {unique_id} has been modified by another process.")
        finally:
            # Always unwatch the key
            pipe.unwatch()




# Function to retrieve the task ID using the unique_id
def get_task_id_by_unique_id(unique_id):
    """
    Retrieves the Celery task ID using the unique identifier of the CSV upload session.

    Parameters:
    - unique_id (str): The unique identifier for the CSV upload session.

    Returns:
    - str or None: The Celery task ID if found, otherwise None.
    """
    # Initialize a Redis client with the specified host, port, database index, and password
    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
    try:
        # Attempt to retrieve the task_id associated with the unique_id from Redis
        task_id = redis_client.hget(unique_id, 'task_id')
        # Check if the task_id was successfully retrieved
        if task_id:
            # If task_id is found, decode it from bytes to a UTF-8 string and return it
            return task_id.decode('utf-8')
        # If task_id is not found, return None
        return None
    except Exception as e:
        # If an exception occurs, log the error with the unique_id and exception message
        logging.error(f"Error retrieving task ID for unique_id {unique_id}: {e}")
        # Return None to indicate that the task_id could not be retrieved
        return None



def get_next_minute_start():
    """
    Calculates the start of the next minute from the current time.

    Returns:
    - datetime: A datetime object representing the start of the next minute.
    """
    
    # Get the current time in UTC
    current_time = datetime.utcnow()
    # Calculate the start of the next minute
    next_minute_start = current_time.replace(second=0, microsecond=0) + timedelta(minutes=2)
    # Return the start of the next minute
    return next_minute_start



################################################
#                  UPLOAD CSV                  #
################################################

# Define an endpoint for uploading a CSV file and initiating its processing
@app.post("/upload_csv/")
async def upload_csv(file: UploadFile = File(...), r: int = 1):
    # Check if the uploaded file has a .csv extension
    if not file.filename.endswith(".csv"):
        # Raise an HTTP exception if the file is not a CSV
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Only CSV files are accepted.",
        )
    # Generate a unique identifier for the CSV upload session
    unique_id = str(uuid4())
    # Create a new table class for the original CSV data
    CSVTable = create_csv_table_class(unique_id)
    # Create a new table class for the processed data
    ProcessedDataTable = create_processed_data_table_class(unique_id)

    async with engine.connect() as conn:
        # Begin a transaction
        async with conn.begin():
            # Create the table for the original CSV data
            await conn.run_sync(CSVTable.__table__.create)
            # Create the table for the processed data
            await conn.run_sync(ProcessedDataTable.__table__.create)
        
        # Commit the transaction
        await conn.commit()

    # Define the file location for the uploaded CSV
    file_location = f"./temp/{unique_id}.csv"
    with open(file_location, "wb+") as file_object:
        file_object.write(file.file.read())
    # Store the CSV data in the database and retrieve the total number of rows
    total_rows = await read_and_store_csv(file_location, CSVTable, ProcessedDataTable)
    # Schedule the first processing task using Celery Beat
    process_csv_task.apply_async(args=[unique_id, unique_id, r, 0, total_rows], eta=get_next_minute_start())
    # Return a success message and the unique_id for the upload session
    return {"message": "CSV upload successful. Processing will start shortly.", "unique_id": unique_id}




################################################
#                 STOP PROCESS                 #
################################################

# Define an endpoint to handle stopping a processing task
@app.post("/stop_processing/")
async def stop_processing(unique_id: str):
    # Use the unique_id to fetch the corresponding task_id
    task_id = get_task_id_by_unique_id(unique_id)
    # Check if a task_id was successfully retrieved
    if task_id:
        # Use Celery's control API to revoke the task and terminate it immediately
        celery_app.control.revoke(task_id, terminate=True)
        # Return a success message indicating the task has been stopped
        return {"message": f"Processing for {unique_id} has been stopped."}
    else:
        # If no task is found, raise an HTTP exception with a 404 status code
        raise HTTPException(
            status_code=404,
            detail=f"No ongoing processing task found for unique_id {unique_id}.",
        )




################################################
#               PAGINATION DATA                #
################################################

# This endpoint retrieves paginated processed data for a given unique_id
@app.get("/processed_data/{unique_id}/")
async def get_processed_data(unique_id: str, page: int = 1):
    # Calculate offset for pagination
    offset = (page - 1) * 15
    # Define the number of records per page
    limit = 15
    # Create a new table class for the processed data
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    # Establish an asynchronous database session
    async with AsyncSessionLocal() as session:
        # Start a new database transaction
        async with session.begin():
            # Asynchronously query the most recent 15 records with pagination
            result = await session.execute(
                select(ProcessedDataTable)
                .order_by(ProcessedDataTable.processed_date.desc())
                .offset(offset)
                .limit(limit)
            )
            # Fetch all the results
            processed_data = result.scalars().all()
    # Convert the processed data records to JSON
    processed_data_json = [row.to_dict() for row in processed_data]
    # Return the JSON response
    return {"data": processed_data_json, "page": page, "limit": limit}


# This endpoint retrieves paginated processed data with status "Opened" for a given unique_id
@app.get("/opened_processed_data/{unique_id}/")
async def get_opened_processed_data(unique_id: str, page: int = 1):
    # Calculate offset for pagination
    offset = (page - 1) * 15
    # Define the number of records per page
    limit = 15
    # Create a new table class for the processed data
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    # Establish an asynchronous database session
    async with AsyncSessionLocal() as session:
        # Start a new database transaction
        async with session.begin():
            # Asynchronously query the records with status "Opened" with pagination
            result = await session.execute(
                select(ProcessedDataTable)
                .where(ProcessedDataTable.status == "Opened")
                .order_by(ProcessedDataTable.processed_date.desc())
                .offset(offset)
                .limit(limit)
            )
            # Fetch all the results
            opened_processed_data = result.scalars().all()
    # Convert the opened processed data records to JSON
    opened_processed_data_json = [row.to_dict() for row in opened_processed_data]
    # Return the JSON response
    return {"data": opened_processed_data_json, "page": page, "limit": limit}


# This endpoint retrieves paginated processed data with status "Clicked" for a given unique_id
@app.get("/clicked_processed_data/{unique_id}/")
async def get_clicked_processed_data(unique_id: str, page: int = 1):
    # Calculate offset for pagination
    offset = (page - 1) * 15
    # Define the number of records per page
    limit = 15
    # Create a new table class for the processed data
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    # Establish an asynchronous database session
    async with AsyncSessionLocal() as session:
        # Start a new database transaction
        async with session.begin():
            # Asynchronously query the records with status "Clicked" with pagination
            result = await session.execute(
                select(ProcessedDataTable)
                .where(ProcessedDataTable.status == "Clicked")
                .order_by(ProcessedDataTable.processed_date.desc())
                .offset(offset)
                .limit(limit)
            )
            # Fetch all the results
            clicked_processed_data = result.scalars().all()
    # Convert the clicked processed data records to JSON
    clicked_processed_data_json = [row.to_dict() for row in clicked_processed_data]
    # Return the JSON response
    return {"data": clicked_processed_data_json, "page": page, "limit": limit}





################################################
#             30-DAY TOTAL SUMS                #
################################################

# TOTAL 30 DAYS COUNT OF PROCESSED DATA
@app.get("/processed_data_count/{unique_id}/")
async def get_processed_data_count(unique_id: str):
    # Define the key for caching
    cache_key = f"processed_data_count:{unique_id}"
    # Try to get the cached result
    cached_result = await redis.get(cache_key)
    if cached_result:
        # If there is a cached result, return it
        return json.loads(cached_result)
    # If there is no cached result, calculate the count
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Query to count the total rows of processed data over the last 30 days
            result = await session.execute(
                select(func.count())
                .where(ProcessedDataTable.processed_date >= thirty_days_ago)
            )
            count = result.scalar()
    # Cache the result in Redis with an expiration time of 60 minutes (3600 seconds)
    await redis.setex(cache_key, 3600, json.dumps(count))
    # Return the count
    return {"count": count}



# TOTAL 30 DAYS COUNT OF PROCESSED DATA WITH STATUS "OPENED"
@app.get("/opened_processed_data_count/{unique_id}/")
async def get_opened_processed_data_count(unique_id: str):
    # Define the key for caching
    cache_key = f"opened_processed_data_count:{unique_id}"
    # Try to get the cached result
    cached_result = await redis.get(cache_key)
    if cached_result:
        # If there is a cached result, return it
        return json.loads(cached_result)
    # If there is no cached result, calculate the count
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Query to count the total rows of processed data with status "Opened" over the last 30 days
            result = await session.execute(
                select(func.count())
                .where(
                    and_(
                        ProcessedDataTable.processed_date >= thirty_days_ago,
                        ProcessedDataTable.status == "Opened"
                    )
                )
            )
            count = result.scalar()
    # Cache the result in Redis with an expiration time of 60 minutes (3600 seconds)
    await redis.setex(cache_key, 3600, json.dumps(count))
    # Return the count
    return {"count": count}



# TOTAL 30 DAYS COUNT OF PROCESSED DATA WITH STATUS "CLICKED"
@app.get("/clicked_processed_data_count/{unique_id}/")
async def get_clicked_processed_data_count(unique_id: str):
    # Define the key for caching
    cache_key = f"clicked_processed_data_count:{unique_id}"
    # Try to get the cached result
    cached_result = await redis.get(cache_key)
    if cached_result:
        # If there is a cached result, return it
        return json.loads(cached_result)
    # If there is no cached result, calculate the count
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Query to count the total rows of processed data with status "Clicked" over the last 30 days
            result = await session.execute(
                select(func.count())
                .where(
                    and_(
                        ProcessedDataTable.processed_date >= thirty_days_ago,
                        ProcessedDataTable.status == "Clicked"
                    )
                )
            )
            count = result.scalar()
    # Cache the result in Redis with an expiration time of 60 minutes (3600 seconds)
    await redis.setex(cache_key, 3600, json.dumps(count))
    # Return the count
    return {"count": count}





################################################
#       30 DAY CHART DATA - DAILY VALUES       #
################################################


# 30 individual one-day counts of a given unique_id 
@app.get("/daily_processed_data_count/{unique_id}/")
async def get_daily_processed_data_count(unique_id: str):
    # Define the key for caching
    cache_key = f"daily_processed_data_count:{unique_id}"
    # Try to get the cached result
    cached_result = await redis.get(cache_key)
    if cached_result:
        # If there is a cached result, return it
        return json.loads(cached_result)
    # If there is no cached result, calculate the daily counts
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    daily_counts = []
    async with AsyncSessionLocal() as session:
        async with session.begin():
            for day in range(30):
                day_start = thirty_days_ago + timedelta(days=day)
                day_end = day_start + timedelta(days=1)
                # Query to count the rows of processed data for each day
                result = await session.execute(
                    select(func.count())
                    .where(
                        and_(
                            ProcessedDataTable.processed_date >= day_start,
                            ProcessedDataTable.processed_date < day_end
                        )
                    )
                )
                count = result.scalar()
                daily_counts.append(count)
    # Prepare the data in the format suitable for ChartJS
    labels = [(thirty_days_ago + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
    chartjs_data = {
        'labels': labels,
        'datasets': [{
            'label': 'Processed Data Count',
            'data': daily_counts,
            'fill': False,
            'borderColor': 'rgb(75, 192, 192)',
            'tension': 0.1
        }]
    }
    # Cache the result in Redis with an expiration time of 60 minutes (3600 seconds)
    await redis.setex(cache_key, 3600, json.dumps(chartjs_data))
    # Return the data for ChartJS
    return chartjs_data



# 30-day open chart for a given unique_id
@app.get("/opened_processed_data_count_daily/{unique_id}/")
async def get_opened_processed_data_count_daily(unique_id: str):
    # Define the key for caching
    cache_key = f"opened_processed_data_count_daily:{unique_id}"
    # Try to get the cached result
    cached_result = await redis.get(cache_key)
    if cached_result:
        # If there is a cached result, return it
        return json.loads(cached_result)
    
    # If there is no cached result, calculate the daily counts
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    daily_counts = []
    async with AsyncSessionLocal() as session:
        async with session.begin():
            for day in range(30):
                day_start = thirty_days_ago + timedelta(days=day)
                day_end = day_start + timedelta(days=1)
                # Query to count the rows of processed data with status "Opened" for each day
                result = await session.execute(
                    select(func.count())
                    .where(
                        and_(
                            ProcessedDataTable.processed_date >= day_start,
                            ProcessedDataTable.processed_date < day_end,
                            ProcessedDataTable.status == "Opened"
                        )
                    )
                )
                count = result.scalar()
                daily_counts.append(count)
    # Prepare the data in the format suitable for ChartJS
    labels = [(thirty_days_ago + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
    chartjs_data = {
        'labels': labels,
        'datasets': [{
            'label': 'Opened Processed Data Count',
            'data': daily_counts,
            'fill': False,
            'borderColor': 'rgb(54, 162, 235)',
            'tension': 0.1
        }]
    }
    # Cache the result in Redis with an expiration time of 60 minutes (3600 seconds)
    await redis.setex(cache_key, 3600, json.dumps(chartjs_data))
    # Return the data for ChartJS
    return chartjs_data




# 30 day clicked chart for a given unique_id
@app.get("/clicked_processed_data_count/{unique_id}/")
async def get_clicked_processed_data_count(unique_id: str):
    # Define the key for caching
    cache_key = f"clicked_processed_data_count:{unique_id}"
    # Try to get the cached result
    cached_result = await redis.get(cache_key)
    if cached_result:
        # If there is a cached result, return it
        return json.loads(cached_result)
    # If there is no cached result, calculate the daily counts
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    thirty_days_ago = datetime.utcnow() - timedelta(days=30)
    daily_counts = []
    async with AsyncSessionLocal() as session:
        async with session.begin():
            for day in range(30):
                day_start = thirty_days_ago + timedelta(days=day)
                day_end = day_start + timedelta(days=1)
                # Query to count the rows of processed data with status "Clicked" for each day
                result = await session.execute(
                    select(func.count())
                    .where(
                        and_(
                            ProcessedDataTable.processed_date >= day_start,
                            ProcessedDataTable.processed_date < day_end,
                            ProcessedDataTable.status == "Clicked"
                        )
                    )
                )
                count = result.scalar()
                daily_counts.append(count)
    # Prepare the data in the format suitable for ChartJS
    labels = [(thirty_days_ago + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(30)]
    chartjs_data = {
        'labels': labels,
        'datasets': [{
            'label': 'Clicked Processed Data Count',
            'data': daily_counts,
            'fill': False,
            'borderColor': 'rgb(255, 99, 132)',
            'tension': 0.1
        }]
    }
    # Cache the result in Redis with an expiration time of 60 minutes (3600 seconds)
    await redis.setex(cache_key, 3600, json.dumps(chartjs_data))
    # Return the data for ChartJS
    return chartjs_data








################################################
#       7 DAY CHART DATA - DAILY VALUES        #
################################################


# This endpoint retrieves the 7 individual one-day counts of processed data for a given unique_id for 7-day chart
@app.get("/weekly_processed_data_count/{unique_id}/")
async def get_weekly_processed_data_count(unique_id: str):
    # Define the key for caching
    cache_key = f"weekly_processed_data_count:{unique_id}"
    # Try to get the cached result
    cached_result = await redis.get(cache_key)
    if cached_result:
        # If there is a cached result, return it
        return json.loads(cached_result)
    # If there is no cached result, calculate the weekly counts
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    daily_counts = []
    async with AsyncSessionLocal() as session:
        async with session.begin():
            for day in range(7):
                day_start = seven_days_ago + timedelta(days=day)
                day_end = day_start + timedelta(days=1)
                # Query to count the rows of processed data for each day
                result = await session.execute(
                    select(func.count())
                    .where(
                        and_(
                            ProcessedDataTable.processed_date >= day_start,
                            ProcessedDataTable.processed_date < day_end
                        )
                    )
                )
                count = result.scalar()
                daily_counts.append(count)
    # Prepare the data in the format suitable for ChartJS
    labels = [(seven_days_ago + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
    chartjs_data = {
        'labels': labels,
        'datasets': [{
            'label': 'Processed Data Count',
            'data': daily_counts,
            'fill': False,
            'borderColor': 'rgb(54, 162, 235)',
            'tension': 0.1
        }]
    }
    # Cache the result in Redis with an expiration time of 60 minutes (3600 seconds)
    await redis.setex(cache_key, 3600, json.dumps(chartjs_data))
    # Return the data for ChartJS
    return chartjs_data



# This endpoint retrieves the 7 individual one-day counts of processed data with status "Opened" for a given unique_id for 7-day chart
@app.get("/weekly_opened_data_count/{unique_id}/")
async def get_weekly_opened_data_count(unique_id: str):
    # Define the key for caching
    cache_key = f"weekly_opened_data_count:{unique_id}"
    # Try to get the cached result
    cached_result = await redis.get(cache_key)
    if cached_result:
        # If there is a cached result, return it
        return json.loads(cached_result)
    # If there is no cached result, calculate the weekly counts
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    daily_counts = []
    async with AsyncSessionLocal() as session:
        async with session.begin():
            for day in range(7):
                day_start = seven_days_ago + timedelta(days=day)
                day_end = day_start + timedelta(days=1)
                # Query to count the rows of processed data with status "Opened" for each day
                result = await session.execute(
                    select(func.count())
                    .where(
                        and_(
                            ProcessedDataTable.processed_date >= day_start,
                            ProcessedDataTable.processed_date < day_end,
                            ProcessedDataTable.status == "Opened"
                        )
                    )
                )
                count = result.scalar()
                daily_counts.append(count)
    # Prepare the data in the format suitable for ChartJS
    labels = [(seven_days_ago + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
    chartjs_data = {
        'labels': labels,
        'datasets': [{
            'label': 'Opened Data Count',
            'data': daily_counts,
            'fill': False,
            'borderColor': 'rgb(75, 192, 192)',
            'tension': 0.1
        }]
    }
    # Cache the result in Redis with an expiration time of 60 minutes (3600 seconds)
    await redis.setex(cache_key, 3600, json.dumps(chartjs_data))
    # Return the data for ChartJS
    return chartjs_data



# This endpoint retrieves the 7 individual one-day counts of processed data with status "Clicked" for a given unique_id for 7-day chart
@app.get("/weekly_clicked_data_count/{unique_id}/")
async def get_weekly_clicked_data_count(unique_id: str):
    # Define the key for caching
    cache_key = f"weekly_clicked_data_count:{unique_id}"
    # Try to get the cached result
    cached_result = await redis.get(cache_key)
    if cached_result:
        # If there is a cached result, return it
        return json.loads(cached_result)
    # If there is no cached result, calculate the weekly counts
    ProcessedDataTable = create_processed_data_table_class(unique_id)
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    daily_counts = []
    async with AsyncSessionLocal() as session:
        async with session.begin():
            for day in range(7):
                day_start = seven_days_ago + timedelta(days=day)
                day_end = day_start + timedelta(days=1)
                # Query to count the rows of processed data with status "Clicked" for each day
                result = await session.execute(
                    select(func.count())
                    .where(
                        and_(
                            ProcessedDataTable.processed_date >= day_start,
                            ProcessedDataTable.processed_date < day_end,
                            ProcessedDataTable.status == "Clicked"
                        )
                    )
                )
                count = result.scalar()
                daily_counts.append(count)
    # Prepare the data in the format suitable for ChartJS
    labels = [(seven_days_ago + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
    chartjs_data = {
        'labels': labels,
        'datasets': [{
            'label': 'Clicked Data Count',
            'data': daily_counts,
            'fill': False,
            'borderColor': 'rgb(54, 162, 235)',
            'tension': 0.1
        }]
    }
    # Cache the result in Redis with an expiration time of 60 minutes (3600 seconds)
    await redis.setex(cache_key, 3600, json.dumps(chartjs_data))
    # Return the data for ChartJS
    return chartjs_data







################################################
#              CLEANUP > 60 DAYS               #
################################################

# Schedule a Celery task to clean up old processed data for all unique ids - run on daily beat schedule
@celery_app.task(name='cleanup_old_processed_data')
def cleanup_old_processed_data_task():
    # Create a new event loop for the cleanup task
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(async_cleanup_old_processed_data())
    finally:
        loop.close()

# Asynchronous function to clean up old processed data for all unique ids
async def async_cleanup_old_processed_data():
    # Get all unique ids
    unique_ids = get_all_unique_ids()
    # Calculate the date 60 days ago from the current date
    sixty_days_ago = datetime.utcnow() - timedelta(days=60)
    
    for unique_id in unique_ids:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                try:
                    # Asynchronously delete the records older than 60 days for the current unique id
                    ProcessedDataTable = create_processed_data_table_class(unique_id)
                    await session.execute(
                        delete(ProcessedDataTable)
                        .where(ProcessedDataTable.processed_date < sixty_days_ago)
                    )
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    logging.error(f"Error cleaning up old processed data for {unique_id}: {e}")

# Function to get all unique ids from the database with Redis caching
def get_all_unique_ids():
    cache_key = "unique_ids_list"
    # Try to get the cached list of unique ids
    cached_unique_ids = redis.get(cache_key)
    if cached_unique_ids:
        # If there is a cached list, return it
        return json.loads(cached_unique_ids)
    else:
        unique_ids = []
        # Create a MetaData instance that is bound to the engine
        metadata = MetaData(bind=engine)
        # Reflect the database schema to our metadata
        metadata.reflect()
        # Iterate over all table names in the database
        for table_name in metadata.tables.keys():
            # Check if the table name starts with 'processed_'
            if table_name.startswith("processed_"):
                # Extract the unique id from the table name
                unique_id = table_name.split("processed_")[1]
                # Append the unique id to the list
                unique_ids.append(unique_id)
        # Cache the list of unique ids in Redis with an expiration time of 12 hours (43200 seconds)
        redis.setex(cache_key, 43200, json.dumps(unique_ids))
        # Return the list of unique ids
        return unique_ids

# Check if the 'cleanup_old_processed_data' task is already in the beat schedule
if 'cleanup_old_processed_data' not in celery_app.conf.beat_schedule:
    # If not present, add the 'cleanup_old_processed_data' task to the beat schedule
    celery_app.conf.beat_schedule['cleanup_old_processed_data'] = {
        'task': 'cleanup_old_processed_data',  # Name of the task to be scheduled
        'schedule': crontab(minute=0, hour=0),  # Schedule the task to run daily at midnight
    }





