from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy import text
import os
import time
import psycopg2
import argparse
from datetime import datetime
import glob
import sys
import subprocess
import shutil
from time import time, sleep
import json

# Database connection string
TIMEOUT_LIMIT = 3 * 60 * 1000
NUM_EXECUTIONS = 3
engine = None

def current_timestamp_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def get_alchemy_engine(db_name):
    """Create SQLAlchemy engine with connection pooling"""
    global engine
    if engine is None:
        db_url = f"postgresql://suite_user:71Vgfi4mUNPm@train.darelab.athenarc.gr:5469/{db_name}"
        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            pool_pre_ping=True
        )
    return engine

def run_query(sql, db_name, bao_select=True, bao_reward=True, use_bao=True):
    measurements = []
    global engine
    if engine is None:
        print("Creating a new SQLAlchemy engine")
        engine = get_alchemy_engine(db_name)
    
    try:
        with engine.connect() as conn:
            # Set all parameters in a single execute
            # SET enable_bao TO {'on' if bao_select or bao_reward else 'off'};
            conn.execute(text(f"""
                SET enable_bao TO {'on' if bao_select or bao_reward else 'off'};
                SET bao_host = '195.251.63.231';
                SET bao_port = 9381;
                SET enable_bao_selection TO {'on' if bao_select else 'off'};
                SET enable_bao_rewards TO {'on' if bao_reward else 'off'};
                SET bao_num_arms TO 5;
                SET statement_timeout TO 180000; -- 3 Minutes
            """))
            
            for i in range(NUM_EXECUTIONS):
                result = conn.execute(
                    text(f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) {sql}")
                ).fetchone()
                
                if result:
                    plan = result[0]
                    bao_hint = plan[0]['Bao']['Bao recommended hint'] if use_bao else None
                    measurements.append({
                        'execution_time': plan[-1]['Execution Time'],
                        'planning_time': plan[-1]['Planning Time'],
                        'hint': bao_hint,
                        'execution_plan': plan
                    })
                    print(f"\t{i}: Execution Time: {measurements[-1]['execution_time']:.4f}\tPlanning Time: {measurements[-1]['planning_time']:.4f}")

    except Exception as e:
        print("An unexpected exception OR timeout occurred during database querying:", e)
        for _ in range(NUM_EXECUTIONS):
            measurements.append({
                'execution_time': 2 * TIMEOUT_LIMIT,
                'planning_time': 2 * TIMEOUT_LIMIT,
                'hint': None,
                'execution_plan': None
            })
    
    return measurements

from tqdm import tqdm

def fetch_queries(directory_path, query_order_file=None, skip_bao_processed=False):
    queries = []
    if query_order_file:
        # Read the query order from the specified file
        with open(query_order_file, 'r') as f:
            query_order = [line.strip() for line in f.readlines()]
        
        for filename in tqdm(query_order, desc="Processing ordered queries"):
            if not filename.endswith('.sql'):
                print(f"Warning: {filename} is not a .sql file. Skipping.")
                continue
            fileId = filename.split(".")[0]
            file_path = os.path.join(directory_path, fileId, filename)  # Moved here to fix reference
            
            if skip_bao_processed:
                query_dir = os.path.dirname(file_path)
                bao_dir = os.path.join(query_dir, "BAO")
                if os.path.exists(bao_dir):
                    print(f"Skipping {filename} as BAO directory already exists")
                    continue
            
            if os.path.isfile(file_path):
                with open(file_path, 'r') as sql_file:
                    sql_query = sql_file.read()
                yield (file_path, filename, sql_query)
                queries.append((file_path, filename, sql_query))
            else:
                print(f"Warning: {file_path} does not exist. Skipping.")
    else:
        print("No query order file provided. Executing all queries in the directory.")
        all_query_paths = []
        for root, dirs, files in os.walk(directory_path):
            pattern = os.path.join(root, "*.sql")
            all_query_paths.extend(sorted(glob.glob(pattern)))
        
        for query_path in tqdm(all_query_paths, desc="Processing unordered queries"):
            fileId = os.path.basename(query_path).split(".")[0]
            if skip_bao_processed:
                query_dir = os.path.dirname(query_path)
                bao_dir = os.path.join(query_dir, "BAO")
                if os.path.exists(bao_dir):
                    print(f"Skipping {os.path.basename(query_path)} as BAO directory already exists")
                    continue
            
            if os.path.isfile(query_path):
                with open(query_path, 'r') as sql_file:
                    sql_query = sql_file.read()
                yield (query_path, os.path.basename(query_path), sql_query)
                queries.append((query_path, os.path.basename(query_path), sql_query))
            else:
                print(f"Warning: {query_path} does not exist. Skipping.")
    
    return queries

def execute_queries_in_directory(directory_path, db_name, output_file_path, query_order_file=None, skip_bao_processed=False):
    queries = fetch_queries(directory_path, query_order_file, skip_bao_processed)
    # print(f"Found {len(list(queries))} queries to execute in {directory_path}")
    for filePath, filename, sql_query in tqdm(queries, desc="Executing queries", unit="query"):
        print(f"Executing query from file: {filename}")
        use_bao = True
        # Call run_query with the SQL from the file (can set bao_select or bao_reward flags as needed)
        measurements = run_query(sql_query, db_name, bao_select=True, bao_reward=True, use_bao=use_bao)

        count = 1        
        for measurement in measurements:
            if measurement['execution_plan'] is None:
                print(f"Warning: Execution plan for {filename} is None. Skipping saving plan.")
                continue
            # Print the best measurement
            output_string = f"{'x' if measurement['hint'] is None else measurement['hint']}, {current_timestamp_str()}, {filename}, {measurement['planning_time']}, {measurement['execution_time']}, {'Bao' if use_bao else 'PG'}"
            print(output_string)
            with open(output_file_path, 'a') as f:
                f.write(output_string)
                f.write(os.linesep)
            
            # From the filepath, get the directory path
            query_dir = os.path.dirname(filePath) 
            # Save the execution plan to a file in a bao directory where the query came from
            bao_dir = os.path.join(query_dir, "BAO")
            
            if NUM_EXECUTIONS > 1:
                run_dir = os.path.join(bao_dir, "run" + str(count))
                os.makedirs(run_dir, exist_ok=True)
                plan_file_path = os.path.join(run_dir ,filename.replace('.sql', '_bao_plan.json'))
            else:
                os.makedirs(bao_dir, exist_ok=True)
                plan_file_path = os.path.join(bao_dir, filename.replace('.sql', '_bao_plan.json'))

            os.makedirs(os.path.dirname(plan_file_path), exist_ok=True)
            with open(plan_file_path, 'w') as plan_file:
                json.dump(measurement['execution_plan'], plan_file, indent=4)
            
            count += 1

import os

import shutil
import subprocess
FINAL_MODEL_DIR = "/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/models/experiment2/random_split/BAO/random/final_model/20250520_141757_bao_default_model"

def kill_bao_server():
    """Terminate bao_server/main.py using pkill"""
    try:
        result = subprocess.run(
            ['pkill', '-f', 'bao_server/main.py'],
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE
        )
        
        # Delete the bao_default_model and bao.db from bao_server directory
        bao_default_model_path = os.path.join("bao_server", "bao_default_model")
        bao_db_path = os.path.join("bao_server", "bao.db")
        if os.path.exists(bao_default_model_path):
            shutil.rmtree(bao_default_model_path)
        if os.path.exists(bao_db_path):
            os.remove(bao_db_path)
        
        # Run the clean_experience.py script
        subprocess.run(
            ['python3', 'bao_server/clean_experience.py'],
            cwd="bao_server",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        if result.returncode == 0:
            print("Successfully terminated bao_server process")
            return True
        elif result.returncode == 1:
            print("No running bao_server processes found")
            return False
        else:
            print(f"Error terminating bao_server: {result.stderr.decode().strip()}")
            return False
    except Exception as e:
        print(f"Error running pkill: {str(e)}")
        return False

def fetch_final_model():
    """Start the bao server with the final trained model"""
    # First kill any existing server
    kill_bao_server()
    
    # Check if model directory exists
    if not os.path.exists(FINAL_MODEL_DIR):
        print(f"Error: Final model directory not found at {FINAL_MODEL_DIR}")
        return False
    
    try:
        # Create bao_server directory if it doesn't exist
        os.makedirs("bao_server/bao_default_model", exist_ok=True)
        
        # Copy all model files to the bao_server directory
        for item in os.listdir(FINAL_MODEL_DIR):
            print(f"Copying {item} to bao_server directory...")
            src_path = os.path.join(FINAL_MODEL_DIR, item)
            dest_path = os.path.join("bao_server/bao_default_model", item)
            
            if os.path.isdir(src_path):
                if os.path.exists(dest_path):
                    shutil.rmtree(dest_path)
                shutil.copytree(src_path, dest_path)
            else:
                shutil.copy2(src_path, dest_path)
                
        return True
            
    except Exception as e:
        print(f"Error starting BAO server: {str(e)}")
        return False

def load_model():
    """Instruct the BAO server to load the specified model"""
    model_path = os.path.join("/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/optimizers/BaoForPostgreSQL/bao_server", "bao_default_model")
    print(f"Loading model from {model_path}")
    cmd = f"cd bao_server && python3 baoctl.py --load {model_path}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Failed to load model: {result.stderr}")
        return False
    
    print("Model loaded successfully")
    return True

def start_bao_server():
    try:
        # Start the server in the bao_server directory
        server_process = subprocess.Popen(
            ["python3", "main.py", "--log-performance", "--log-file-path", "performance_log.txt"],
            cwd="bao_server",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Give it some time to start
        sleep(30)

        # Check if it's running
        if server_process.poll() is None:
            print("BAO server started successfully with final model")
            return True
        else:
            print("Failed to start BAO server")
            print("STDOUT:", server_process.stdout.read().decode())
            print("STDERR:", server_process.stderr.read().decode())
            return False        

    except Exception as e:
        print(f"Error starting BAO server: {str(e)}")
        return False


def main(workload_directory_path, db_name, output_file_path, query_order, skip_bao_processed):
    # Fetch the final model and load it
    fetch_final_model()
    # start_bao_server()
    engine = get_alchemy_engine(db_name)  # This will use the global engine variable

    # Explicitly load the final model
    if not load_model():
        print("Failed to load final model, exiting.")
        kill_bao_server()
        return
        
    # Execute queries and get the path to the actual latencies file
    execute_queries_in_directory(workload_directory_path, db_name, output_file_path, query_order, skip_bao_processed)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute SQL queries from files in a directory, record latencies, and calculate Q-error.")
    parser.add_argument("workload_directory", type=str, help="Directory path containing SQL files to execute")
    parser.add_argument("output_file", type=str, default="test_output.txt", help="Path to the output file")
    parser.add_argument("db_name", type=str, default="imdbload", help="Postgres Database name")
    parser.add_argument('--query_order_file', type=str, help='Text file specifying the order of query files to execute')
    parser.add_argument('--skip_bao_processed', action='store_true', help='Skip queries that already have a BAO directory')
    
    args = parser.parse_args()
    workload_directory_path = args.workload_directory
    db_name = args.db_name
    output_file_path = args.output_file
    query_order = args.query_order_file
    skip_bao_processed = args.skip_bao_processed
    
    # Ensure the provided directory exists
    if not os.path.isdir(workload_directory_path):
        print(f"Error: The directory {workload_directory_path} does not exist.")
    else:
        main(workload_directory_path, db_name, output_file_path, query_order, skip_bao_processed)