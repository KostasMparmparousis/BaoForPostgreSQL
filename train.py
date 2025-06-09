import psycopg2
import os
import sys
import random
from time import time, sleep
import shutil
from datetime import datetime
import subprocess
import argparse
import glob

USE_BAO = True

def kill_bao_server():
    """Terminate bao_server/main.py using pkill"""
    try:
        result = subprocess.run(
            ['pkill', '-f', 'bao_server/main.py'],
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE
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
    
def save_final_model_and_history(db_name):
    """Save the final trained model and all history, then clean original repo"""
    # Configuration
    model_dir = f"/data/hdd1/users/kmparmp/models/bao/{db_name}/final_model"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(model_dir, exist_ok=True)

    print("Terminating bao_server processes...")
    server_was_running = kill_bao_server()

    # Files/directories to transfer
    transfer_items = [
        "bao_server/bao_default_model",
        "bao_server/bao_default_model.metadata.json",
        "training_time.txt",
        "bao_server/model_archive"
    ]

    # 1. Transfer items to central location
    transferred = []
    for src_path in transfer_items:
        if os.path.exists(src_path):
            if src_path == "bao_server/model_archive":
                # Special handling for archive directory
                archive_dest = os.path.join(model_dir, "archive")
                os.makedirs(archive_dest, exist_ok=True)
                
                for item in os.listdir(src_path):
                    item_src = os.path.join(src_path, item)
                    item_dest = os.path.join(archive_dest, item)
                    
                    if os.path.isdir(item_src):
                        shutil.copytree(item_src, item_dest)
                    else:
                        shutil.copy2(item_src, item_dest)
                transferred.append(src_path)
                print(f"Transferred model archive to {archive_dest}")
            else:
                # Handle regular files/directories
                dest_name = f"{timestamp}_{os.path.basename(src_path)}" if "archive" not in src_path else os.path.basename(src_path)
                dest_path = os.path.join(model_dir, dest_name)
                
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, dest_path)
                else:
                    shutil.copy2(src_path, dest_path)
                transferred.append(src_path)
                print(f"Saved {os.path.basename(src_path)} to {dest_path}")
        else:
            print(f"Warning: Source not found - {src_path}")

    # 2. Clean up original repository
    for src_path in transferred:
        try:
            if os.path.isdir(src_path):
                shutil.rmtree(src_path)
            else:
                os.remove(src_path)
            print(f"Cleaned: Removed {src_path} from original location")
        except Exception as e:
            print(f"Warning: Could not remove {src_path} - {str(e)}")

    # 3. Additional cleanup operations
    try:
        # Additional cleanups
        db_paths = ["bao_server/bao.db", "bao_server/bao_previous_model.metadata.json"]
        for db_path in db_paths:
            if os.path.exists(db_path):
                os.remove(db_path)
        
        # Run experience cleaner
        print("Running experience cleaner...")
        os.system("")
        os.system("python3 bao_server/clean_experience.py")
        print("Experience cleaning completed")

    except Exception as e:
        print(f"Error during additional cleanup: {str(e)}")
        
    # 4. Create summary README
    readme_content = f"""BAO Model Training Summary
===========================
Timestamp: {timestamp}
Storage Location: {model_dir}

Contents:
- Current Model: {timestamp}_bao_default_model
- Model Metadata: {timestamp}_bao_default_model.metadata.json
- Execution Time: {timestamp}_training_time.txt
- Archived Models: archive/ directory

Original repository has been cleaned.
"""
    readme_path = os.path.join(model_dir, f"{timestamp}_README.md")
    with open(readme_path, "w") as f:
        f.write(readme_content)

def save_model_checkpoint(c_idx, db_name):
    """Save Bao model checkpoint after retraining iteration"""
    checkpoint_dir = f"/data/hdd1/users/kmparmp/models/bao/{db_name}/checkpoints"
    os.makedirs(checkpoint_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Define files to copy
    checkpoint_files = [
        "bao_server/bao_default_model",
        "bao_server/bao_default_model.metadata.json"
    ]
    
    for file_path in checkpoint_files:
        if os.path.exists(file_path):
            dest_path = os.path.join(checkpoint_dir, f"{timestamp}_chunk{c_idx}_{os.path.basename(file_path)}")
            if os.path.isdir(file_path):
                shutil.copytree(file_path, dest_path)
            else:
                shutil.copy2(file_path, dest_path)
            print(f"Checkpoint saved: {dest_path}")
        else:
            print(f"Warning: {file_path} not found during checkpointing.")

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

USE_BAO = True
TIMEOUT_LIMIT = 3 * 60 * 1000
NUM_EXECUTIONS = 3

PG_CONNECTION_STR = "dbname=imdbload user=suite_user host=train.darelab.athenarc.gr port=5468 password=71Vgfi4mUNPm"

def pg_connection_string(db_name):
    return f"dbname={db_name} user=suite_user host=train.darelab.athenarc.gr port=5468 password=71Vgfi4mUNPm"

def run_query(sql, bao_select=False, bao_reward=False, db_name='imdbload'):
    while True:
        try:
            conn = psycopg2.connect(pg_connection_string(db_name=db_name))
            cur = conn.cursor()

            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET bao_host = '195.251.63.231'")
            cur.execute(f"SET bao_port = 9381")
            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET enable_bao_selection TO {bao_select}")
            cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
            cur.execute("SET bao_num_arms TO 5")
            cur.execute(f"SET statement_timeout TO {TIMEOUT_LIMIT}")

            # As visible in the #should_report_reward method of the pg_extension
            # found in pg_extension/bao_util.h, EXPLAIN (and ANALYZE) queries are not
            # put into the experience buffer and need to be run without EXPLAIN to
            # ensure that they are used to train Bao
            #
            if bao_reward:
                cur.execute(sql)
                cur.fetchall()

            # Execute once more to extract planning (+= Bao inference) and execution times
            cur.execute(f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) {sql}")        
            result = cur.fetchall()[0][0][-1]
            
            measurement = {
                'execution_time': result['Execution Time'],
                'planning_time': result['Planning Time']
            }

            conn.close()
            break

        except Exception as e:
            print("An unexpected exception OR timeout occured during database querying:", e)
            conn.close()

            return {
                'execution_time': 2 * TIMEOUT_LIMIT,
                'planning_time': 2 * TIMEOUT_LIMIT
            }
            
    return measurement

def current_timestamp_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def write_to_file(file_path, output_string):
    print(output_string)
    with open(file_path, 'a') as f:
        f.write(output_string)
        f.write(os.linesep)

def main(args):
    # Look for .sql files
    pattern = os.path.join(args.query_dir, '**/*.sql')
    query_paths = sorted(glob.glob(pattern, recursive=True))
    print(f"Found {len(query_paths)} queries in {args.query_dir} and its subdirectories.")

    queries = []
    for fp in query_paths:
        with open(fp) as f:
            query = f.read()
        queries.append((fp, query))
    print("Using Bao:", USE_BAO)

    db_name = args.database_name
    print("Running against DB:", db_name)

    random.seed(42)

    queries_to_run = 500 if len(queries) < 500 else len(queries)
    query_sequence = random.choices(queries, k=queries_to_run)
    pg_chunks, *bao_chunks = list(chunks(query_sequence, 25))

    print("Executing queries using PG optimizer for initial training")

    if os.path.exists(args.output_file):
        raise FileExistsError(f"The file {args.output_file} already exists, stopping.")

    for q_idx, (fp, q) in enumerate(pg_chunks):
        # Warm up the cache
        for iteration in range(NUM_EXECUTIONS - 1):
            measurement = run_query(q, db_name=db_name)
            output_string = f"x, {q_idx}, {iteration}, {current_timestamp_str()}, {fp}, {measurement['planning_time']}, {measurement['execution_time']}, PG"
            write_to_file(args.output_file, output_string)
        
        measurement = run_query(q, bao_reward=True, db_name=db_name)
        output_string = f"x, {q_idx}, {NUM_EXECUTIONS-1}, {current_timestamp_str()}, {fp}, {measurement['planning_time']}, {measurement['execution_time']}, PG"
        write_to_file(args.output_file, output_string)

    for c_idx, chunk in enumerate(bao_chunks):
        print("==="*30, flush=True)
        print(f"Iteration over chunk {c_idx + 1}/{len(bao_chunks)}...")
        if USE_BAO:
            print(f"[{current_timestamp_str()}]\t[{c_idx + 1}/{len(bao_chunks)}]\tRetraining Bao...", flush=True)
            os.system("cd bao_server && python3 baoctl.py --retrain")
            os.system("sync")
            save_model_checkpoint(c_idx + 1, db_name)
            print(f"[{current_timestamp_str()}]\t[{c_idx + 1}/{len(bao_chunks)}]\tRetraining done.", flush=True)

        for q_idx, (fp, q) in enumerate(chunk):
            # Warm up the cache
            for iteration in range(NUM_EXECUTIONS - 1):
                measurement = run_query(q, bao_reward=False, bao_select=USE_BAO, db_name=db_name)
                output_string = f"{c_idx}, {q_idx}, {iteration}, {current_timestamp_str()}, {fp}, {measurement['planning_time']}, {measurement['execution_time']}, Bao"
                write_to_file(args.output_file, output_string)

            measurement = run_query(q, bao_reward=USE_BAO, bao_select=USE_BAO, db_name=db_name)
            output_string = f"{c_idx}, {q_idx}, {NUM_EXECUTIONS-1}, {current_timestamp_str()}, {fp}, {measurement['planning_time']}, {measurement['execution_time']}, Bao"
            write_to_file(args.output_file, output_string)

    print("Saving final model and cleaning repository...")
    save_final_model_and_history(db_name)

# Example Call:
#
# python3 run_queries.py --query_dir queries/job__base_query_split_1/train --output_file train__bao__base_query_split_1.txt
#
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_name', type=str, default='imdbload', help='Database name to query against')
    parser.add_argument('--query_dir', type=str, required=True, help='Directory which contains all the *training* queries')
    parser.add_argument('--output_file', type=str, required=True, help='File in which to store the results')

    args = parser.parse_args()
    main(args)