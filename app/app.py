import os
import argparse
import json
from cassandra.cluster import Cluster
import subprocess

def create_schema(session):
    """Create keyspace and tables in Cassandra"""
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS index_keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)

    session.set_keyspace('index_keyspace')

    # Table for vocabulary 
    session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            term TEXT PRIMARY KEY,
            doc_freq INT
        )
    """)

    # Table for inverted index 
    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term TEXT,
            doc_id TEXT,
            term_freq INT,
            positions LIST<INT>,
            PRIMARY KEY (term, doc_id)
        )
    """)

    # Table for document statistics
    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id TEXT PRIMARY KEY,
            title TEXT,        
            total_terms INT,
            unique_terms INT
        )
    """)
    
    print("Cassandra table created without errors.")

def process_hdfs_file(session, path):
    """Process an HDFS file and insert data into Cassandra"""
    insert_vocab = session.prepare(
        "INSERT INTO vocabulary (term, doc_freq) VALUES (?, ?)")
    
    insert_index = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, term_freq, positions) VALUES (?, ?, ?, ?)")
    
    insert_doc_stats = session.prepare(
        "INSERT INTO doc_stats (doc_id, title, total_terms, unique_terms) VALUES (?, ?, ?, ?)")
    
    cmd = ["hdfs", "dfs", "-cat", f"{path}/part-*"]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    line_count,index_count,stats_count = 0,0,0
    
    for line in process.stdout:
        line = line.strip()
        if not line: continue
            
        parts = line.split('\t', 1)
        if len(parts) != 2: continue
            
        record_type, data = parts
        line_count += 1
        
        if record_type == "INDEX":
            try:
                index_data = json.loads(data)
                term = index_data['term']
                doc_freq = index_data['doc_frequency']
                session.execute(insert_vocab, (term, doc_freq))
                for doc in index_data['docs']:
                    doc_id = doc['doc_id']
                    term_freq = doc['term_frequency']
                    positions = [int(pos) for pos in doc['positions'].split(',')]
                    session.execute(insert_index, (term, doc_id, term_freq, positions))
                index_count += 1
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error processing index: {e}")
                
        elif record_type == "STATS":
            try:
                doc_data = json.loads(data)
                doc_id = doc_data['doc_id']
                title = doc_data.get('title', '')
                total_terms = doc_data['total_terms']
                unique_terms = doc_data['unique_terms']
                session.execute(insert_doc_stats, (doc_id, title, total_terms, unique_terms))
                stats_count += 1
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error processing stats: {e}")
    
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        print(f"Error reading hdfs: {stderr}")
        return False
    print(f"Processed {line_count} lines: {index_count} ind entries and {stats_count} doc stats")
    return True

def main():
    parser = argparse.ArgumentParser(description="Store index data in Cassandra")
    parser.add_argument("--index_path", required=True, help="HDFS path to the MapReduce output")
    args = parser.parse_args()
    print("Connecting to cassandra")
    cluster = Cluster(['cassandra-server'], port=9042)
    session = cluster.connect()
    try:
        create_schema(session)
        print(f"Process data from hdfs path: {args.index_path}")
        success = process_hdfs_file(session, args.index_path)
        if success:
            print("Successfull stored index data in cassandra")
            return 0
        else:
            print("Couldn't  store index data in cassandra")
            return 1
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)