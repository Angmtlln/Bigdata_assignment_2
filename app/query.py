import sys
import re
import math
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

K1,B = 1.2, 0.75

def clean_text(text):
    text = re.sub(r'[^\w\s]', '', text.lower())
    return text.split()

def main():
    spark = SparkSession.builder \
        .appName("BM25 Document Ranker") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .getOrCreate()
    
    sc = spark.sparkContext
    if len(sys.argv) > 1:
        query = ' '.join(sys.argv[1:])  
        print(f"Original query: {query}")
    else:
        print("Error: No query")
        return
    query_terms = clean_text(query)
    if not query_terms:
        print("No valid terms in query.")
        return
    cluster = Cluster(['cassandra-server'], port=9042)
    session = cluster.connect('index_keyspace')
    try:
        rows = session.execute("SELECT COUNT(*) FROM doc_stats")
        total_docs = rows.one()[0]
        if total_docs == 0:
            print("No documents in index.")
            return
        rows = session.execute("SELECT SUM(total_terms) FROM doc_stats")
        total_terms_all_docs = rows.one()[0] or 0
        avg_doc_len = total_terms_all_docs / total_docs
        corpus_stats = {'total_docs': total_docs, 'avg_doc_len': avg_doc_len}
        bc_corpus_stats = sc.broadcast(corpus_stats)
        term_df_dict = {}
        prep_vocab = session.prepare("SELECT doc_freq FROM vocabulary WHERE term = ?")
        for term in query_terms:
            rows = session.execute(prep_vocab, [term])
            row = rows.one()
            if row: term_df_dict[term] = row.doc_freq
        if not term_df_dict:
            print("None query terms found in index.")
            return
        bc_term_df = sc.broadcast(term_df_dict)   
        all_docs_data = []
        prep_index = session.prepare("SELECT doc_id, term_freq FROM inverted_index WHERE term = ?")
        for term in term_df_dict.keys():
            rows = session.execute(prep_index, [term])
            for row in rows: all_docs_data.append((row.doc_id, term, row.term_freq))
        
        docs_terms_rdd = sc.parallelize(all_docs_data)
        doc_terms_grouped = docs_terms_rdd.map(lambda x: (x[0], (x[1], x[2]))) \
                                         .groupByKey() \
                                         .mapValues(list)
        
        doc_ids = docs_terms_rdd.map(lambda x: x[0]).distinct().collect()
        doc_lengths = {}
        prep_doc_stats = session.prepare("SELECT total_terms FROM doc_stats WHERE doc_id = ?")
        for doc_id in doc_ids:
            rows = session.execute(prep_doc_stats, [doc_id])
            row = rows.one()
            if row:doc_lengths[doc_id] = row.total_terms
        bc_doc_lengths = sc.broadcast(doc_lengths)
        def calculate_bm25_score(doc_data):
            doc_id, term_tf_list = doc_data
            corpus_stats = bc_corpus_stats.value
            term_df_dict = bc_term_df.value
            doc_len = bc_doc_lengths.value.get(doc_id, corpus_stats['avg_doc_len'])
            score = 0
            for term, tf in term_tf_list:
                df = term_df_dict.get(term, 0)
                if df > 0:
                    idf = math.log((corpus_stats['total_docs'] - df + 0.5) / (df + 0.5) + 1.0)
                    tf_component = ((K1 + 1) * tf) / (K1 * ((1 - B) + B * (doc_len / corpus_stats['avg_doc_len'])) + tf)
                    score += idf * tf_component
            return (doc_id, score)
        
        doc_scores = doc_terms_grouped.map(calculate_bm25_score)
        top_docs = doc_scores.top(10, key=lambda x: x[1])
        if not top_docs:
            print("No matching documents.")
            return
        prep_titles = session.prepare("SELECT title FROM doc_stats WHERE doc_id = ?")


        #Printing template for queries
        print(f"\nTop 10 documents for query: {query}")
        print("------------------------------")
        for doc_id, score in top_docs:
            rows = session.execute(prep_titles, [doc_id])
            row = rows.one()
            title = row.title if row and row.title else "No title"
            print(f"Document ID: {doc_id}\nTitle: {title}\nScore: {score:.4f}")
            print("------------------------------")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
    finally:
        cluster.shutdown()
        spark.stop()

if __name__ == "__main__":
    main()