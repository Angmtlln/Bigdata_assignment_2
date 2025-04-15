#!/usr/bin/env python3
import sys
import json

term_docs,doc_stats = {},{}  

for line in sys.stdin:
    try:
        line,parts = line.strip(),line.split('\t')
        if len(parts) < 3:
            sys.stderr.write(f"Skip invalid reducer 2 input: {line}\n")
            continue
        record_type = parts[0]
        if record_type == "IDX" and len(parts) >= 5:
            term, doc_id, freq, positions = parts[1], parts[2], parts[3], parts[4]
            try:
                freq = int(freq)
            except ValueError:
                sys.stderr.write(f"Invalid freq. value: {freq}\n")
                continue
            if term not in term_docs:
                term_docs[term] = []
            doc_exists = False
            for doc in term_docs[term]:
                if doc['doc_id'] == doc_id:
                    doc_exists = True
                    doc['term_frequency'] += freq
                    try:
                        existing_positions = set(int(p) for p in doc['positions'].split(',') if p)
                        new_positions = set(int(p) for p in positions.split(',') if p)
                        all_positions = sorted(existing_positions.union(new_positions))
                        doc['positions'] = ','.join(str(p) for p in all_positions)
                    except:
                        pass
                    break
                    
            if not doc_exists:
                term_docs[term].append({'doc_id': doc_id,'term_frequency': freq,'positions': positions})
            
        elif record_type == "DOC" and len(parts) >= 4:
            doc_id, term, freq = parts[1], parts[2], parts[3]
            doc_title = parts[4] if len(parts) >= 5 else ""
            try:
                freq = int(freq)
            except ValueError:
                sys.stderr.write(f"Invalid freq. value: {freq}\n")
                continue
            if doc_id not in doc_stats:
                doc_stats[doc_id] = {'total_terms': 0,'unique_terms': 0,'seen_terms': set(),  'title': doc_title}
            if doc_title and not doc_stats[doc_id].get('title'):doc_stats[doc_id]['title'] = doc_title
            doc_stats[doc_id]['total_terms'] += freq
            if term not in doc_stats[doc_id]['seen_terms']:
                doc_stats[doc_id]['unique_terms'] += 1
                doc_stats[doc_id]['seen_terms'].add(term)
        
        elif record_type == "TITLE" and len(parts) >= 3:
            doc_id, title = parts[1], parts[2]
            if doc_id not in doc_stats: doc_stats[doc_id] = {'total_terms': 0,'unique_terms': 0,'seen_terms': set(),'title': title}
            else: doc_stats[doc_id]['title'] = title
            
    except Exception as e:
        sys.stderr.write(f"Error in reducer 2: {str(e)}\n")
        continue

for term, docs in term_docs.items():
    try:
        term_data = {'term': term,'doc_frequency': len(docs),  'docs': docs}
        print(f"INDEX\t{json.dumps(term_data)}")
    except Exception as e:
        sys.stderr.write(f"Error in output term {term}: {str(e)}\n")

for doc_id, stats in doc_stats.items():
    try:
        seen_terms = stats.pop('seen_terms', None)
        doc_data = {'doc_id': doc_id,'title': stats.get('title', ''),'total_terms': stats['total_terms'],'unique_terms': stats['unique_terms']}
        print(f"STATS\t{json.dumps(doc_data)}")
    except Exception as e:
        sys.stderr.write(f"Error in output doc {doc_id}: {str(e)}\n")