#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        line,parts = line.strip(),line.split('\t')
        if len(parts) != 5:
            sys.stderr.write(f"Skip invalid maper 2 input: {line}\n")
            continue
        term, doc_id, freq, positions, doc_title = parts
        try:
            freq = int(freq)
        except ValueError:
            sys.stderr.write(f"Invalid freq. value: {freq}\n")
            continue
        print(f"IDX\t{term}\t{doc_id}\t{freq}\t{positions}\nDOC\t{doc_id}\t{term}\t{freq}\t{doc_title}")        
    except Exception as e:
        sys.stderr.write(f"Error in mapper 2: {str(e)}\n")
        continue