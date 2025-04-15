#!/usr/bin/env python3
import sys

current_title = ""
current_term = None
current_doc = None
positions = []

for line in sys.stdin:
    try:
        line,parts = line.strip(),line.split('\t')
        if len(parts) != 4:
            sys.stderr.write(f"Skipping invalid reducer1 input: {line}\n")
            continue
        term, doc_id, position, title = parts
        try:
            position = int(position)
        except ValueError:
            sys.stderr.write(f"Invalid position value: {position}\n")
            continue
        if current_term != term or current_doc != doc_id:
            if current_term and current_doc:
                positions_str = ','.join(map(str, positions))
                print(f"{current_term}\t{current_doc}\t{len(positions)}\t{positions_str}\t{current_title}")
            current_term,current_doc,current_title = term,doc_id,title
            positions = [position]
        else:
            positions.append(position)
    except Exception as e:
        sys.stderr.write(f"Error in reducer1: {str(e)}\n")
        continue

if current_term and current_doc:
    positions_str = ','.join(map(str, positions))
    print(f"{current_term}\t{current_doc}\t{len(positions)}\t{positions_str}\t{current_title}")