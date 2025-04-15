#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    try:
        if line.strip().startswith('000'):
            parts = line.strip().split()
            if len(parts) >= 2:
                doc_id = parts[1]
                content_text = ' '.join(parts[2:]) if len(parts) > 2 else ""
                content = content_text
            else: continue
        else:
            parts = line.strip().split('\t')
            if len(parts) >= 3: doc_id, title, content = parts[0], parts[1], parts[2]
            elif len(parts) == 2:
                doc_id, content = parts[0], parts[1]
                title = ""
            else:
                sys.stderr.write(f"Skipping malformed line: {line[:50]}...\n")
                continue
        tokens,position = re.sub(r'[^\w\s]', ' ', content.lower()).split(), 0
        for token in tokens:
            print(f"{token}\t{doc_id}\t{position}\t{title}")
            position += 1
    except Exception as e:
        sys.stderr.write(f"Error processing line: {str(e)}\n")
        continue