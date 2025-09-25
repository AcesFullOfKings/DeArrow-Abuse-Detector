import requests
import json
import csv
import sqlite3
import os
from time import time

def ask_ai(title):
    start_time = time()
    headers = {"Content-Type": "application/json"}

    with open("prompt.txt", "r") as f:
        prompt = f.read()

    content = {
    "model": "gemma3:27b",
    "prompt": prompt.format(title=title),
    "stream": False,
    "format": {
        "type": "object",
        "properties": {
            "score": {
                "type": "integer"
            },
            "explanation": {
                "type": "string"
            }
        },
        "required":[
            "score",
            "explanation"
        ]
    }
    }

    content = json.dumps(content)

    result = requests.post("http://localhost:11434/api/generate", data=content, headers=headers, timeout=60)

    response = result.json()["response"]
    response = json.loads(response)
    
    print(f"Processed | Score: {response['score']} | Time: {round(time()-start_time, 2)}s | {title}")
    
    return response["score"], response["explanation"]

def check_title_status(uuid, cursor):
    """Check if title is locked, shadowHidden, or removed in the database"""
    cursor.execute("SELECT locked, shadowHidden, removed FROM titleVotes WHERE UUID = ?", (uuid,))
    result = cursor.fetchone()
    
    if result is None:
        # UUID not found in database. shouldn't really happen but assume it's ok to ask
        return True
    
    locked, shadow_hidden, removed = result
    
    # Only ask if all three are 0 (false)
    return (locked == 0) and (shadow_hidden == 0) and (removed == 0)

field_names = ['videoID', 'title', 'original', 'userID', 'service', 'hashedVideoID', 'timeSubmitted', 'UUID', 'casualMode', 'userAgent']

conn = sqlite3.connect("titleVotes.sqlite3")
cursor = conn.cursor()

titles_file = open("titles.csv")
titles_reader = csv.DictReader(titles_file, fieldnames=field_names) 

titles_reader.__next__() # discard first row (title row)

progress_skipped_count = 0 

# Handle resuming from previous position
start_uuid = None
if os.path.exists("progress.txt"):
    with open("progress.txt", "r") as f:
        start_uuid = f.read().strip()
    print(f"Finding previous position from UUID: {start_uuid}")
    
    # Skip to the position after the last processed UUID
    found = False
    for row in titles_reader:
        if row["UUID"] == start_uuid:
            found = True
            break
        else:
            progress_skipped_count += 1
    
    if found:
        print(f"Previous position located after {progress_skipped_count} rows. Continuing from next item...")
    else:
        print("Warning: Previous position not found. Starting from beginning...")
        titles_file.close()
        titles_file = open("titles.csv")
        titles_reader = csv.DictReader(titles_file, fieldnames=field_names)
        titles_reader.__next__() # discard header again
else:
    print("Starting fresh (no progress file found)")

# Determine if we're resuming or starting fresh
resuming = os.path.exists("title_scores.csv") and os.path.getsize("title_scores.csv") > 0

# Open output CSV file for writing (append if resuming)
file_mode = "a" if resuming else "w"
with open("title_scores.csv", file_mode, newline='', encoding='utf-8') as output_file:
    output_writer = csv.writer(output_file)
    
    # Write header row only if starting fresh
    if not resuming:
        output_writer.writerow(["UUID", "title", "score", "explanation"])
    
    processed_count = 0
    skipped_count = 0
    
    for row in titles_reader:
        if row["original"] in [0, "0"]:
            uuid = row["UUID"]
            title = row["title"]

            if check_title_status(uuid, cursor): # lookup uuid and only ask ai if it's not locked/removed
                try:
                    score, explanation = ask_ai(title)
                    output_writer.writerow([uuid, title, score, explanation])
                    output_file.flush()
                    processed_count += 1
                    
                    with open("progress.txt", "w") as f: # note progress
                        f.write(uuid)
                        
                except Exception as ex:
                    if "timeout" in str(ex).lower() or "timed out" in str(ex).lower():
                        print(f"Request timed out after 60 seconds for title: {title}")
                    else:
                        print(f"Exception on title: {title} // {ex}")
                    
                    with open("progress.txt", "w") as f: # Still note progress
                        f.write(uuid)
            else:
                print(f"Skipped: {uuid} {title}")
                skipped_count += 1
                
                # Update progress for skipped items too
                with open("progress.txt", "w") as f:
                    f.write(uuid)

titles_file.close()
conn.close()

print(f"\nSummary: Processed {processed_count} titles, skipped {skipped_count} titles")