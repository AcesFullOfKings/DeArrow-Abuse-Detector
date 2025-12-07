import os
import csv
import json
import sqlite3
import requests
import threading
import subprocess

from time import time, sleep

output_filepath = "title_scores.csv"
progress_filepath = "progress.txt"

def ask_ai(title, uuid):
    start_time = time()
    headers = {"Content-Type": "application/json"}

    with open("prompt.txt", "r") as f:
        prompt = f.read()

    content = {
		"model": "gemma3:27b",
		"prompt": prompt.format(title=title),
		"stream": False,
		"options": {
			"num_ctx": 3000,
			"temperature": 0
		},
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
    
    print(f"Done | {uuid:<36} | {response['score']:<1} | {f"{time()-start_time:<5.2f}s"} | {title}")
    
    return response["score"], response["explanation"]

def check_title_status(uuid, cursor):
    """Check if title is locked, shadowHidden, or removed in the database"""
    cursor.execute("SELECT locked, shadowHidden, removed, votes, downvotes FROM titleVotes WHERE UUID = ?", (uuid,))
    result = cursor.fetchone()
    
    if result is None:
        # UUID not found in database. shouldn't really happen but assume it's ok to ask
        return False
    
    locked, shadow_hidden, removed, votes, downvotes = result
    score = int(votes) - int(downvotes) # maybe also need to factor in the "verification" column?
    
    # Only ask the AI if it isn't already removed, hidden, or downvoted
    if (locked == "0") and (shadow_hidden == "0") and (removed == "0") and (score > -2):
        return False
    else:
        return [int(locked), int(shadow_hidden), int(removed), score]
    
def is_on_battery():
	try:
		output = subprocess.check_output(["pmset", "-g", "ps"], text=True)
		return "Battery Power" in output
	except Exception:
		return False

def power_monitor(stop_event):
	battery_start_time = None
	while not stop_event.is_set():
		sleep(10)
		if is_on_battery():
			os.system("afplay -v 25 -t 0.5 /System/Library/Sounds/Ping.aiff")
			if battery_start_time is None:
				battery_start_time = time()
			else:
				if time() - battery_start_time > 600:
					os._exit(1)
		else:
			battery_start_time = None
               
stop_event = threading.Event()
power_thread = threading.Thread(target=power_monitor, args=(stop_event,), daemon=True)
power_thread.start()

conn = sqlite3.connect("titleVotes.sqlite3")
cursor = conn.cursor()

field_names = ['videoID', 'title', 'original', 'userID', 'service', 'hashedVideoID', 'timeSubmitted', 'UUID', 'casualMode', 'userAgent']

titles_file = open("titles.csv")
titles_reader = csv.DictReader(titles_file, fieldnames=field_names) 

titles_reader.__next__() # discard first row (title row)

progress_count = 0
total_titles = 513432 # maybe count the rows in the db instead

# Handle resuming from previous position
start_uuid = None
if os.path.exists(progress_filepath):
    with open(progress_filepath, "r") as f:
        start_uuid = f.read().strip()
    print(f"Finding previous position from UUID: {start_uuid}")
    
    # Skip to the position after the last processed UUID
    found = False
    for row in titles_reader:
        if row["UUID"] == start_uuid:
            found = True
            break
        else:
            progress_count += 1
    
    if found:
        print(f"Previous position located after {progress_count} rows. Continuing from next item...")
    else:
        print("Warning: Previous position not found. Starting from beginning...")
        titles_file.close()
        titles_file = open("titles.csv")
        titles_reader = csv.DictReader(titles_file, fieldnames=field_names)
        titles_reader.__next__() # discard header again
else:
    print("Starting fresh (no progress file found)")
    
# boot up the model (might take a while)
result = requests.post("http://localhost:11434/api/generate", data=json.dumps({
    "model": "gemma3:27b"})) # sending a blank request just boots up the model, per the documentation

# Determine if we're resuming from previous position or starting fresh
resuming = os.path.exists(output_filepath) and os.path.getsize(output_filepath) > 0

try:
	file_mode = "a" if resuming else "w"
	with open(output_filepath, file_mode, newline='', encoding='utf-8') as output_file:
		output_writer = csv.writer(output_file)
		
		# Write header row only if starting fresh
		if not resuming:
			output_writer.writerow(["UUID", "Title", "Score", "Explanation"])
		
		for row in titles_reader:
			uuid = row["UUID"]
			title = row["title"]
			if row["original"] == "0":
				if not (status := check_title_status(uuid, cursor)): # lookup uuid and only ask ai if it's not locked/removed
					try:
						sanitised_title = title.replace(">", "")
						score, explanation = ask_ai(sanitised_title, uuid)
						if score >= 1:
							output_writer.writerow([uuid, title, score, explanation])
							output_file.flush()
							
					except Exception as ex:
						if "timeout" in str(ex).lower() or "timed out" in str(ex).lower():
							print(f"Request timed out after 60 seconds for title: {title}")
						else:
							print(f"Exception on title: {title} // {ex}")
							sleep(1)
				else:
					reason = ""
					if status[0]:
						reason = "locked"
					elif status[1]:
						reason = "S-hidden" #"shadowhidden" doesn't fit into the 10-chr budget and I'm not willing to make the column wider LOL
					elif status[2]:
						reason = "removed"
					elif status[3] != 0:
						reason = "downvoted"
					else:
						reason = "??????" # shouldn't happen
					
					print(f"Skip | {uuid:<36} | {reason:<10} | {title}")
					
				# Update progress either way
				with open(progress_filepath, "w") as f:
					f.write(uuid)
			else:
				reason = "original"
				print(f"Skip | {uuid:<36} | {reason:<10} | {title}")

	progress_count += 1

except KeyboardInterrupt:
	print(f"\nProgress: {progress_count//1000}k/{total_titles//1000}k ({round(progress_count/total_titles,3)*100}%)")
	# explicitly handling the error here should stop it from printing the stack trace?
finally:
	stop_event.set()
	
power_thread.join()

titles_file.close()
conn.close()

print("Done!")