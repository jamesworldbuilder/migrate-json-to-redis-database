# Script to migrate a database collection in JSON file format to a Redis database
import redis
import json

# --- CONFIGURATION ---
# External Value Key URL
REDIS_URL = "rediss://XXXXXXX@location-keyvalue.render.com:port" # Example: Render.com value
# The clean JSON file we created
JSON_FILE_PATH = "./tetris-leaderboard.json"
# The Redis Key Value Name
LEADERBOARD_KEY = "tetris-leaderboard"

def migrate_scores():
    """
        - Reads a JSON file,
        - Migrates scores to Redis,
        - Verifies the write action
    """
    print("Connecting to Redis...")
    try:
        # Establish a connection to the Redis server
        r = redis.from_url(REDIS_URL, decode_responses=True)
        # PING to confirm the connection
        r.ping()
        print("✅ Connection to Redis successful")
    except redis.exceptions.ConnectionError as e:
        print(f"❌ Could not connect to Redis: {e}")
        return

    print(f"Reading scores from {JSON_FILE_PATH}...")
    try:
        # Read JSON file
        with open(JSON_FILE_PATH, 'r') as f:
            # Parse JSON file 
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ JSON file not found at: {JSON_FILE_PATH}")
        return
    except json.JSONDecodeError:
        print(f"❌ Could not read the JSON file")
        return

    # Navigate the nested structure of the JSON file to find the score data
    # The first `.get()` looks for the `__collections__` key
    # The second `.get()` looks for the specific leaderboard key inside `__collections__`
    scores_data = data.get("__collections__", {}).get(LEADERBOARD_KEY, {})

    # Check if the score data was successfully extracted
    if not scores_data:
        print(f"No data found for the '{LEADERBOARD_KEY}' collection in the JSON file")
        return

    # Initializes an empty dictionary to hold the scores to be added to Redis
    scores_to_add = {}
    # Loop through each item in the `scores_data` dictionary
    # `doc_id` is the unique key for each record and `record` is the score data
    for doc_id, record in scores_data.items():
        # Skip metadata entries that are not actual score records
        if doc_id == "__collections__":
            continue

        # Extract the player's initials and score from the record
        player_initials = record.get("name")
        player_score = record.get("score")

        # Ensure that both the initials and score exist before adding them
        if player_initials and player_score is not None:
             scores_to_add[player_initials] = player_score

    # Check if any valid scores were found after processing the file
    if not scores_to_add:
        print("No valid scores found to migrate")
        return

    print(f"Found {len(scores_to_add)} score(s) to migrate - Attempting to write to Redis...")
    try:
        # Add all the collected scores to the Redis sorted set
        r.zadd(LEADERBOARD_KEY, scores_to_add)
        print("✅ Write command sent successfully")
    except Exception as e:
        print(f"❌ An error occurred during the write command: {e}")
        return

    print("\n--- Verification Step ---")
    try:
        # Get the total number of items (cardinality) in the sorted set
        count = r.zcard(LEADERBOARD_KEY)
        # Check if the number of items in the database is greater than zero
        if count > 0:
            print(f"✅ SUCCESS! Found {count} item(s) in the Redis database")
            # Get a range of members - `0, -1` means to get all members
            # Includes the scores and sorts from highest to lowest
            top_scores = r.zrange(LEADERBOARD_KEY, 0, -1, withscores=True, desc=True)
            print("Leaderboard now contains:")
            # Loop through the retrieved scores and prints each one
            for player, score in top_scores:
                print(f"  - Player: {player}, Score: {score}")
        else:
            print("❌ VERIFICATION FAILED. The write operation did not save the data to Redis")
    except Exception as e:
        print(f"❌ An error occurred during verification: {e}")

if __name__ == "__main__":
    migrate_scores()
