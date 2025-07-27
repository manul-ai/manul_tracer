#!/usr/bin/env python3
import duckdb

# Connect to the database
conn = duckdb.connect('databases/traces.db')

# Check what tables exist
print("Tables in database:")
tables = conn.execute("SHOW TABLES").fetchall()
for table in tables:
    print(f"  - {table[0]}")

# Check traces table
print("\nTraces table structure:")
schema = conn.execute("DESCRIBE traces").fetchall()
for col in schema:
    print(f"  {col[0]}: {col[1]}")

# Count records
trace_count = conn.execute("SELECT COUNT(*) FROM traces").fetchone()[0]
print(f"\nNumber of traces: {trace_count}")

# Check if messages table exists
message_count = 0
try:
    message_count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    print(f"Number of messages: {message_count}")
except Exception as e:
    print(f"Messages table not found: {e}")

# Check if trace_messages table exists
junction_count = 0
try:
    junction_count = conn.execute("SELECT COUNT(*) FROM trace_messages").fetchone()[0]
    print(f"Number of trace-message links: {junction_count}")
except Exception as e:
    print(f"Trace_messages table not found: {e}")

# Check if sessions table exists
session_count = 0
try:
    session_count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    print(f"Number of sessions: {session_count}")
except Exception as e:
    print(f"Sessions table not found: {e}")

# Show sample records
if trace_count > 0:
    print("\nSample trace records:")
    sample = conn.execute("SELECT trace_id, session_id, model, prompt_tokens, completion_tokens, success FROM traces LIMIT 3").fetchall()
    for record in sample:
        print(f"  ID: {record[0]}, Session: {record[1]}, Model: {record[2]}, Tokens: {record[3]}/{record[4]}, Success: {record[5]}")

if message_count > 0:
    print("\nSample message records:")
    try:
        messages = conn.execute("SELECT message_id, session_id, role, LEFT(content, 50), token_count FROM messages LIMIT 5").fetchall()
        for msg in messages:
            print(f"  ID: {msg[0]}, Session: {msg[1]}, Role: {msg[2]}, Content: {msg[3]}..., Tokens: {msg[4]}")
    except Exception as e:
        print(f"Error reading messages: {e}")

if junction_count > 0:
    print("\nSample junction table records:")
    try:
        junctions = conn.execute("SELECT trace_id, message_id, message_order FROM trace_messages LIMIT 5").fetchall()
        for junction in junctions:
            print(f"  Trace: {junction[0]}, Message: {junction[1]}, Order: {junction[2]}")
    except Exception as e:
        print(f"Error reading junction table: {e}")

if session_count > 0:
    print("\nSample session records:")
    try:
        sessions = conn.execute("SELECT session_id, session_type, created_at, total_requests, total_tokens, is_active FROM sessions LIMIT 5").fetchall()
        for session in sessions:
            print(f"  ID: {session[0]}, Type: {session[1]}, Created: {session[2]}, Requests: {session[3]}, Tokens: {session[4]}, Active: {session[5]}")
    except Exception as e:
        print(f"Error reading sessions: {e}")

conn.close()