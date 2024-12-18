# You can views logs for this script with 'tail -f /var/log/backfill.log'

import obspy
import os
from obspy import read, Stream
from obspy.core import UTCDateTime

from flask import Flask, request, send_file, render_template_string
app = Flask(__name__)

import logging
app.debug = True


logging.basicConfig(
    filename='/var/log/backfill.log', # MAKE THIS FILE AND GIVE USER PERMISSIONS (SEE LOGS WITH 'tail -f /var/log/backfill.log') 
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s:%(message)s'
)

miniseed_dir = "/var/cache/guralp/miniseed_fed" # CHANGE THIS TO YOUR SAVE LOCATION!

trim_data = True # toggle whether to trim .mseed files or not (trim feature is currently under development and is unsafe)

def convert_unix_to_utc(timestamp):
    utc_datetime = UTCDateTime(timestamp)
    return utc_datetime

# Find files insdie a directory with a matching seedname
def find_name_matching_files(directory, search_name):
    name_matching_files = []
    search_name = search_name.replace('.', '_') # Match naming format
    logging.info(f"Searching in directory: {directory} for files containing '{search_name}'")

    try:
        for root, dirs, files in os.walk(directory):
            for file in files:
                if search_name in file:
                    file_path = os.path.join(root, file)
                    name_matching_files.append(file_path)

    except Exception as e:
        logging.error(f"An error occurred while accessing the directory: {e}")

    logging.info(f"Total name matching files found: {len(name_matching_files)}")
    return name_matching_files

# Get start and end times of a .mseed file
def get_start_end_times(input_file):
    try:
        st = obspy.read(input_file)
        first_trace_start_time = st[0].stats.starttime
        last_trace_end_time = st[-1].stats.endtime

        return first_trace_start_time, last_trace_end_time

    except Exception as e:
        logging.error(f"Error in get_start_end_times with file {input_file}: {e}")
        raise

# Check if two .mseed files are overlapping
def check_overlap(backfill_start, backfill_end, file):
    try:
        try:
            file_start, file_end = get_start_end_times(file)
        except Exception as e:
            logging.error(f"Failed to retrieve start/end times for {file}: {e}")
            return False

        # Check if overlapping
        if backfill_start <= file_end and file_start <= backfill_end:
            return True
        else:
            return False
    except Exception as e:
        logging.error(f"Error in check_overlap for file {file}: {e}")
        return False

def check_position(backfill_start, backfill_end, file_start, file_end):
    # 1 - file is fully within backfill request (backfill contains the file)
    # 2 - backfill request is fully within the file (file contains the backfill request)
    # 3 - file is partially within backfill request (file starts before backfill)
    # 4 - file is partially within backfill request (file ends after backfill)
    # 0 - file doesn't overlap with backfill request at all

    #logging.info(f"(backfill_start: {backfill_start}, backfill_end: {backfill_end}, file_start: {file_start}, file_end: {file_end})")

    if (file_start >= backfill_start) and (file_end <= backfill_end):
        logging.info("check_position result: 1")
        return 1
    elif (file_start < backfill_start) and (file_end > backfill_end):
        logging.info("check_position result: 2")
        return 2
    elif (file_start < backfill_start) and (file_end >= backfill_start) and (file_end <= backfill_end):
        logging.info("check_position result: 3")
        return 3
    elif (file_start <= backfill_end) and (file_start >= backfill_start) and (file_end > backfill_end):
        logging.info("check_position result: 4")
        return 4
    else:
        return 0

# Combine multiple .mseed files into one
def combine_mseed_files(file_paths, output_file):
    combined_stream = None
    for file_path in file_paths:
        stream = read(file_path)

        # If combined_stream is None, start it with the first file
        if combined_stream is None:
            combined_stream = stream
        else:
            # Append new stream to the combined stream
            combined_stream += stream

    combined_stream.merge(method=1)  # Method 1 merges and fills gaps with NaNs if needed
    combined_stream.write(output_file, format='MSEED')
    logging.info(f"Combined file written to: {output_file}")

# Combine multiple ObsPy Stream objects 
def combine_streams(streams, output_file):
    combined_stream = Stream()
    for stream in streams:
        combined_stream += stream

    combined_stream.merge(method=1)  # Method 1 merges traces and fills gaps with NaNs if needed

    # Write to single .mseed file
    combined_stream.write(output_file, format='MSEED')
    logging.info(f"Combined file written to: {output_file}")


@app.route('/backfill/', methods=['GET'])
def main():
    # Extract params
    seedname = request.args.get('channel')
    start = int(request.args.get('from'))
    end = int(request.args.get('to'))

    # Convert times to utc
    start_utc = convert_unix_to_utc(start)
    end_utc = convert_unix_to_utc(end)
                                                            
      # Find files with matching seednames
    name_matching_files = find_name_matching_files(miniseed_dir, seedname)
    if name_matching_files:
        logging.info(f"Found {len(name_matching_files)} files with a matching seedname to the backfill request")
    else:
        logging.info(f"No files found matching '{seedname}'")
        return f"No files found matching '{seedname}' found.", 404

    # Within the matching name files look for files that match the backfill request time
    good_files = []

    if trim_data:
        for file in name_matching_files:
            try:
                st = read(file)
                file_start, file_end = get_start_end_times(file)

                overlap_state = check_position(start_utc, end_utc, file_start, file_end)

                if overlap_state == 1:
                    logging.info(f"{file} is FULLY within the time boundaries of the backfill request")
                    good_files.append(st)
                elif overlap_state == 2:
                    trimmed_stream = st.slice(start_utc, end_utc)
                    logging.info(f"Adding trimmed stream for {file} within bounds (2)")
                    good_files.append(trimmed_stream)
                elif overlap_state == 3:
                    trimmed_stream = st.slice(start_utc, file_end)
                    logging.info(f"Adding trimmed stream for {file} partial start overlap (3)")
                    good_files.append(trimmed_stream)
                elif overlap_state == 4:
                    trimmed_stream = st.slice(file_start, end_utc)
                    logging.info(f"Adding trimmed stream for {file} partial end overlap (4)")
                    good_files.append(trimmed_stream)
            except Exception as e:
                logging.error(f"Error processing file {file}: {e}")
    else:
        for file in name_matching_files:
            try:
                if check_overlap(start_utc, end_utc, file):
                    st = read(file)
                    good_files.append(st)
            except Exception as e:
                logging.error(f"Error reading file {file}: {e}")


    if good_files:
        logging.info(f"Found {len(good_files)} files that match the backfill request")
    else:
        logging.info("No files found matching the backfill request times")
        return "No files found matching the backfill request times.", 404

    # Now that we have the relevant file(s), combine them into one .mseed file for sending
    output_file = f"{miniseed_dir}/backfill_{seedname}_{start}_{end}.mseed"
    combine_streams(good_files, output_file)

    # Send as HTTP response
    if os.path.exists(output_file):
        return send_file(output_file, as_attachment=True, download_name="backfill.mseed")
    else:
        logging.info("Returning 404")
        return "Backfill file failed to generate.", 404

# Store active challenges (normally, you'd use a Redis cache, but this is simple for now)
active_challenges = {}

# Dummy user database (passwords should be hashed in reality, not plain text)
USER_DATABASE = {
    'tom': 'heskey',  # Example: Tom's password is "heskey"
}

@app.route('/request_challenge/', methods=['GET'])
def request_challenge():
    logging.info(f"request_challenge called")
    username = request.args.get('username')
    if username not in USER_DATABASE:
        logging.info("user not found")
        return "error - User not found", 404


    # Generate a random challenge (use something stronger in production)
    challenge = str(random.randint(100000, 999999)) + str(int(time.time()))
    active_challenges[username] = challenge  # Store the challenge for this user
    logging.info(f"Challenge for {username}: {challenge}")  # For debugging
    return jsonify({'challenge': challenge})


# Step 2: Verify the client's response
@app.route('/verify_response/', methods=['POST'], endpoint='verify_response')
def verify_response():
    logging.info(f"verify_response called")
    data = request.json
    username = data.get('username')
    client_response = data.get('response')

    logging.info(f"client {username} has responded with {client_response}")

    if not username or not client_response:
        logging.info('error - Missing username or response')
        return jsonify({'error': 'Missing username or response'}), 400

    if username not in USER_DATABASE:
        logging.info('error - User not found')
        return jsonify({'error': 'User not found'}), 404

    # Get the challenge for this user
    challenge = active_challenges.get(username)
    if not challenge:
        logging.info('error - Challenge not found')
        return jsonify({'error': 'Challenge not found'}), 400

    # Get the user's stored password (hash it in production)
    password = USER_DATABASE[username]
    logging.info(f"password from database: {password}")

    # Recreate the expected response using the same method as the client
    expected_response = hashlib.sha256((challenge + password).encode()).hexdigest()
    logging.info(f"expected_reponse = {expected_response}")


    if client_response == expected_response:
        logging.info('message - Login successful!')
        return jsonify({'message': 'Login successful!'}), 200
    else:
        logging.info('error - Invalid response')
        return jsonify({'error': 'Invalid password'}), 401


@app.route('/login/', methods=['GET'])
def login():
    logging.info("login start")
    username = request.args.get('username')
    if not username:
        return "error - Username is required", 400
    if not password:
        return "error - Password is required", 400
    password = request.args.get('password')
    logging.info(f"login success for {username} with password {password}")
    return "Login function executed successfully", 200


@app.before_request
def log_request_info():
    logging.info(f"Request: {request.method} {request.path}")



if __name__ == "__main__":
  #  app.run()
    app.run(host='0.0.0.0', port=8080)
