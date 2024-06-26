import os
import argparse
import requests
import time
import re

# Environment variables to provide defaults if URL or cert are not provided at the command line.
WRES_HOST_NAME = os.getenv('WRES_HOST_NAME')
WRES_CA_FILE = os.getenv('WRES_CA_FILE')

#==============================================================================
# The two functions, below, are used to support the evaluation. The function
# obtain_file_list obtains the list of files indicated by one of the data
# posting options, --observed, --predicted, or --baseline. The function 
# post_data_files posts the data files for a specific job and side, left  
# (observed), right (predicted), and baseline.
#==============================================================================

def obtain_file_list(file_argument) -> list:
    """
    Obtain a list of files from an argument.
    The file_argument must be either a file name or a directory.
    If its a directory, then the files will be obtained by listing
    the contents of the directory non-recursively!
    
    :param: str file_argument: Either a file or directory name.
    :return: The list of files, or None if the argument is None.
    :raise: An Exception if the file_argument is specified, but the path
    does not exist.
    """
    files = list()

    # If the argument is not specified, return an empty list.
    if file_argument is None:
        return files

    # If the path indicated does not exist, then raise an exception
    if not os.path.exists(file_argument):
        raise Exception(f"Path for data, {file_argument}, does not exist.")
        
    # For a directory, obtain the files within that directory. This is not recursive, however.
    if os.path.isdir(file_argument):
        files = [file_argument + "/" + f for f in os.listdir(file_argument) if os.path.isfile(file_argument + "/" + f)]

    # For a file, just add it to the list.
    else:
        files.append(file_argument)

    return files

def post_data_files(files, job_location, side):
    """
    Post the files provided to the side indicated of the job_location provided.
    :param: files: The files to post in a list, or an empty list if nothing is to be posted.
    :param: job_location: The URL of the job.
    :param: side: The job side to which to post the data, either 'left', 'right', or 'baseline'.
    :raise: Exception if a problem is encountered posting data.
    """
    if not args.silent:
        print(f"Uploading data to the {side} side")
    for filename in files:
        if os.path.getsize(filename) > 0:
            file_content = open(filename, "rb")

            # The use of files in this command ensures the data is posted as
            # multipart/form-data, which is required of data posted to a cluster service.
            data_post_response = requests.post( url=job_location + "/input/" + side,
                                                 verify = wres_ca_file,
                                                 files = {"data": file_content} )
            # Anything other than a 200 or 201 indicates a problem.
            if data_post_response.status_code != 201 and data_post_response.status_code != 200:
                raise Exception("The response code was NOT 201 nor 200, failed to post data.")
            if not args.silent:
                print(f"Uploaded {filename} successfully: {data_post_response.status_code}")
                if keep_input:
                    print(f"File persisted at {data_post_response.headers['content-location']}")
    if not args.silent:
        print(f"Finished uploading data to the {side} side\n")

def wait_for_job_finish(job_location):
    """
    Waits for the job to return with a termination status by repeatedly getting the status endpoint
    If the job fails then we also get the stdout endpoint to print the failure cause
    :param: job_location: The URL of the job.
    """
    evaluation_status=""
    current_time=0
    print_time=5
    if not args.silent:
        print("Job IN_PROGRESS checking every 5 seconds, printing again in 5 seconds")
    while ( evaluation_status != "COMPLETED_REPORTED_SUCCESS"
            and evaluation_status != "COMPLETED_REPORTED_FAILURE"
            and evaluation_status != "NOT_FOUND" ):
        # Pause for two seconds before asking the server for status.
        # If your evaluations take around 2 minutes to 2 hours this is appropriate.
        # If your evaluations take over 2 hours, increase to 20 seconds.
        # If your evaluations take less than 2 minutes, drop to 0.2 seconds.
        evaluation_status = requests.get( url = job_location + "/status",
                                          verify = wres_ca_file
                                        ).text
        if current_time >= print_time:
            print_time=print_time*2
            if not args.silent:
                print(f"Job IN_PROGRESS checking every 5 seconds, printing again in {print_time} seconds")
        time.sleep( 5 )
        current_time=current_time+5

    print( f"{evaluation_status}\n" )

    # Check for success. If not, then there is no reason to push any further.
    if evaluation_status != "COMPLETED_REPORTED_SUCCESS":
        stdout = requests.get( url = job_location + "/stdout",
                              verify = wres_ca_file
                             ).text
        if not args.silent:
            print("Failure reason is as follows: "+ stdout.split("EvaluationService The evaluation failed with the following stack trace:",1)[1])
        exit( 1 )

def obtain_output(job_location, job_id, output_folder):
    """
    Obtains the output and stores it into a temp directory at the output location
    :param: job_location: The URL of the job.
    :param: job_id: the id of the job we are getting the output of
    :param: output_folder: the folder we want to store the output at
    """
    response = requests.get(job_location + "/output/", allow_redirects=True,
                            verify = wres_ca_file,
                            headers = { 'Accept': 'text/html' })
    output = response.text
    pattern = r'<a\s+href="(?P<url>.*?)".*?>(?P<text>.*?)</a>'
    regex = re.compile(pattern)
    results = regex.findall(output, re.IGNORECASE | re.DOTALL)

    # This is the bit that actually pulls down the data from the WRES site and
    # writes it to files in awips.
    if not args.silent:
        print("Creating the directory " + output_folder + "/" + job_id + " to store output")
    os.mkdir("./" + output_folder + "/" + job_id)

    for result in results:
       output_link = job_location + "/" + result[0]
       filename = "./" + output_folder + "/" + job_id + "/" + result[1]
       r = requests.get(output_link, allow_redirects = True, verify = wres_ca_file,
                            headers = { 'Accept': 'text/html' })
       with open(filename, "wb") as f:
                f.write(r.content)
    if not args.silent:
        print( f"Stored the data at {output_folder}/{job_id}" )

#==============================================================================
# The evaluation process is captured in the Python code, below.
#==============================================================================
# Process the command line arguments and options. The argument descriptions
# are provided below.
#==============================================================================

parser = argparse.ArgumentParser(
                    prog='call_wres',
                    description='Calls a cluster instance of the WRES, such as the COWRES.',
                    epilog='Not sure if an epilog in the help will be needed, but this is where it would go.')
parser.add_argument('-f', '--filename',   help='Declaration filename')
parser.add_argument('-u', '--host',       help='Cluster WRES instance host (without the http prefix); defaults to WRES_HOST_NAME environment variable.')
parser.add_argument('-c', '--cert',       help='The certificate .pem file to authenticate the WRES instance; defaults to WRES_CA_FILE environment variable.')
parser.add_argument('-o', '--output',     help='Directory where output is to be written.', default=".")
parser.add_argument('-l', '--observed',   help='Data to post for the observed sources either one file or a directory.')
parser.add_argument('-p', '--predicted',  help='Data to post for the predicted sources either one file or a directory.')
parser.add_argument('-b', '--baseline',   help='Data to post for the baseline sources either one file or a directory.')
parser.add_argument('-k', '--keep_input', help='Instruct COWRES to not remove posted data after evaluation is completed', default=False, action="store_true")
parser.add_argument('-s', '--silent',     help='Suppresses additional logging produced by this script', default=False, action="store_true")

# Parse the arguments.
args = parser.parse_args()

# Declartion filename is required.
declaration_filename = args.filename

if declaration_filename is None:
    print('A project declaration is required to use this script. Please pass one in with the -f or --filename flags')
    exit(1)


# The host name is not required if we have the environment variable available to us.
host = args.host
if host is None:
    host = WRES_HOST_NAME
    if host is None:
        print('WRES host name was not specified as argument or environment variable. Aborting!')
        exit(1)

# A cert file is not required if we have the environment variable available to us.
wres_ca_file = args.cert
if wres_ca_file is None:
    wres_ca_file = WRES_CA_FILE
    if wres_ca_file is None:
        print('Certificate was not specified as argument or environment variable. Aborting!')
        exit(1)

# The output file argument has a default, so no checking is needed.
output_folder=args.output

# Obtain the list of files indicated by the --observed, --predicted, and --baseline options.
# This uses the obtain_file_list function near the top of this script.
try:
    observed_files = obtain_file_list(args.observed)
except Exception as e:
    print(f"Failed to obtain file list for observed. Exception: {e}")
    exit(1)
try:
    predicted_files = obtain_file_list(args.predicted)
except Exception as e:
    print(f"Failed to obtain file list for predicted. Exception: {e}")
    exit(1)
try:
    baseline_files = obtain_file_list(args.baseline)
except Exception as e:
    print(f"Failed to obtain file list for baseline. Exception: {e}")
    exit(1)

# Is data-direct posting to be performed? Check the length of the
# lists containing the files for each side. If data is to be posted,
# will it be kept? Identify the flag based on the argument.
data_posted = (len(predicted_files) > 0) or (len(observed_files) > 0) or (len(baseline_files) > 0)
keep_input=False
if data_posted and args.keep_input:
    keep_input=True

#==============================================================================
# Prepare for the evaluation!
# The declaration must be read into memory, and we need to determine if data
# is to be posted.
#==============================================================================

# Read the declaration into memory.
#print ( f"Reading the declaration file, {declaration_filename}..." )
with open(declaration_filename,'r') as decfile:
    evaluation = decfile.read()
if evaluation is None:
    print('Unable to read evaluation from declaration file.')
    exit(4)

# Printing arguments passed in
if not args.silent:
    print( "Executing the script with the following information provided")
    print( f"Declaration file:    {args.filename}")
    print( f"Host:                {args.host}")
    print( f"Cert Location:       {args.cert}")
    print( f"Output Location:     {args.output}")
    print( f"Predicted Data:      {args.predicted}")
    print( f"Baseline Data:       {args.baseline}")
    print( f"Observed Data:       {args.observed}")
    print( f"Data Posted:         {data_posted}")
    print( f"Keep Data:           {args.keep_input}")
    print( f"Silent:              {args.silent}\n")

#====================================================================================
# First, post the evaluation using the read in declaration. The data handed off to
# the service may need to include postInput and keepInput if evaluation data is to be 
# posted. If the evaluation is successfully posted, then obtain the location URL from
# the response.
#====================================================================================
if not args.silent:
    print( f"Posting the declaration to the WRES host: {host}..." )

# By default, the data posted is just the evaluation declaration.
data = { 'projectConfig': evaluation }

# If data is to be posted, then...
if data_posted:

    # Build the data to include the declaration, postInput, and keepInput.
    data = { 'projectConfig': evaluation, 
             'postInput': 'true',
             'keepInput': keep_input }

# Post the request.
post_result = requests.post( url="https://"+host+"/job",
                             verify = wres_ca_file,
                             data = data )

# Print the response. Check the status code. If neither 200 nor 201, then a failure
# occurred. Abort the execution.
# I'm including detailed prints for this call, being the first post, as an illustration.
# For latter requests, I'll only print a message if an error code is returned.
if post_result.status_code == 201 or post_result.status_code == 200:
    if not args.silent:
        print( "Posted the job successfully: " + str( post_result.status_code ) )
else:
    if not args.silent:
        print( "The response code was NOT 201 nor 200, failed to create evaluation. Aborting!" )
    exit( 1 )

# Obtain the job location for future use.
job_location = post_result.headers['Location']
job_id = job_location.split("/job/",1)[1]
if not args.silent:
    print( "Job ID is: " + job_id )
    print( "You can see the job at: " + job_location + "\n")

#====================================================================================
# Second, if data is to be posted, then post it. Data must be posted for observed
# (left), predicted (right), and baseline separately, and each file must be posted
# independently.  Post a postInputDone=true at the end once we are done posting data.
#====================================================================================

# Handle the posted data files if any were specified by the user. This uses the 
# post_data_files function near the top of this script.
if data_posted:
    if len(observed_files) > 0:
        try:
            post_data_files(observed_files, job_location, "left")
        except Exception as e:
            print( f"Failed to post at least one of the indicated observed files to the 'left' side. Exception: {e}" )
            exit( 1 )
    if len(predicted_files) > 0:
        try:
            post_data_files(predicted_files, job_location, "right")
        except Exception as e:
            print( f"Failed to post at least one of the indicated predicted files to the 'right' side. Exception: {e}" )
            exit( 1 )
    if len(baseline_files) > 0:
        try:
            post_data_files(baseline_files, job_location, "baseline")
        except Exception as e:
            print( f"Failed to post at least one of the indicated baseline files to the 'baseline' side. Exception: {e}" )
            exit( 1 )

    # Post that there is no more data to be posted and to kick off the evaluation
    data_post_done = requests.post( url=job_location + "/input",
                                    verify = wres_ca_file,
                                    data = {'postInputDone': 'true'} )
    if data_post_done.status_code != 201 and data_post_done.status_code != 200:
        if not args.silent:
            print( "The response code was NOT 201 nor 200, failed to post postInputDone. Aborting!" )
        exit( 1 )
    if not args.silent:
        print(f"Finished uploading data successfully: {data_post_done.status_code}\nStarting evaluation now")

#====================================================================================
# Third, with the evaluation now ongoing, poll the job status with GET requests until
# the job status changes to be COMPLETED_REPORTED_SUCCESS, COMPLETED_REPORTED_FAILURE,
# or NOT_FOUND (happens only for expired jobs).
#
# If a job fails we will get the stdout endpoint to print the cause of failure
#====================================================================================
wait_for_job_finish(job_location)

#==============================================================================
# Fourth, obtain the output. This section of code grabs and parses the full url
# of each line in the output using regular expressions.
# 
# output_link will contain the full url including the .png, .csv etc extension.
# 
# The code then creates the output file name by combining the filepath at the
# beginning of the code with the name of each output image/file. This is necessary
# for the request.get to pull each of the files on the /output url.
#==============================================================================
obtain_output(job_location, job_id, output_folder)
   
#==============================================================================
# Fifth: after completing the evaluation, and retrieving any data from it, it is
# important to clean up resources created by the evaluation. Therefore we 
# DELETE the output data through the service API.
#==============================================================================
if not args.silent:
    print( f"Posting a delete request for the job {job_id}..." )
remove_output = requests.delete( url = job_location + "/output",
                 verify = wres_ca_file )
if not args.silent:
    print( "Successfully deleted output resources for job "
       + job_id )
exit( 0 )

