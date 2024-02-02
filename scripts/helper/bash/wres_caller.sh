#!/bin/bash

declaration=""
host=""
cert=""
output_location="."
predicted=""
baseline=""
observed=""
keep_input=false
job_location=""
job_failed=false
silent=false

# Function to display script usage
usage() {
 echo "Usage: $0 [OPTIONS]"
 echo "-f, --filename     Declaration filename"
 echo "-u, --host         Cluster WRES instance host (without the http prefix); defaults to WRES_HOST_NAME environment variable."
 echo "-c, --cert         The certificate .pem file to authenticate the WRES instance; defaults to WRES_CA_FILE environment variable."
 echo "-o, --output       Directory where output is to be written in relation to directory calling the script from."
 echo "-l, --observed     Data to post for the observed_path sources either one file or a directory."
 echo "-p, --predicted    Data to post for the predicted sources either one file or a directory."
 echo "-b, --baseline     Data to post for the baseline sources either one file or a directory."
 echo "-k, --keep_input   Instruct COWRES to not remove posted data after evaluation is completed"
 echo "-s, --silent       Suppresses additional logging produced by this script"
}

# Posts the data needed for an evaluation
# $1 location of data files
# $2 side this data represents
post_data() {
  if [[ "$silent" != true ]]; then
    echo "Uploading data to $2 side"
  fi

  find "$1" -type f -print0 | while read -r -d '' file; do

    if [[ -s "$file" ]]; then
      post_result=$(curl -i -s --cacert $cert -F data="@$file" https://$host/job/$job_location/input/$2 )

      post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )

      if [ "$post_result_http_code" == "201" ] || [ "$post_result_http_code" == "200" ]
      then
        if [[ "$silent" != true ]]; then
          echo "Uploaded $file successfully: $post_result_http_code"
          if [[ "$keep_input" = true ]]; then
            echo "File persisted at: $( echo "$post_result" | grep Location )"
          fi
        fi
      else
        echo "test"
        if [[ "$silent" != true ]]; then
          echo "The response code was NOT 201 nor 200, exiting..."
          echo "Response was: $post_result"
        fi
        exit 2
      fi
    fi
  done
  if [[ "$silent" != true ]]; then
    echo "Finished uploading data to $2 side"
    echo " "
  fi
}

# Posts that data upload has finished
post_data_finish() {
  post_result=$(curl -i -s --cacert $cert -d "postInputDone=true" https://$host/job/$job_location/input )

  post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )
  if [ "$post_result_http_code" == "201" ] || [ "$post_result_http_code" == "200" ]
  then
    if [[ "$silent" != true ]]; then
      echo "Finished uploading data successfully: $post_result_http_code"
      echo "Starting evaluation now"
    fi
  else
    if [[ "$silent" != true ]]; then
      echo "The response code was NOT 201 nor 200, failed to create, exiting..."
      echo "Response was: $post_result"
    fi
    exit 2
  fi
}

# Posts the evaluation job
post_job() {
  post_result=$(curl -i -s --cacert $cert -d "projectConfig=$(cat $declaration)" -d "postInput=$data_posted" -d "keepInput=$keep_input" https://$host/job/ )
  job_location=$( echo "$post_result" | grep Location | sed 's/^.*job\///' | tr -d "\n\r" )

  post_result_http_code=$( echo -n "$post_result" | grep HTTP | tail -n 1 | cut -d' ' -f2 )
  if [ "$post_result_http_code" == "201" ] || [ "$post_result_http_code" == "200" ]
  then
    if [[ "$silent" != true ]]; then
      echo "Posted the job successfully: $post_result_http_code"
      echo "Job ID is: $job_location"
      echo "You can see the job at: https://$host/job/$job_location"
      echo " "
    fi
  else
    if [[ "$silent" != true ]]; then
      echo "The response code was NOT 201 nor 200, failed to create, exiting..."
      echo "Response was: $post_result"
    fi
    exit 2
  fi
}

# Gets the output for an evaluation job and stores it to the output directory
get_job_output() {
  post_result=$(curl -s --cacert $cert https://$host/job/$job_location/output )

  if [[ "$silent" != true ]]; then
    echo "Creating the directory $output_location/$job_location to store output"
  fi
  mkdir "./$output_location/$job_location"
  cd "./$output_location/$job_location"
  while IFS= read -r line; do
    $(curl -i -s --cacert $cert -O https://$host/job/$job_location/$line )
  done <<< "$post_result"

  if [[ "$silent" != true ]]; then
    echo "Stored the data at $output_location/$job_location"
  fi

}

# Delete the evaluation job outputs
delete_job_output() {
  post_result=$(curl -X "DELETE" -s --cacert $cert https://$host/job/$job_location/output )

  if [[ "$silent" != true ]]; then
    echo "$post_result"
  fi

}

# Posts that data upload has finished
wait_for_job_finish() {

  post_result=""
  time=0
  print_time=5
  if [[ "$silent" != true ]]; then
    echo "Job IN_PROGRESS, checking every 5 second, printing again in 5 seconds"
  fi
  while [[ "$post_result" != *"COMPLETED_REPORTED_SUCCESS"* && "$post_result" != *"COMPLETED_REPORTED_FAILURE"* && "$post_result" != *"NOT_FOUND"* ]]
  do
    post_result=$(curl -s --cacert $cert https://$host/job/$job_location/status )
    if [[ "$time" -ge "$print_time" ]]; then
      print_time=$(($print_time * 2))
      if [[ "$silent" != true ]]; then
        echo "Job IN_PROGRESS, checking every 5 second, printing again in $print_time seconds"
      fi
    fi
    time=$(($time + 5))
    sleep 5
  done

  if [[ "$post_result" = *"COMPLETED_REPORTED_FAILURE"* ]]; then
    job_failed=true
  fi

  echo "$post_result"
  echo " "
}

has_argument() {
  [[ ("$1" == *=* && -n ${1#*=}) || ( ! -z "$2" && "$2" != -*)  ]];
}

extract_argument() {
  echo "${2:-${1#*=}}"
}

# Function to handle options and arguments
handle_options() {
  while [ $# -gt 0 ]; do
    case $1 in
      -h | --help)
        usage
        exit 0
        ;;
      -f | --filename)
        if ! has_argument $@; then
          echo "declaration not specified." >&2
          usage
          exit 1
        fi

        declaration=$(extract_argument $@)

        shift
        ;;
      -u | --host)
        if ! has_argument $@; then
          echo "host not specified." >&2
          usage
          exit 1
        fi

        host=$(extract_argument $@)

        shift
        ;;
      -c | --cert)
        if ! has_argument $@; then
          echo "cert not specified." >&2
          usage
          exit 1
        fi

        cert=$(extract_argument $@)

        shift
        ;;
      -o | --output)
        if ! has_argument $@; then
          echo "output not specified." >&2
          usage
          exit 1
        fi

        output_location=$(extract_argument $@)

        shift
        ;;
      -p | --predicted)
        if ! has_argument $@; then
          echo "predicted not specified." >&2
          usage
          exit 1
        fi

        predicted=$(extract_argument $@)

        shift
        ;;
      -b | --baseline)
        if ! has_argument $@; then
          echo "baseline not specified." >&2
          usage
          exit 1
        fi

        baseline=$(extract_argument $@)

        shift
        ;;
      -l | --observed)
        if ! has_argument $@; then
          echo "observed not specified." >&2
          usage
          exit 1
        fi

        observed=$(extract_argument $@)

        shift
        ;;
      -k | --keep_input)
        keep_input=true
        ;;
      -s | --silent)
        silent=true
        ;;
      *)
        echo "Invalid option: $1" >&2
        usage
        exit 1
        ;;
    esac
    shift
  done
}

# If there are no arguments print usage help
if [ $# -eq 0 ]; then
  usage
  exit 1
fi


# Main script execution
handle_options "$@"

if [[ -z "$host" ]]; then
  host=$WRES_HOST_NAME
  if [[ -z "$host" ]]; then
    echo "WRES host name was not specified as argument or environment variable. Aborting!"
    exit 1
  fi
fi

if [[ -z "$cert" ]]; then
  host=$WRES_CA_FILE
  if [[ -z "$cert" ]]; then
    echo "Certificate was not specified as argument or environment variable. Aborting!"
    exit 1
  fi
fi

if [[ -z "$declaration" ]]; then
  echo "A project declaration is required to use this script. Please pass one in with the -f or --filename flags"
  exit 1
fi


if [[ -n "$observed" ]] || [[ -n "$predicted" ]] || [[ -n "$baseline" ]]; then
  data_posted=true
fi

if [[ "$silent" != true ]]; then
  echo "Executing the script with the following information provided"
  echo "Declaration file:    $declaration"
  echo "Host:                $host"
  echo "Cert Location:       $cert"
  echo "Output Location:     $output_location"
  echo "Predicted Data:      $predicted"
  echo "Baseline Data:       $baseline"
  echo "Observed Data:       $observed"
  echo "Data Posted:         $data_posted"
  echo "Keep Data:           $keep_input"
  echo "Silent:              $silent"
  echo " "
fi


if [[ "$silent" != true ]]; then
  echo "Posting the declaration to the WRES host: $host..."
fi

post_job "$data_posted"


# Post data
if [[ -n "$observed" ]]; then
   post_data "$observed" "left"
fi

if [[ -n "$predicted" ]]; then
  post_data "$predicted" "right"
fi

if [[ -n "$baseline" ]]; then
  post_data "$baseline" "baseline"
fi

if [[ "$data_posted" == true ]]; then
  post_data_finish
fi

# Wait for job to no longer be IN_PROGRESS
wait_for_job_finish

if [[ "$job_failed" == true ]]; then
  if [[ "$silent" != true ]]; then
    echo " "
    post_result=$(curl -s --cacert $cert https://$host/job/$job_location/stdout )
    echo "Failure reason is as follows: "
    echo "$( echo "$post_result" | sed -n -e '/The\ evaluation\ failed with/,$p' )"
  fi
  exit 1
fi

# Get output and clean up if the job didn't fail
get_job_output
delete_job_output

exit 0