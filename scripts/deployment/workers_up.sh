#!/bin/bash

# Get the options
pulling=yes
while getopts ":n" option; do
    case $option in
        n) # no pulling to be done
           pulling=no
           echo "Not pulling images; images must exist locally."
           shift
           ;;
    esac
done

if [ $# -ne 1 ]; then
    echo "This script requires one argument: the .yml file to use to run the docker command."
    exit 1
fi

if [ ! -f "$1" ]; then
    echo "The .yml file specified does not exist.  Specify one that does."
    exit 1
fi

if [ $pulling == "yes" ]
then

    echo "Creating the image pull container..."
    echo Running ... docker create --rm -e HOST_NAME=$(hostname) -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:$PWD" -w "$PWD" --cap-drop ALL --cpus 2 --memory 512M docker/compose:1.29.2 --file $1 pull. 
    container_id=$(docker create --rm -e HOST_NAME=$(hostname) -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:$PWD" -w "$PWD" --cap-drop ALL --cpus 2 --memory 512M docker/compose:1.29.2 --file $1 pull)

    if [ $? != 0 ]
    then
        echo "Docker command to create pull container failed.  See logging above."
        exit 1
    fi

    echo "The container id returned for the pull container is $container_id"
    echo "Running ... docker cp ~/.docker $container_id:/root/."
    docker cp ~/.docker $container_id:/root/.

    if [ $? != 0 ]
    then
        echo "Docker command to copy docker registry credentials into container failed.  See logging above."
        exit 1
    fi

    echo "Start the container $container_id to pull images. Observe the logging to see if problems occur."
    echo "Running ... docker container start -a $container_id"
    docker container start -a $container_id

    if [ $? != 0 ]
    then
        echo "Docker command to start the pull image container failed.  See logging above."
        exit 1
    fi
fi

echo "Bringing up the service using images that have been pulled to this machine."
echo "If any images cannot be found locally, this command will fail."
echo Running ... docker run -e HOST_NAME=$(hostname) -d -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:$PWD" -v "$HOME/.docker/config.json:/root/.docker/config.json" -w "$PWD" --cap-drop ALL --cpus 2 --memory 512M docker/compose:1.29.2 --file $1 up --scale worker=2 --scale eventsbroker=1 --scale graphics=2 --scale writing=2
echo ""
echo "============== COMMAND RUNNING ========================"
docker run -e HOST_NAME=$(hostname) -d -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:$PWD" -w "$PWD" --cap-drop ALL --cpus 2 --memory 512M docker/compose:1.29.2 --file $1 up --scale worker=3 --scale eventsbroker=1 --scale graphics=3 --scale writing=3
echo "======================================================="
echo ""

echo "To view the up status, run 'docker logs [number]' where the [number] is what is displayed above as the return for the executed command."
echo "When complete, check for the expected containers using 'docker container ls'."

