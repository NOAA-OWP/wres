

if [ $# -ne 1 ]; then
    echo "This script requires one argument: the .yml file to use to run the docker command."
    exit 1
fi

if [ ! -f "$1" ]; then
    echo "The .yml file specified does not exist.  Specify one that does."
    exit 1
fi

echo "Running the command..."
echo docker run -e HOST_NAME=$(hostname) -d -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:$PWD" -v "$HOME/.docker/config.json:/root/.docker/config.json" -w "$PWD" --cap-drop ALL --cpus 2 --memory 512M docker/compose:1.29.2 --file $1 up --scale worker=2 --scale eventsbroker=1 --scale graphics=2 
echo ""
echo "============== COMMAND RUNNING ========================"
docker run -e HOST_NAME=$(hostname) -d -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:$PWD" -v "$HOME/.docker/config.json:/root/.docker/config.json" -w "$PWD" --cap-drop ALL --cpus 2 --memory 512M docker/compose:1.29.2 --file $1 up --scale worker=2 --scale eventsbroker=1 --scale graphics=2
echo "======================================================="
echo ""

echo "To view the up status, run 'docker logs [number]' where the [number] is what is displayed above as the return for the executed command."
echo "When complete, check for the expected containers using 'docker container ls'."


