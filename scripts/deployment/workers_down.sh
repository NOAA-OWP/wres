

if [ $# -ne 1 ]; then
    echo "This script requires one argument: the .yml file to use to run the docker command."
    exit 1
fi

if [ ! -f "$1" ]; then
    echo "The .yml file specified does not exist.  Specify one that does."
    exit 1
fi

echo "Running the command..."
echo docker run -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:$PWD" -w "$PWD" --cap-drop ALL --cpus 2 --memory 512M docker/compose:1.29.2 --file $1 down --volumes
echo ""
echo "============== COMMAND RUNNING ========================"
docker run -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD:$PWD" -w "$PWD" --cap-drop ALL --cpus 2 --memory 512M docker/compose:1.29.2 --file $1 down --volumes
echo "======================================================="
echo ""

echo "After the command, the following containers are on the system:"
docker container ls


