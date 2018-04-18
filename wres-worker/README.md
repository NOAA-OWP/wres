# wres-worker

A long-running worker shim that waits for a job, launches a WRES in response
to a job, and repeats.

## Options

A worker will look at the -Dwres.broker value for a broker hostname, defaults
to localhost if none is present.

## docker-compose.yml

This docker-compose configuration is intended for machines that solely run
workers.

(The docker-compose configuration one level up is intended for the machine that
runs several roles simultaneously, including the broker and ui/service/tasker).

This docker-compose configuration can also be used on a development machine to
run workers independent from the docker-compose run of the tasker and broker.