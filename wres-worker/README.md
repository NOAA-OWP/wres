# wres-worker

A long-running worker shim that waits for a job, launches a WRES in response
to a job, and repeats.

## Options

A worker will look at the -Dwres.broker value for a broker hostname, defaults
to localhost if none is present.

