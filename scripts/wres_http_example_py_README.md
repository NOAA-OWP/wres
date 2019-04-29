# Get started with wres_http_example.py

## The Problem: Dependencies

Software has dependencies. The python program will print an ImportError without
the dependencies being available:

    $ ./wres_http_example.py
    Traceback (most recent call last):
      File "./wres_http_example.py", line 7, in <module>
        import requests
    ImportError: No module named 'requests'
    $

The requests HTTP client library is used by the WRES example and needs to
be available for the example program to work.

## The Solution: pip and virtual environments

The following example was written from and for a CentOS 7 machine. YMMV.

Next to the wres_http_example.py is a wres_http_example_py_requirements.txt
which can be used by pip to get dependencies.

But prior to using pip, for isolation of program A's dependencies from program
B's dependencies, use venv or virtualenv to create an isolated environment.

    $ python3 -m venv wres_http_venv
    $

Activate the virtual environment

    $ source wres_http_venv/bin/activate
    (wres_http_venv)$

Upgrade pip

    (wres_http_venv)$ pip3 install --upgrade pip
    (wres_http_venv)$

Install the dependencies of wres_http_example.py

    (wres_http_venv)$ pip3 install -r wres_http_example_py_requirements.txt
    ...
    Installing collected packages: certifi, chardet, idna, urllib3, requests
    (wres_http_venv)$

Now the example program should run

    (wres_http_venv)$ ./wres_http_example.py [name]
    Here is the WRES evaluation project conf...
    ...
    Congratulations! You successfully used the WRES HTTP API to
    ...
    (wres_http_venv)$

Deactivate the virtual environment

    (wres_http_venv)$ deactivate
    $

## Routine

After having created the virtual environment for the example, use only the
activate and deactivate steps unless the dependencies change.

Activate

    $ source wres_http_venv/bin/activate
    (wres_http_venv)$

Run (and more)

    (wres_http_venv)$ ./wres_http_example.py [name]

Deactivate

    (wres_http_venv)$ deactivate
    $

## More information

None of the above is unique to WRES. These are standard tools and there are
alternative ways to manage the dependencies and isolation.

References for above tools:

See https://pypi.org/project/pip/
See https://docs.python.org/3/library/venv.html

Similar alternatives:

See https://virtualenv.pypa.io/en/stable/
See https://pipenv.readthedocs.io/en/latest/
