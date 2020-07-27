import os
import sys
import shutil
import subprocess

# Create an __init__.py if it doesn't already exist
f = open( 'wresproto/__init__.py', 'w' )

# Build a wheel in a subprocess
subprocess.check_call( [sys.executable, 'setup.py', 'bdist_wheel', '-d', '../../../dist/wresproto'] )

# Clean-up
try:
    shutil.rmtree( 'build' )
except OSError as e:
    print ( "Error: %s - %s." % ( e.filename, e.strerror ) )

try:
    shutil.rmtree( 'wresproto.egg-info' )
except OSError as e:
    print ( "Error: %s - %s." % ( e.filename, e.strerror ) )
