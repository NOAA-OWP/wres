import os
import sys
import shutil
import subprocess

# Create an __init__.py if it doesn't already exist
f = open( 'wresproto/__init__.py', 'w' )

# Build a wheel in a subprocess
subprocess.check_call( [sys.executable, '-m' 'build', '--wheel', '--no-isolation', '--outdir', '../../../dist/wresproto'] )

# Clean-up
try:
    shutil.rmtree( 'build' )
except OSError as e:
    print ( "Warn: %s - %s." % ( e.filename, e.strerror ) )

try:
    shutil.rmtree( '__pycache__' )
except OSError as e:
    print ( "Warn: %s - %s." % ( e.filename, e.strerror ) )

try:
    shutil.rmtree( 'wresproto.egg-info' )
except OSError as e:
    print ( "Warn: %s - %s." % ( e.filename, e.strerror ) )
