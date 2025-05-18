#!/bin/bash

cd /src

# Starts Flask server
echo "### Starting flask uwsgi server."
exec uwsgi --ini /etc/uwsgi/uwsgi.ini  # runs flask