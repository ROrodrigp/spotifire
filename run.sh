#!/bin/bash
source venv/bin/activate
gunicorn -w 4 -b 0.0.0.0:8080 --log-file=gunicorn.log --log-level=debug app:app