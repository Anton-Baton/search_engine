#!/usr/bin/env bash
# TODO better way of finding gunicorn binary
gunicorn web_ui:app -b 127.0.0.1:8000 --access-logfile -