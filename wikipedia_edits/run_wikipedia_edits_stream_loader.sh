#!/bin/bash

until pipenv run python wikipedia_edits_stream_loader.py
do
	echo "wikipedia_edits_stream_loader.py exited due to an exception, respawning..." >&2
	sleep 1
done

