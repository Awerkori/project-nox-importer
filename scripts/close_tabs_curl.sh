#!/bin/bash
IDS=$(curl -s http://127.0.0.1:9222/json/list | grep -E '"id"|"url"' | grep -B 1 "supabase.com" | grep '"id"' | awk -F'"' '{print $4}')
for id in $IDS; do
  echo "Closing $id..."
  curl -s http://127.0.0.1:9222/json/close/$id
done
