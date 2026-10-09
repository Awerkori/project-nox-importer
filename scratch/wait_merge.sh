#!/bin/bash
until gh pr checks 358 --fail-fast=false; do
  echo "Waiting for checks..."
  sleep 15
done
gh pr merge 358 --squash --delete-branch
