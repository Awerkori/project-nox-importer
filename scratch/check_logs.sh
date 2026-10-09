#!/bin/bash
tail -n 200 ~/.pm2/logs/project-nox-importer-out.log || true
tail -n 200 ~/.pm2/logs/project-nox-importer-error.log || true
