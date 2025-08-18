#!/bin/sh
git pull
conda activate customer_projects
python --version
python -m pip install --upgrade pip
pip install --upgrade -r requirements.txt
