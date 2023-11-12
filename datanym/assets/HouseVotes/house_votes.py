import requests
from pathlib import Path
import zipfile
import shutil
import os

import csv
from collections import defaultdict
from datetime import datetime
import io
from pathlib import Path
import pandas as pd
from typing import Dict, List
import json

import votes


@asset
def bulk_download() -> None:
    """
    Downloads the list of votes using 
    https://github.com/unitedstates/congress/wiki/votes
    """
    for congress in [110, 111, 112, 113, 114, 115, 116, 117, 118]:
        opts = {"congress": congress, "chamber": "house"}
        votes.run(opts)


@asset
def parse_votes() -> Dict[str, List[Dict]]:
    """
    Parses the votes from the bulk download.
    """
votes = defaultdict(list)
bills = []
votes = []
no_bill = []
vote_transaction = []
count = 0
vote_keys = [ 'category','chamber','congress','date','number','question','requires','result','result_text','session','source_url','subject','type','updated_at','vote_id']
for file in Path("data").glob("**/*.json"):
    with open(file) as f:
        data = json.load(f)

    try:
        bill = data['bill']
        bill['vote_id']=data['vote_id']
        bills.append(bill)
    except: 
        no_bill.append(data)

    vote = {}
    for key in vote_keys:
        vote[key]=data.get(key, None)
    votes.append(vote)

    reps = []
    reps=[(vote,key) for key in data["votes"].keys() for vote in data["votes"][key]]
    for (vote,key) in reps:
        vote['key'] = key
        vote['vote_id'] = data['vote_id']
        vote_transaction.append(vote)
    

    return votes