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


from dagster import (asset,
                     AssetOut,
                     multi_asset,
                     get_dagster_logger,
                     )
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


@multi_asset(
        out = {
            "votes_staging": AssetOut(io_manager_key="local_to_s3_io_manager"),
            "reps_staging": AssetOut(io_manager_key="local_to_s3_io_manager"),
            "vote_transaction_staging": AssetOut(io_manager_key="local_to_s3_io_manager"),
        })
def parse_votes() -> Dict[str, List[Dict]]:
    """
    Parses the votes from the bulk download.

    Creates three lists:
    - reps: list of representatives
    - votes: list of votes
    - vote_transaction: list of how each representative voted

    """
    votes = []
    vote_transaction = []
    reps = []
    vote_cat = defaultdict(list)
    vote_keys = [ 
        'bill_number','bill_type',
        'amendment_author','amendment_number','amendment_type',
        'category','chamber','congress','date','number','question','requires','result','result_text',
        'session','source_url','subject','type','updated_at','vote_id']
    for file in Path("data").glob("**/*.json"):
        with open(file) as f:
            data = json.load(f)

        try:
            data['bill_number'] = data['bill']['number']
            data['bill_type'] = data['bill']['type']
        except: 
            data['bill_number'] = None
            data['bill_type'] = None

        try:
            data['amendment_author'] = data['amendment']['author']
            data['amendment_number'] = data['amendment']['number']
            data['amendment_type'] = data['amendment']['type']
            
        except:
            data['amendment_author'] = None
            data['amendment_number'] = None
            data['amendment_type'] = None 
        
        vote_cat[data['category']].append(data)
        vote = {}
        for key in vote_keys:
            vote[key]=data.get(key, None)
        votes.append(vote)

        rs=[(vote,key) for key in data["votes"].keys() for vote in data["votes"][key]]
        vote = {}
        for (v,key) in rs:
            vote['voter_id'] = v['id']
            vote['vote'] = key
            vote['vote_id'] = data['vote_id']
            vote_transaction.append(vote)

        rep=[vote for key in data["votes"].keys() for vote in data["votes"][key]]
        reps = reps + rep
    return (votes, reps, vote_transaction)