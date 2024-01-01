import requests
from pathlib import Path
import zipfile
import shutil
import os
from ...resources.duckpond import SQL

import csv
from collections import defaultdict
from datetime import datetime
import io
from pathlib import Path
import pandas as pd
from typing import Dict, List
import json
import yaml

from copy import deepcopy

from dagster import (asset,
                     AssetOut,
                     multi_asset,
                     get_dagster_logger,
                     )
from . import votes
from git import Repo


@asset(io_manager_key="local_io_manager",group_name="house_assets")
def bulk_download() -> Path:
    """
    Downloads the list of votes using 
    https://github.com/unitedstates/congress/wiki/votes
    """

    base_dir = Path("data/sessions")
    for congress in [110, 111, 112, 113, 114, 115, 116, 117, 118]:
        opts = {"congress": congress, "chamber": "house"}
        votes.run(opts)
        shutil.move("data/" + str(congress), base_dir / str(congress))
    return base_dir


@multi_asset(
        outs = {
            "landing_house_votes": AssetOut(io_manager_key="DuckPondIOManager"),
            "landing_house_reps": AssetOut(io_manager_key="DuckPondIOManager"),
            "landing_house_vote_transaction": AssetOut(io_manager_key="DuckPondIOManager"),
        },group_name="house_assets")
def parse_votes(bulk_download) -> tuple[SQL, SQL, SQL]:
    """
    Parses the votes from the bulk download.

    Creates three lists:
    - reps: list of representatives
    - votes: list of votes
    - vote_transaction: list of how each representative voted

    """
    logger = get_dagster_logger()
    votes = []
    vote_transaction = []
    reps = []
    vote_cat = defaultdict(list)
    vote_keys = [ 
        'bill_number','bill_type',
        'amendment_author','amendment_number','amendment_type',
        'category','chamber','congress','date','number','question','requires','result','result_text',
        'session','source_url','subject','type','updated_at','vote_id']
    for file in Path("data/sessions/").glob("**/*.json"):
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
            vote_transaction.append(deepcopy(vote))

        rep=[vote for key in data["votes"].keys() for vote in data["votes"][key]]
        reps = reps + rep

    return tuple(SQL('select * from $df', df=pd.DataFrame(itm)) for itm in tuple([votes, reps, vote_transaction]))

@asset(group_name="house_assets")
def house_reps_download():
    """
    Downloads the list of representatives
    """

    local_directory_path=Path("data/congress-legislators")
    git_url="https://github.com/unitedstates/congress-legislators"

    if local_directory_path.exists():
        repo = Repo(local_directory_path)
        o = repo.remotes.origin
        o.pull()
    else:
        repo = Repo.clone_from(git_url, local_directory_path)

@multi_asset(
    outs = {
            "landing_house_reps_reps": AssetOut(io_manager_key="DuckPondIOManager"),
            "landing_house_reps_terms": AssetOut(io_manager_key="DuckPondIOManager"),
    },group_name="house_assets")
def parse_reps(house_reps_download) -> tuple[SQL, SQL]:
    logger = get_dagster_logger()

    with open("data/congress-legislators/legislators-current.yaml","r") as f:
        raw = yaml.safe_load(f)

    reps = []
    terms = []
    for rep in raw:
        temp = {'first': rep['name']['first'],
                'last': rep['name']['last'],
                'official_full': rep['name'].get('official_full', None),
                'birthday': rep['bio']['birthday'],
                'gender': rep['bio']['gender']}

        for k,v in rep['id'].items(): temp[k]=v

        reps.append(deepcopy(temp))
        for t in rep['terms']:

            if t['type'] == 'sen':
                continue

            cols = ['url','caucus','address','office','phone','fax','contact_form','party_affiliations','rss_url','how']
            for col in cols:
                if not t.get(col,False): t[col] = ""

            t['official_full'] = rep['name'].get('official_full', None)
            t['bioguide'] = rep['id']['bioguide']
            terms.append(deepcopy(t))
    return tuple(SQL('select * from $df', df=pd.DataFrame(itm)) for itm in tuple([reps, terms]))
