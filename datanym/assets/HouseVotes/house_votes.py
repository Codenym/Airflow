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

    base_dir = Path("data")
    for congress in [118]:#[110, 111, 112, 113, 114, 115, 116, 117, 118]:
        opts = {"congress": congress, "chamber": "house"}
        votes.run(opts)
    return base_dir




@multi_asset(
        outs = {
            "votes_staging": AssetOut(io_manager_key="local_to_s3_io_manager"),
            "reps_staging_": AssetOut(io_manager_key="local_to_s3_io_manager"),
            "vote_transaction_staging": AssetOut(io_manager_key="local_to_s3_io_manager"),
        },group_name="house_assets")
def parse_votes(bulk_download) -> tuple[List[Dict], List[Dict], List[Dict]]:
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
            vote_transaction.append(deepcopy(vote))

        rep=[vote for key in data["votes"].keys() for vote in data["votes"][key]]
        reps = reps + rep
    return tuple([votes, reps, vote_transaction])


@asset(io_manager_key='s3_to_sqlite_manager',group_name="house_assets")
def reps_staging_sql(reps_staging_):
    """
    Loads data from s3 into sql
    """
    return reps_staging_


@asset(io_manager_key='s3_to_sqlite_manager',group_name="house_assets")
def votes_staging_sql(votes_staging):
    """
    Loads data from s3 into sql
    """
    return votes_staging


@asset(io_manager_key='s3_to_sqlite_manager',group_name="house_assets")
def vote_transaction_staging_sql(vote_transaction_staging):
    """
    Loads data from s3 into sql
    """
    return vote_transaction_staging

@asset(group_name="house_assets")
def house_reps_download():
    """
    Downloads the list of representatives
    """

    local_directory_path=Path("congress-legislators")
    git_url="https://github.com/unitedstates/congress-legislators"

    if local_directory_path.exists():
        repo = Repo(local_directory_path)
        o = repo.remotes.origin
        o.pull()
    else:
        repo = Repo.clone_from(git_url, local_directory_path)

@multi_asset(
    outs = {
            "reps_staging": AssetOut(io_manager_key="local_to_s3_io_manager"),
            "terms_staging": AssetOut(io_manager_key="local_to_s3_io_manager"),
    },group_name="house_assets")
def parse_reps(house_reps_download) -> tuple[List[Dict], List[Dict]]:
    logger = get_dagster_logger()

    with open("data/congress-legislators/legislators-current.yaml","r") as f:
        raw = yaml.safe_load(f)

    reps = []
    terms = []
    for rep in raw:
        temp = {}
        temp['first']           = rep['name']['first']
        temp['last']            = rep['name']['last']
        temp['official_full']   = rep['name'].get('official_full', None)
        temp['birthday']        = rep['bio']['birthday']
        temp['gender']          = rep['bio']['gender']

        for k,v in rep['id'].items(): temp[k]=v

        reps.append(deepcopy(temp))
        for t in rep['terms']:
            if t['type'] == 'sen':
                continue
            if not t.get('url',False): t['url'] = ""
            if not t.get('caucus',False): t['caucus'] = ""
            if not t.get('address',False): t['address'] = ""
            if not t.get('office',False): t['office'] = ""
            if not t.get('phone',False): t['phone'] = ""
            if not t.get('fax',False): t['fax'] = ""
            if not t.get('contact_form',False): t['contact_form'] = ""
            if not t.get('party_affiliations',False): t['party_affiliations'] = ""
            if not t.get('rss_url',False): t['rss_url'] = ""
            if not t.get('how',False): t['how'] = ""


            t['official_full'] = rep['name'].get('official_full', None)
            t['bioguide'] = rep['id']['bioguide']
            terms.append(deepcopy(t))
    return tuple([reps, terms])

@asset(io_manager_key='s3_to_sqlite_manager',group_name="house_assets")
def house_rep_staging_sql(reps_staging):
    """
    Loads data from s3 into sql
    """
    return reps_staging

@asset(io_manager_key='s3_to_sqlite_manager',group_name="house_assets")
def house_terms_staging_sql(terms_staging):
    """
    Loads data from s3 into sql
    """
    return terms_staging