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

import pandas as pd 
from copy import deepcopy
from functools import partial

from dagster import (asset,
                     AssetOut,
                     multi_asset,
                     get_dagster_logger,
                     StaticPartitionsDefinition
                     )
from . import votes
from git import Repo

download_sessions = [110, 111, 112, 113, 114, 115, 116, 117, 118]

@asset(io_manager_key="local_io_manager",group_name="house_assets")
def bulk_download() -> Path:
    """
    Downloads the list of votes using 
    https://github.com/unitedstates/congress/wiki/votes
    """

    base_dir = Path("data/sessions")
    for congress in download_sessions:
        if os.path.isdir(base_dir / str(congress)):
            continue
        opts = {"congress": congress, "chamber": "house"}
        votes.run(opts)
        shutil.move("data/" + str(congress), base_dir / str(congress))
    return base_dir


# @multi_asset(
#         partition_defs=StaticPartitionsDefinition([str(i) for i in download_sessions]),
#         outs = {
#             "landing_house_votes": AssetOut(io_manager_key="DuckPondIOManager"),
#             "landing_house_reps": AssetOut(io_manager_key="DuckPondIOManager"),
#             "landing_house_vote_transaction": AssetOut(io_manager_key="DuckPondIOManager"),
#         },group_name="house_assets")
# def parse_votes(bulk_download) -> tuple[SQL, SQL, SQL]:
#     """
#     Parses the votes from the bulk download.

#     Creates three lists:
#     - reps: list of representatives
#     - votes: list of votes
#     - vote_transaction: list of how each representative voted

#     """
#     logger = get_dagster_logger()
#     votes = []
#     vote_transaction = []
#     reps = []
#     vote_cat = defaultdict(list)
#     vote_keys = [ 
#         'bill_number','bill_type',
#         'amendment_author','amendment_number','amendment_type',
#         'category','chamber','congress','date','number','question','requires','result','result_text',
#         'session','source_url','subject','type','updated_at','vote_id']
#     for file in Path("data/sessions/").glob("**/*.json"):
#         # print(file)
#         with open(file) as f:
#             data = json.load(f)

#         try:
#             data['bill_number'] = data['bill']['number']
#             data['bill_type'] = data['bill']['type']
#         except: 
#             data['bill_number'] = None
#             data['bill_type'] = None

#         try:
#             data['amendment_author'] = data['amendment']['author']
#             data['amendment_number'] = data['amendment']['number']
#             data['amendment_type'] = data['amendment']['type']
            
#         except:
#             data['amendment_author'] = None
#             data['amendment_number'] = None
#             data['amendment_type'] = None 
        
#         vote_cat[data['category']].append(data)
#         vote = {}
#         for key in vote_keys:
#             vote[key]=data.get(key, None)
#         votes.append(vote)

#         rs=[(vote,key) for key in data["votes"].keys() for vote in data["votes"][key]]
#         vote = {}
#         for (v,key) in rs:
#             vote['voter_id'] = v['id']
#             vote['vote'] = key
#             vote['vote_id'] = data['vote_id']
#             vote_transaction.append(deepcopy(vote))

#         rep=[vote for key in data["votes"].keys() for vote in data["votes"][key]]
#         reps = reps + rep

#     return tuple(SQL('select * from $df', df=pd.DataFrame(itm)) for itm in tuple([votes, reps, vote_transaction]))


# parse an individual vote file
def parse_votes_file(file: Path):
    with open(file) as f:
        raw = json.load(f)

    def create_votes(vote_type='Yea', vote_id=''):
        df = pd.DataFrame(raw['votes'][vote_type])
        df['vote_type'] = vote_type
        df['vote_id'] = vote_id
        return df

    create_votes = partial(create_votes, vote_id=raw['vote_id'])
    vote_transaction = pd.concat(list(map(create_votes,raw['votes'])))

    try:
        df = pd.DataFrame(raw['bill'],index=[0])
        df.columns = ['bill_'+c for c in df.columns]
    except:
        df = pd.DataFrame(index=[0])
    no_votes = {k:v for k,v in raw.items() if k not in ['votes','bill']}
    vote = pd.concat([pd.DataFrame(no_votes,index=[0]),df],axis=1)
    return vote_transaction, vote

@multi_asset(
    outs = {
        "landing_house_votes": AssetOut(io_manager_key="DuckPondIOManager"),
        "landing_house_vote_transactions": AssetOut(io_manager_key="DuckPondIOManager")},
    group_name="house_assets",
)
def parse_votes(bulk_download) -> tuple[SQL, SQL]:
    logger = get_dagster_logger()
    vote_transactions = []
    votes = []
    for file in Path("data/sessions/").glob("**/*.json"):
        vt, v = parse_votes_file(file)
        vote_transactions.append(vt)
        votes.append(v)
    return tuple(SQL('select * from $df', df=pd.concat(itm)) for itm in tuple([votes,vote_transactions]))
    

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
            "landing_house_reps": AssetOut(io_manager_key="DuckPondIOManager"),
            "landing_house_rep_terms": AssetOut(io_manager_key="DuckPondIOManager"),
    },group_name="house_assets")
def parse_reps(house_reps_download) -> tuple[SQL, SQL]:
    logger = get_dagster_logger()


    reps = []
    terms = []

    id_cols = ["bioguide","thomas","lis","govtrack","opensecrets","votesmart","fec",
               "cspan","wikipedia","house_history","ballotpedia","maplight","icpsr",
               "wikidata","google_entity_id","wikipedia","wikidata"]
    
    for file in ["data/congress-legislators/legislators-current.yaml","data/congress-legislators/legislators-historical.yaml"]:
        with open(file,"r") as f:
            raw = yaml.safe_load(f)

        for rep in raw:
            temp = {'first': rep['name']['first'],
                    'last': rep['name']['last'],
                    'official_full': rep['name'].get('official_full', ''),
                    'birthday': rep['bio'].get('birthday', ''),
                    'gender': rep['bio']['gender']}

            for col in id_cols:
                temp[col] = rep['id'].get(col, '')
            # for k,v in rep['id'].items(): temp[k]=v

            reps.append(deepcopy(temp))
            for t in rep['terms']:

                if t['type'] == 'sen':
                    continue

                cols = ['url','caucus','address','office','phone','fax','contact_form','party_affiliations','rss_url','how']
                for col in cols:
                    if not t.get(col,False): t[col] = ''

                t['official_full'] = rep['name'].get('official_full', '')
                t['bioguide'] = rep['id']['bioguide']
                terms.append(deepcopy(t))
    return tuple(SQL('select * from $df', df=pd.DataFrame(itm)) for itm in tuple([reps, terms]))

def load_sql_file(sql_file: Path):
    with open(sql_file, "r") as f:
        return f.read()
    
@asset(group_name="house_assets",io_manager_key="DuckPondIOManager")
def staging_house_votes(landing_house_votes):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/HouseVotes/sql_scripts/staging_house_votes.sql"))
    return SQL(sql_template,landing_house_votes=landing_house_votes)

@asset(group_name="house_assets",io_manager_key="DuckPondIOManager")
def staging_house_vote_transactions(landing_house_vote_transactions):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/HouseVotes/sql_scripts/staging_house_vote_transactions.sql"))
    return SQL(sql_template, landing_house_vote_transactions=landing_house_vote_transactions)

@asset(group_name="house_assets",io_manager_key="DuckPondIOManager")
def staging_house_reps(landing_house_reps):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/HouseVotes/sql_scripts/staging_house_reps.sql"))
    return SQL(sql_template, landing_house_reps=landing_house_reps)


@asset(group_name="house_assets",io_manager_key="DuckPondIOManager")
def staging_house_rep_terms(landing_house_rep_terms):
    sql_template = load_sql_file(sql_file=Path("datanym/assets/HouseVotes/sql_scripts/staging_house_rep_terms.sql"))
    return SQL(sql_template, landing_house_rep_terms=landing_house_rep_terms)
