"""This script updates the metadata spreadsheet with the latest data from the sheets in the local directory (see parse.py)."""
from parse import *
import httplib2
import os
from apiclient import discovery
from google.oauth2 import service_account
import pygsheets
import pandas as pd


df = get_big_df()
gc = pygsheets.authorize(service_file='google_secret.json')
sh = gc.open("test")
wks = sh.worksheet_by_title('raw')
wks.set_dataframe(df, (1,1))

### summarize
wks = sh.worksheet_by_title('summary')
summ = get_summary_df()
wks.set_dataframe(summ, (1,1))
