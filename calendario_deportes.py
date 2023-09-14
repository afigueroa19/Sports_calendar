from dataclasses import replace
from azure.cosmos import CosmosClient
import uuid
import json
from datetime import date, datetime, timedelta
import requests
import calendar
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import os
from azure.core.credentials import AzureKeyCredential
import seaborn as sns
import matplotlib.pyplot as plt


###Ruta
url_cosmos = 'https://proyecto-cuotas-pty.documents.azure.com:443/'
key_cosmos = 'NOPE=='

client_cosmos = CosmosClient(url_cosmos, credential=key_cosmos)
database_cosmos = client_cosmos.get_database_client('sc_pty')
container = database_cosmos.get_container_client('cuotas')