import pandas as pd
import geopandas as gpd
import os, json
from tqdm import tqdm
import numpy as np
from census import Census


def censusAPI_tract(api_key, table, year, stateFIPS, countyFIPSlist, var_code_list, var_name_list):
    '''
    last modified: 04/24/2023
    :param api_key: census api key
    :param table: ['detailed' 'profile']
    :param year
    :param stateFIPS: string
    :param countyFIPSlist: a list of countyFIPS
    :param var_code_list: ["B01003_001E"]
    :param var_name_list: ['totpop']
    :return: a pandas dataframe of requested census data
    '''
    # api_key ='d54b04fce5ead0b754d8951da1ced097f3d050e1'
    c = Census(api_key, year=year)
    if table == 'detailed':
        df = c.acs5.state_county_tract(tuple(var_code_list), stateFIPS, '*', '*')
    if table == 'profile':
        df = c.acs5dp.state_county_tract(tuple(var_code_list), stateFIPS, '*', '*')

    df = pd.DataFrame.from_dict(df)

    df['FIPS'] = df.state + df.county + df.tract
    output = df.loc[df.county.isin(countyFIPSlist), :].\
                reset_index(drop = True).\
                rename(columns = {'state':'STATEFP', 'county':'COUNTYFP','tract':'TRACTCE'})

    for i, code in enumerate(var_code_list):
        output = output.rename(columns={code: var_name_list[i]})

    return output



