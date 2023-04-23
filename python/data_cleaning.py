from python.data_util import *


# use this line of code to change wd to Yilun's ssd (for Yilun's use only)
# os.chdir("/Volumes/BigBadBoi/data")


''' file loading'''
# 1. load SafeGraph raw geometry dataset
geometry = pd.read_parquet("data/raw/SafeGraph/geometry_us_naics722410_202301/geometry_us_naics722410_202301.parquet")

# 2. load atlanta metro shapefile
tract = gpd.read_file("data/raw/shapefile/tract_ATLmetro_2019/tract_ATLmetro_2019.shp")
tractFIPS = (tract.STATEFP + tract.COUNTYFP + tract.TRACTCE).to_list()

# 3. load SafeGraph raw patterns dataset
months = ["0" + str(x) for x in range(1,10)] + [str(x) for x in range(10,13)]
patterns_list = [pd.read_csv(f"data/raw/SafeGraph/mpatterns_us_naics722410_2019{x}/mpatterns_us_naics722410_2019{x}.csv") for x in months]



''' process patterns dataset'''
for ix, patterns in enumerate(patterns_list):
    # 1. only retain visits initiated from census tracts in the Atlanta metro
    df = patterns.loc[patterns['FIPS'].isin(tractFIPS),['placekey', 'normalized_visits_by_state_scaling', 'time', 'FIPS', 'n_visitor']].reset_index(drop = True)

    # 2. resample visitor counts = 4
    rand_n = sum(df['n_visitor'] == 4)
    df.loc[df['n_visitor'] == 4,'n_visitor'] = np.random.randint(1,5,rand_n)


    # 3. recalculate normalized visits based on the relative share of number of visitors
    new_df = df.join(df.groupby('placekey').aggregate(total = ('n_visitor','sum')), how = 'left', on = 'placekey')
    new_df['normalized_visits'] = new_df['n_visitor'] / new_df['total'] * new_df['normalized_visits_by_state_scaling']

    # 4. re-organize columns
    patterns_list[ix] = new_df.loc[:, ['time', 'placekey', 'FIPS', 'normalized_visits']].reset_index(drop = True)

    # 5. output to csvs
    patterns_list[ix].to_csv(f'data/cleaned/SafeGraph/mpatterns_atl_naics722410_2019{months[ix]}.csv', index = False)



''' process geometry dataset '''
# 1. filter out bars that do not have any visit from tracts in ATL metro
placekey_atl = pd.concat([patterns_list[ix].placekey for ix in range(0,12)]).unique()
geometry_atl = geometry.loc[geometry.placekey.isin(placekey_atl),
                            ['placekey', 'location_name', 'brands', 'latitude', 'longitude', 'category_tags',
                             'opened_on', 'closed_on']].reset_index(drop = True)

# 2. output to csvs
geometry_atl.to_csv('data/cleaned/SafeGraph/geometry_atl_naics722410_202301.csv', index = False)