from python.data_util import *


# use this line of code to change wd to Yilun's ssd (for Yilun's use only)
# os.chdir("/Volumes/BigBadBoi/data")


''' file loading'''
# load SafeGraph raw geometry dataset
geometry = pd.read_parquet("data/raw/SafeGraph/geometry_us_naics722410_202301/geometry_us_naics722410_202301.parquet")
placekeys = geometry.loc[geometry.region == 'GA', 'placekey'].to_list()

# load atlanta metro shapefile
tract = gpd.read_file("data/raw/shapefile/tract_ATLmetro_2019/tract_ATLmetro_2019.shp")





''' process patterns parquet'''
#TODO
#df = pd.read_parquet("../data/cleaned/SafeGraph/mpatterns_us_201901/core_poi-geometry-patterns-part1.parquet")
#patterns = df.loc[df.placekey.isin(placekeys),:]


