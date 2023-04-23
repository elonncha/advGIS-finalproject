from python.data_util import *


''' file loading'''
# 1. load SafeGraph raw geometry dataset
geometry = pd.read_csv("data/cleaned/SafeGraph/geometry_atl_naics722410_202301.csv")

# 2. load atlanta metro shapefile
tract = gpd.read_file("data/raw/shapefile/tract_ATLmetro_2019/tract_ATLmetro_2019.shp")
tract['FIPS'] = (tract.STATEFP + tract.COUNTYFP + tract.TRACTCE)

# 3. load SafeGraph raw patterns dataset
months = ["0" + str(x) for x in range(1,10)] + [str(x) for x in range(10,13)]
patterns = [pd.read_csv(f"data/cleaned/SafeGraph/mpatterns_atl_naics722410_2019{x}.csv") for x in months]
