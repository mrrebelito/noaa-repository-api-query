from glob import glob
import pandas as pd

def get_platform_name(value):
    
    value = value.split('.')[0]
    return value.upper()

f = glob('*.csv')

dfs = [pd.read_csv(x) for x in f]

new_dfs = []

for fn,i in zip(f,dfs):
    i['From Platform'] = get_platform_name(fn)
    # i = i.drop(columns=['mods.sm_localcorpname'])
    new_dfs.append(i)


df = pd.concat(new_dfs)

  
df.to_csv('concated_platforms.csv', index=False)