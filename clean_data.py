import pandas as pd
import os

def checkId(x):
    try :  
        float(x) 
        return True
    except : 
        print(x)
        return False

main_dir = '/home/vca_rishik/rishik/COVID-19-tweets/'
full_dir = main_dir + 'data_full/'
id_dir   = main_dir + 'data/'

for file in sorted(os.listdir(full_dir))[:-2]:
    print('Reading... ' + file)
    print()
    a = pd.read_csv(full_dir + file)
    print("Original length: ", len(a))
    a = a.dropna(subset = ['id'])
    print("Length after dropping NaNs: ", len(a))
    a = a.drop_duplicates()
    print("Length after dropping duplicates: ", len(a))

    # a.id
    x = a.id.apply(checkId)
    b = a[~x]
    a = a[x]
    
    if(len(b) > 0):
        a.to_csv(full_dir + file, index = None)
        print("Invalid rows(dropped!): ", len(b))
        print("Final length of csv: ", len(a))
    else:
        print(file + " .. already clean!")
        
    print()
    print(file + ' ..done!')
    print()
    print()