#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 21:44:50 2021

@author: lindsaybraun
"""

import sys
import pandas as pd
import os

# change chunk size to fit the data
chunksize = 10000
# list of input file names from the command line
filenames = sys.argv[1:]
dataframes = []

for f in filenames:
    # list of chunks from a single file
    file_df = []
    # read in csv file by chunks to avoid potential memory issue for larger files
    reader = pd.read_csv(f, chunksize = chunksize, warn_bad_lines = True, error_bad_lines = False)
    for chunk in reader:
        # add chunks to list and then concatenate outside of loop for increased efficiency
        file_df.append(chunk)
    # add to list a dataframe of data from a single file created from chunks with a new column added for the filename
    dataframes.append(pd.concat(file_df).assign(filename = os.path.basename(f)))

# combine all dataframes from each file
combined_files = pd.concat(dataframes, ignore_index = True)
# write combined dataframe as a csv and output file to stdout
combined_files.to_csv(sys.stdout)
