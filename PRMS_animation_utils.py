# -*- coding: utf-8 -*-
"""
Created on Thu Jan 02 15:00:29 2014

@author: aleaf
"""
import os
import pandas as pd
import csv


# object containing information on the PRMS animation file
class AnimationFile:
    
    def __init__(self, infile):
        
        self.delimiter = None 
        self.infile = infile
        self.header_row = 9999
        self.column_names = None
        self.df = pd.DataFrame()
        
        # get header info
        try:
            indata = open(infile).readlines()
        except:
            raise(InputFileError(infile))
            
        # get delimiter
        dialect = csv.Sniffer().sniff(indata[-2])
        self.delimiter= dialect.delimiter
        
        # get header info
        self.header_row=0
        for line in indata:
            if line.split(self.delimiter)[0]=='timestamp':
                break
            else:
                self.header_row+=1
        self.column_names=indata[self.header_row].strip().split(self.delimiter)
        
        # read animation file into pandas dataframe
        print "\nreading {0:s} into pandas dataframe...".format(self.infile)
        self.df = pd.read_csv(self.infile,sep=self.delimiter,header=self.header_row,skiprows=[self.header_row+1],index_col=0)
        self.df.index = pd.to_datetime(self.df.index,format='%Y-%m-%d:%H:%M:%S')

        
class PeriodStatistics:

    def __init__(self,f):
        self.f = f

    def Annual(self, ani_file):
        print "\ncalculating annual statistics..."
        if ani_file.df.index[1].month == 10:
            # data are in water years; shift index to 1982
            ani_file.df = ani_file.df.shift(3,freq='MS')
            
        df_yr_hru=ani_file.df.groupby([lambda x: x.year, 'nhru']).agg(self.f)

        # flatten column names; preserve original order of variables
        df_yr_hru.columns=[c[0] for c in df_yr_hru.columns]
        self.df_yr=df_yr_hru[ani_file.df.columns].copy()
        
        # put index back for monthly analysis
        ani_file.df = ani_file.df.shift(-3,freq='MS')
        
        # remove hrus from index (they are also in the first column), and reformat to mimic original animation results
        datefmt = '%Y-09-30:00:00:00'
        self.df_yr.index = [item[0] for item in self.df_yr.index]
        self.df_yr.index = ['{0}-09-30:00:00:00'.format(year) for year in self.df_yr.index]
        
        # write to output
        outfile = '{0}animation.nhru'.format(ani_file.infile.split('animation')[0])
        print "writing annual stats to {0}".format(outfile)
        self.df_yr.to_csv('test.txt',sep=ani_file.delimiter,float_format='%.6e',index_label='year')
        
        
    def Monthly(self, ani_file):
        print "\ncalculating monthly statistics..."
        df_M_hru=ani_file.df.groupby([lambda x: x.month, lambda x: x.year, 'nhru']).agg(self.f)

        # flatten column names; preserve original order of variables
        df_M_hru.columns=[c[0] for c in df_M_hru.columns]
        self.df_M=df_M_hru[ani_file.df.columns] # indexing drops the 'nhru' column (already in index)

        # write each month to separate output file
        months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
        for i in range(12):
            # get dataframe for month
            df_month = self.df_M.ix[1] # selects January group for dataframe
            
            # remove hrus from index (they are also in the first column), and reformat to mimic original animation results
            datefmt = '%Y-09-30:00:00:00'
            df_month.index = [item[0] for item in df_month.index]
            df_month.index = ['{0}-09-30:00:00:00'.format(year) for year in df_month.index]
            
            outfile = '{0}{1}.animation.nhru'.format(ani_file.infile.split('animation')[0],months[i])
            print "writing {0} stats to {1}".format(months[i],outfile)
            df_month.to_csv(outfile,sep=ani_file.delimiter,float_format='%.6e',index_label='year')
            
        datefmt = '%Y-09-30:00:00:00'
        
class InputFileError(Exception):
    def __init__(self,infile):
        self.infile = infile
    def __str__(self):
        return('\n\nCould not open or parse input file {0}\n'.format(self.infile))