"""
Classes for post-processing of PRMS animation data
"""
import os
import pandas as pd
import csv


class Input:
    # this class parses the Input file, which contains information on
    # - path to the raw PRMS output
    # - a list of the PRMS animation files to process
    # - a list of each column in the PRMS animation files and corresponding operation

    def __init__(self, configfile):
    
        self.configfile = configfile
        self.input_files = list()
        self.operations = dict()
        
        configdata = open(configfile).readlines()
        config = False
        inputpath = False
        for line in configdata:
            if 'input path' in line.lower():
                inputpath=True
                continue
            if inputpath:
                input_path = line.strip()
                inputpath=False
            if line.strip().endswith('.nhru'):
                self.input_files.append(os.path.join(input_path, line.strip()))
            if "operations------" in line.lower():
                config = True
            if config:
                if line[0] == "#" or len(line.strip())==0:
                    continue
                else:
                    var = line.strip().split(',')[0]
                    self.operations[var] = [line.strip().split(',')[1]]
    

# object containing information on the PRMS animation file
class AnimationFile:
    
    def __init__(self, infile):
        
        self.delimiter = None 
        self.infile = infile
        self.header_info = []
        self.header_row = 0
        self.column_names = None
        self.formats = None
        self.df = pd.DataFrame()
        
        # get header info
        try:
            indata = open(infile).readlines()[0:100]
        except:
            raise(InputFileError(infile))
            
        # get delimiter
        dialect = csv.Sniffer().sniff(indata[-2])
        self.delimiter= dialect.delimiter
        
        # get header info
        for line in indata:
            if line.split(self.delimiter)[0]=='timestamp':
                break
            else:
                self.header_info.append(line)
                self.header_row+=1
        self.column_names=indata[self.header_row].strip().split(self.delimiter)
        self.formats = indata[self.header_row+1]
        
        # read animation file into pandas dataframe
        print "reading {0:s} into pandas dataframe...".format(self.infile)
        self.df = pd.read_csv(self.infile, sep=self.delimiter, header=self.header_row, skiprows=[self.header_row+1], index_col=0)
        self.df.index = pd.to_datetime(self.df.index, format='%Y-%m-%d:%H:%M:%S')

        
class PeriodStatistics:

    def __init__(self, operations):
        self.f = operations

    def Annual(self, ani_file, format_line=False):
        # group data by year, using operations specified in config file
        # ani_file: animation file object produced by AnimationFile class
        # format_line (T/F): whether or not to include the format line (in between the header and the data) in the output
        # format_line=True also renames the Date column to "timestamp," consistent with previous processed files
        
        print "calculating annual statistics..."
        if ani_file.df.index[1].month == 10:
            # data are in water years; shift index to 1982
            ani_file.df = ani_file.df.shift(3, freq='MS')
            
        df_yr_hru = ani_file.df.groupby([lambda x: x.year, 'nhru']).agg(self.f)

        # flatten column names; preserve original order of variables
        df_yr_hru.columns=[c[0] for c in df_yr_hru.columns]
        self.df_yr=df_yr_hru[ani_file.df.columns].copy()
        
        # put index back for monthly analysis
        ani_file.df = ani_file.df.shift(-3, freq='MS')
        
        # remove hrus from index (they are also in the first column), and reformat to mimic original animation results
        datefmt = '%Y-09-30:00:00:00'
        self.df_yr.index = [item[0] for item in self.df_yr.index]
        self.df_yr.index = ['{0}-09-30:00:00:00'.format(year) for year in self.df_yr.index]
        
        # write to output
        outfile = '{0}annual.animation.nhru'.format(ani_file.infile.split('\\')[-1].split('animation')[0])
        print "\twriting annual stats to {0}".format(outfile)
        self.df_yr.to_csv('temp.txt',sep=ani_file.delimiter,float_format='%.6e',index_label='year')
        
        # if specified, add format line to output file (simply copied from input)
        if format_line:
            self.apply_formatting_to_output(ani_file, 'temp.txt', outfile)
        
    def Monthly(self, ani_file, format_line=False):
        # group data by month, using operations specified in config file
        # ani_file: animation file object produced by AnimationFile class
        # format_line (T/F): whether or not to include the format line (in between the header and the data) in the output
        # format_line=True also renames the Date column to "timestamp," consistent with previous processed files
    
        print "calculating monthly statistics..."
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
            
            # write each month to output
            outfile = '{0}{1}.animation.nhru'.format(ani_file.infile.split('\\')[-1].split('animation')[0],months[i])
            print "\twriting {0} stats to {1}".format(months[i],outfile)
            df_month.to_csv('temp.txt',sep=ani_file.delimiter,float_format='%.6e',index_label='year')
            
            # if specified, add format line to output file (simply copied from input)
            if format_line:
                self.apply_formatting_to_output(ani_file, 'temp.txt', outfile)
                
    def apply_formatting_to_output(self, ani_file, outfile, formatted_outfile):
        # reopen output file, just to add in formatting line! (should only run this method if necessary)
        
        with open(outfile,'r') as input_file:
            with open(formatted_outfile,'w') as output:
                output.write(ani_file.delimiter.join(ani_file.column_names)+'\n')
                input_file.next()
                output.write(ani_file.formats)
                input=True
                while input:
                    try:
                        output.write(input_file.next())
                    except:
                        input=False
                        #break
            output.close()
        
        
class InputFileError(Exception):
    def __init__(self,infile):
        self.infile = infile
    def __str__(self):
        return('\n\nCould not open or parse input file {0}\n'.format(self.infile))