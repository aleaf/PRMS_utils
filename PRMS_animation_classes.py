"""
Classes for post-processing of PRMS animation data
"""
import os
import pandas as pd
from functools import partial
import datetime as dt
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
        self.header = []
        self.header_row = 0

        self.column_names = None
        self.formats_line = None
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
                self.header.append(line.strip())
                self.header_row+=1
        self.column_names=indata[self.header_row].strip().split(self.delimiter)
        self.formats_line = indata[self.header_row+1]
        
        # read animation file into pandas dataframe
        print "reading {0:s}...".format(self.infile)
        self.df = pd.read_csv(self.infile, sep=self.delimiter, header=self.header_row, skiprows=[self.header_row+1], index_col=0)
        self.df.index = pd.to_datetime(self.df.index, format='%Y-%m-%d:%H:%M:%S')

    def parse_header(self):
        fmt = {}

        def fmt_col(column_width, precision, type, x):
            fmt = '{:{width}.{precision}{type}}\t'.format(x, width=column_width-1,
                                                          precision=precision, type=type)
            return fmt

        for i, line in enumerate(self.header):

            l = line.strip().split(',')
            variable = l[0].strip('# ')

            # kludge! otherwise nhru prints in exponent notation
            if variable == 'nhru':
                type = 'f'
            else:
                type = 'E'

            if 'FIELD_DECIMAL' in line:
                column_width = int(l[-2])
                precision = int(l[-1])
                # use partial to make an individual function to format each column
                # lambda x doesn't work, because of late binding closure
                # (see http://docs.python-guide.org/en/latest/writing/gotchas/)
                fmt[variable] = partial(fmt_col, column_width, precision, type)

            elif 'DATETIME' in line:
                fmt[variable] = ''.format
            else:
                continue
        self.fmts = fmt


    def write_output(self, dataframe, outfile):

        print 'writing {}...'.format(outfile)
        df = dataframe.copy()
        if df.index.name not in df.columns:
            # put index in as a column otherwise won't print properly
            df.insert(0, df.index.name, df.index.values)

        self.parse_header()

        ofp = open(outfile, 'w')

        # write the header; only write entries corresponding to columns in the dataframe
        ofp.write('#\n# Begin DBF\n')
        for c in df.columns:
            headerline = [h for h in self.header if c in h][0] + '\n'
            ofp.write(headerline)
        ofp.write('#\n# End DBF\n#\n')

        # write the column names
        ofp.write('\t'.join(df.columns) + '\n')

        # write the formatting stuff below the header (not even sure what this is)
        # only include the formatting stuff corresponding to columns that are in the output dataframe
        formats_line = self.formats_line.strip().split()
        formats_line = [f for i, f in enumerate(formats_line) if self.column_names[i] in df.columns]
        ofp.write('\t'.join(formats_line) + '\n')

        # make list of output formatters consistent with dataframe columns
        formatters = [self.fmts[c] for c in df.columns]

        # now write the damn dataframe!
        ofp.write(df.to_string(header=False, index=False, formatters=formatters))
        ofp.close()




class hruStatistics:

    def __init__(self, period_files, baseline_file=None):

        self.period_files = period_files
        self.baseline_file = baseline_file
        self.period = None
        self.periods = {}
        if isinstance(period_files, list):
            for pf in period_files:
                self.periods[pf] = AnimationFile(pf)
        else:
            self.period = AnimationFile(period_file)

        if baseline_file is not None:

            self.baseline = AnimationFile(baseline_file)

    def hru_mean(self, dataframe, nyears=None):
        """Computes mean values for each hru, for each column (state variable)

        Parameters
        ----------
        dataframe : dataframe
            Dataframe with datetime index, a column named 'nhru' with PRMS hru number,
            and additional columns of PRMS state variables

        nyears : int (optional)
            Compute mean for last nyears of period.
            (e.g. for neglecting a period of model "spin-up"

        Returns
        -------
        A dataframe of mean values for each hru (rows), for each state variable (columns)
        """
        if nyears is not None:
            startyear = dataframe.index[-1].year - nyears
            df = dataframe.ix[dt.datetime(startyear, 1, 1):].copy()
            print df.index
        else:
            df = dataframe.copy()
        return df.groupby('nhru').mean()

    def hru_mean_pct_diff(self, nyears=None):
        """Computes percent differences in the state variable means for each hru.
        Mean values are computed for the baseline_df and period_df using the hru_mean method.

        Parameters
        ----------
        baseline_df : dataframe
            Dataframe representing a baseline period,
            with a datetime index, a column named 'nhru' with PRMS hru number,
            and additional columns of PRMS state variables

        period_df : dataframe
            Dataframe representing a period to compare with baseline,
            with a datetime index, a column named 'nhru' with PRMS hru number,
            and additional columns of PRMS state variables

        nyears : int (optional)
            Compute mean for last nyears of period.
            (e.g. for neglecting a period of model "spin-up"

        Returns
        -------
        A dataframe of percent differences for each hru (rows), for each state variable (columns)
        """
        bl_mean = self.hru_mean(self.baseline.df, nyears=nyears)
        self.baseline.means = bl_mean

        # if a list of period files was supplied, process all of the dataframes
        if len(self.periods) > 0:
            for pf, period in self.periods.iteritems():
                per_mean = self.hru_mean(period.df, nyears=nyears)
                self.periods[pf].pct_diff = 100 * (per_mean - bl_mean) / bl_mean
                self.periods[pf].means = per_mean

        # otherwise process the single dataframe
        else:
            per_mean = self.hru_mean(self.period.df, nyears=nyears)
            self.period.pct_diff = 100 * (per_mean - bl_mean) / bl_mean
            self.period.means = per_mean

    def write_output(self, outdir):

        if not os.path.isdir(outdir):
            os.makedirs(outdir)

        baseline_outpath = os.path.join(outdir, os.path.split(self.baseline_file)[-1][:-4])
        self.baseline.write_output(self.baseline.means, '{}hru_means.nhru'.format(baseline_outpath))

        if len(self.periods) > 0:
            for pf, period in self.periods.iteritems():
                per_outpath = os.path.join(outdir, os.path.split(pf)[-1][:-4])
                period.write_output(period.means, '{}hru_means.nhru'.format(per_outpath))
                period.write_output(period.pct_diff, '{}hru_pct_diff.nhru'.format(per_outpath))

        else:
            per_outpath = os.path.join(outdir, os.path.split(self.period_files)[-1][:-4])
            self.period.write_output(self.period.means, '{}hru_means.nhru'.format(per_outpath))
            self.period.write_output(self.period.pct_diff, '{}hru_pct_diff.nhru'.format(per_outpath))


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
                output.write(ani_file.format_line)
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