"""
Classes for post-processing of PRMS animation data
"""
import os
import numpy as np
import pandas as pd
from functools import partial
import datetime as dt
import csv


def check_finite(dataframe, file, errorfile, exclude_cols=[]):
    """Identify any values in an array that are nans or +/- inf
    """
    nans = False
    df = dataframe.drop(list(set(exclude_cols).intersection(set(dataframe.columns))), axis=1)
    column_sums = np.sum(~np.isfinite(df))
    if column_sums.sum() > 0:
        nans = True
        errorfile.write('{}:\n'.format(file))
        errorfile.write('Number of non-finite by column:\n')
        errorfile.write(column_sums.to_string())
        errorfile.write('\nIndices of non-finite values:\n')
        isnans = np.isnan(df)
        inds = dict([(c, df.ix[isnans[c].values, c].index.tolist()) for c in df.columns])
        for k, v in inds.iteritems():
            if len(v) > 0:
                errorfile.write('{}:'.format(k))
                for i in v:
                    errorfile.write(' {}'.format(i))
                errorfile.write('\n')
        errorfile.write('\n')
    return nans

def compute_timeseries_midpoint(timeseries):
    return timeseries[0] + (timeseries[-1] - timeseries[0])/2

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
                self.header_row += 1
        self.column_names = indata[self.header_row].strip().split(self.delimiter)
        self.timestamp_column = self.column_names[0]
        self.formats_line = indata[self.header_row+1]
        
        # read animation file into pandas dataframe
        print "reading {0:s}...".format(self.infile)
        self.df = pd.read_csv(self.infile, sep=self.delimiter, header=self.header_row, skiprows=[self.header_row+1])
        self.df[self.timestamp_column] = pd.to_datetime(self.df[self.timestamp_column], format='%Y-%m-%d:%H:%M:%S')
        self.df.index = pd.to_datetime(self.df[self.column_names[0]], format='%Y-%m-%d:%H:%M:%S')

    def parse_header(self):
        fmt = {}

        def fmt_col(column_width, precision, type, x):
            if type.lower() == 'e':
                fmt = '{:{width}.{precision}{type}}\t'.format(x, width=column_width-1,
                                                              precision=precision, type=type)
            if type.lower() == 'f':
                fmt = '{:{width}.{precision}{type}}\t'.format(x, width=column_width,
                                                          precision=precision, type=type)
            return fmt

        def fmt_datetime(x):
            fmt = pd.Timestamp(x).strftime('%Y-%m-%d:%H:%M:%S')
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
                fmt[variable] = partial(fmt_datetime)
            else:
                continue
        self.fmts = fmt


    def write_output(self, dataframe, outfile, timestamp=None):

        print 'writing {}...'.format(outfile)
        df = dataframe.copy()
        if df.index.name not in df.columns:
            # put index in as a column otherwise won't print properly
            df.insert(0, df.index.name, df.index.values)
        if timestamp is not None:
            df.insert(0, 'timestamp', [pd.Timestamp(timestamp)] * len(df))

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

    def __init__(self, period_files, baseline_file=None, nyears=None, error_file='hruStatistics_errors.txt'):

        self.period_files = period_files
        self.baseline_file = baseline_file
        self.period = None
        self.periods = {}
        if isinstance(period_files, list):
            for pf in period_files:
                self.periods[pf] = AnimationFile(pf)
        else:
            self.period = AnimationFile(period_files)

        if baseline_file is not None:
            self.baseline = AnimationFile(baseline_file)

        self.nyears = nyears
        self.nans = False
        self.error_file = open(error_file, 'w')

        self.trim_to_last_nyears()

    def trim_to_last_nyears(self):

        for pf, period in self.periods.iteritems():
            if self.nyears is not None:
                startyear = period.df.index[-1].year - self.nyears
                self.periods[pf].df = period.df.ix[dt.datetime(startyear, 1, 1):].copy()
            self.periods[pf].dt_midpoint = compute_timeseries_midpoint(self.periods[pf].df.index)

        if self.period is not None:
            if self.nyears is not None:
                startyear = self.period.df.index[-1].year - self.nyears
                self.period.df = self.period.df.ix[dt.datetime(startyear, 1, 1):].copy()
            self.period.dt_midpoint = compute_timeseries_midpoint(self.period.df.index)

        if self.baseline_file is not None:
            if self.nyears is not None:
                startyear = self.baseline.df.index[-1].year - self.nyears
                self.baseline.df = self.baseline.df.ix[dt.datetime(startyear, 1, 1):].copy()
            self.baseline.dt_midpoint = compute_timeseries_midpoint(self.baseline.df.index)
    '''
    def hru_mean(self, dataframe, timestamp_column=None):
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

        if self.nyears is not None:
            startyear = dataframe.index[-1].year - self.nyears
            df = dataframe.ix[dt.datetime(startyear, 1, 1):].copy()
            print df.index
        else:

        df = dataframe.copy()
        
        dfg = df.groupby('nhru').mean()
        
        # Kludge! to restore datetime column that is dropped by pandas in groupby above
        # a waste of space (and effort), but CIDA's programs require it
        # as add hru index as a column after timestamp
        if timestamp_column is not None:
            dt_midpoint = df.index[0] + (df.index[-1] - df.index[0])/2
            dfg[timestamp_column] = dt_midpoint
            dfg[dfg.index.name] = dfg.index
            cols = list(dfg.columns)
            cols.remove(timestamp_column)
            cols = [timestamp_column, dfg.index.name] + cols
            dfg = dfg[cols]
            
        return dfg
    '''
    def hru_mean_pct_diff(self):
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
        #bl_mean = self.hru_mean(self.baseline.df)
        bl_mean = self.baseline.df.groupby('nhru').mean()
        self.baseline.means = bl_mean

        # if a list of period files was supplied, process all of the dataframes
        if len(self.periods) > 0:
            for pf, period in self.periods.iteritems():
                #per_mean = self.hru_mean(period.df)
                per_mean = period.df.groupby('nhru').mean()
                self.periods[pf].pct_diff = 100 * (per_mean - bl_mean) / bl_mean
                self.periods[pf].means = per_mean
                self.nans = check_finite(self.periods[pf].pct_diff,
                                         '{}\n(in percent differences)'.format(pf), self.error_file,
                                         exclude_cols=period.timestamp_column)

        # otherwise process the single dataframe
        else:
            #per_mean = self.hru_mean(self.period.df)
            per_mean = self.period.df.groupby('nhru').mean()
            self.period.pct_diff = 100 * (per_mean - bl_mean) / bl_mean
            self.period.means = per_mean
            self.nans = check_finite(self.period.pct_diff,
                                     '{}\n(in percent differences)'.format(self.period_files), self.error_file)

        if self.nans:
            print 'Warning, nan values found in percent differences. See error_file.'
        self.error_file.close()

    def write_output(self, outdir):

        for dir in [outdir, outdir + '/hru_means', outdir + '/hru_pct_diff']:
            if not os.path.isdir(dir):
                os.makedirs(dir)

        baseline_outpath = os.path.join(outdir + '/hru_means', os.path.split(self.baseline_file)[-1][:-4])
        self.baseline.write_output(self.baseline.means, '{}hru_means.nhru'.format(baseline_outpath),
                                   timestamp=self.baseline.dt_midpoint)

        if len(self.periods) > 0:
            for pf, period in self.periods.iteritems():
                per_outpath = os.path.join(outdir + '/hru_means', os.path.split(pf)[-1][:-4])
                period.write_output(period.means, '{}hru_means.nhru'.format(per_outpath),
                                    timestamp=period.dt_midpoint)

                period.pct_diff.replace([np.inf, -np.inf], np.nan, inplace=True)
                per_outpath = os.path.join(outdir + '/hru_pct_diff', os.path.split(pf)[-1][:-4])
                period.write_output(period.pct_diff, '{}hru_pct_diff.nhru'.format(per_outpath),
                                    timestamp=period.dt_midpoint)

        else:
            per_outpath = os.path.join(outdir + '/hru_means', os.path.split(self.period_files)[-1][:-4])
            self.period.write_output(self.period.means, '{}hru_means.nhru'.format(per_outpath),
                                     timestamp=self.period.dt_midpoint)

            self.period.pct_diff.replace([np.inf, -np.inf], np.nan, inplace=True)
            per_outpath = os.path.join(outdir + '/hru_pct_diff', os.path.split(self.period_files)[-1][:-4])
            self.period.write_output(self.period.pct_diff, '{}hru_pct_diff.nhru'.format(per_outpath),
                                     timestamp=self.period.dt_midpoint)




class PeriodStatistics:

    def __init__(self, operations):
        self.f = operations

    def Annual(self, ani_file, csv_output=False, ani_output=False):
        # group data by year, using operations specified in config file
        # ani_file: animation file object produced by AnimationFile class
        # format_line (T/F): whether or not to include the format line (in between the header and the data) in the output
        # format_line=True also renames the Date column to "timestamp," consistent with previous processed files
        
        print "calculating annual statistics..."
        df = ani_file.df.copy()

        if df.index[1].month == 10:
            # data are in water years; shift index to 1982
            df = ani_file.df.shift(3, freq='MS')
            
        df_yr_hru = df.groupby([lambda x: x.year, 'nhru']).agg(self.f)

        # flatten column names; preserve original order of variables
        df_yr_hru.columns = df_yr_hru.columns.levels[0]
        self.df_yr = df_yr_hru[[c for c in ani_file.column_names if c in df_yr_hru.columns]]
        
        # put index back for monthly analysis
        #ani_file.df = ani_file.df.shift(-3, freq='MS')

        #remove hrus from index (they are also in the first column)
        self.df_yr.index = [item[0] for item in self.df_yr.index]

        if csv_output:
            outfile = '{0}annual.csv'.format(ani_file.infile.split('\\')[-1].split('ani')[0])
            print 'writing {}...'.format(outfile)
            self.df_yr.to_csv(outfile, index_label='year')

        elif ani_output:
            # reformat to mimic original animation results
            self.df_yr.index = ['{0}-09-30:00:00:00'.format(year) for year in self.df_yr.index]

            # write to output
            outfile = '{0}annual.animation.nhru'.format(ani_file.infile.split('\\')[-1].split('animation')[0])
            print "\twriting annual stats to {0}".format(outfile)
            self.df_yr.to_csv('temp.txt',sep=ani_file.delimiter,float_format='%.6e',index_label='year')

            # if specified, add format line to output file (simply copied from input)
            self.apply_formatting_to_output(ani_file, 'temp.txt', outfile)
        else:
            pass
        
    def Monthly(self, ani_file, csv_output=False, ani_output=False):
        # group data by month, using operations specified in config file
        # ani_file: animation file object produced by AnimationFile class
        # format_line (T/F): whether or not to include the format line (in between the header and the data) in the output
        # format_line=True also renames the Date column to "timestamp," consistent with previous processed files
    
        print "calculating monthly statistics..."
        df_M_hru=ani_file.df.groupby([lambda x: x.month, lambda x: x.year, 'nhru']).agg(self.f)

        # flatten column names; preserve original order of variables
        #df_M_hru.columns=[c[0] for c in df_M_hru.columns]
        #self.df_M=df_M_hru[ani_file.df.columns] # indexing drops the 'nhru' column (already in index)
        df_M_hru.columns = df_M_hru.columns.levels[0]
        self.df_M = df_M_hru[[c for c in ani_file.column_names if c in df_M_hru.columns]]

        # remove hrus from index (they are also in the first column), and reformat to mimic original animation results
        self.df_M.index = [item[0] for item in self.df_M.index]

        # write each month to separate output file
        months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
        if csv_output:
            for i in range(12):
                outfile = '{0}{1}.csv'.format(ani_file.infile.split('\\')[-1].split('ani')[0], months[i])
                print '\rwriting {}...'.format(outfile),
                self.df_M.ix[i+1].to_csv(outfile, index_label='month')

        elif ani_output:
            for i in range(12):
                # get dataframe for month
                df_month = self.df_M.ix[1+1] # selects January group for dataframe

                df_month.index = ['{0}-09-30:00:00:00'.format(year) for year in df_month.index]

                # write each month to output
                outfile = '{0}{1}.animation.nhru'.format(ani_file.infile.split('\\')[-1].split('animation')[0],months[i])
                print "\twriting {0} stats to {1}".format(months[i],outfile)
                df_month.to_csv('temp.txt',sep=ani_file.delimiter,float_format='%.6e',index_label='year')

                # if specified, add format line to output file (simply copied from input)
                self.apply_formatting_to_output(ani_file, 'temp.txt', outfile)
        else:
            pass
                
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