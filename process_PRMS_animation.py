# example script to process multiple PRMS animation files using classes in PRMS_animation_classes

import PRMS_animation_classes as prms

configfile = 'EXAMPLE_process_PRMS_animation.in'

input = prms.Input(configfile)
c=0
for infile in input.input_files:
    c+=1
    print "\n{0} of {1}".format(c,len(input.input_files))
    # dictionary to determine annual aggregation of variables (e.g. whether mean or sum)
    #f = {'nhru':['mean'], 'soil_moist':['mean'], 'recharge':['sum'], 'hru_ppt':['sum'], 'hru_rain':['sum'], 'hru_snow':['sum'], 'tminf':['mean'], 'tmaxf':['mean'], 'potet':['sum'], 'hru_actet':['sum'], 'pkwater_equiv':['max'], 'snowmelt':['sum'], 'hru_streamflow_out':['mean']}

    # read PRMS animation file into object
    indata = prms.AnimationFile(infile)

    # calculate period statistics (and write to output files)
    stats = prms.PeriodStatistics(input.operations)

    stats.Annual(indata, format_line=True)

    stats.Monthly(indata, format_line=True)
    
