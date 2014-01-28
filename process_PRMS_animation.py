# -*- coding: utf-8 -*-
"""
Created on Fri Jan 03 11:31:38 2014

@author: aleaf
"""
import pandas as pd
import PRMS_animation_utils as prms

infile='D:\\ATLData\\PRMS_visualization\\cccma_cgcm3_1.20c3m.1981-2000.animation.nhru'

# dictionary to determine annual aggregation of variables (e.g. whether mean or sum)
f = {'nhru':['mean'], 'soil_moist':['mean'], 'recharge':['sum'], 'hru_ppt':['sum'], 'hru_rain':['sum'], 'hru_snow':['sum'], 'tminf':['mean'], 'tmaxf':['mean'], 'potet':['sum'], 'hru_actet':['sum'], 'pkwater_equiv':['max'], 'snowmelt':['sum'], 'hru_streamflow_out':['mean']}

# read PRMS animation file into object
indata = prms.AnimationFile(infile)

# calculate period statistics (and write to output files)
stats = prms.PeriodStatistics(f)

stats.Annual(indata, format_line=True)

stats.Monthly(indata, format_line=True)
