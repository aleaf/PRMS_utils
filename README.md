PRMS_utils
==========

#### Notes on input file

##### Input Path
location of the raw PRMS animation output to process

##### Input Files
list of animation files to process, one per line

##### Operations
<column name>,<operation> (one per line)

operations are limited to those used by numpy/pandas:
(see http://pandas.pydata.org/pandas-docs/dev/basics.html) 
Function	Description
count	Number of non-null observations
sum	Sum of values
mean	Mean of values
mad	Mean absolute deviation
median	Arithmetic median of values
min	Minimum
max	Maximum
mode	Mode
abs	Absolute Value
prod	Product of values
std	Unbiased standard deviation
var	Unbiased variance
skew	Unbiased skewness (3rd moment)
kurt	Unbiased kurtosis (4th moment)
quantile	Sample quantile (value at %)
cumsum	Cumulative sum
cumprod	Cumulative product
cummax	Cumulative maximum
cummin	Cumulative minimum

