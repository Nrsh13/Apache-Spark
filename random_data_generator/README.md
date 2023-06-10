# Random Data Generator

## Purpose

To generate a data file in JSON or any other format having N number of records separated by any delimter like TAB, PIPE, COMMA etc.

## Usage
```
[root@apache-spark ]$ python random_data_generator.py -h

usage: random_data_generator.py [-h] [-f FORMAT] [-s SEPERATOR] records

positional arguments:
  records               Number of Records to be Generated

optional arguments:
  -h, --help            show this help message and exit
  
  -f FORMAT, --format FORMAT
                        Format of Data to be Generated
                        
  -s SEPERATOR, --seperator SEPERATOR
                        Field Delimiter
```
## Example

### Generating json data file having 10 Records
```
[root@apache-spark]$ python random_data_generator.py -f json 10

INFO: Validating Arguments

INFO: Calling gen_data() to Create JSON File /tmp/random_data.json having 10 Records

INFO: Removing Old /tmp/random_data.json

INFO: Creating/Opening New /tmp/random_data.json

INFO: Generating Random Data and Writing to /tmp/random_data.json

INFO: Data Written Successfully to /tmp/random_data.json !!
```

### Generating pipe separated data file having 10 Records
```
[root@apache-spark]$ python random_data_generator.py -f psv -s "|" 10

INFO: Validating Arguments

INFO: Calling gen_data() to Create psv File /tmp/random_data.psv having 10 '|' Delimited Records

INFO: Removing Old /tmp/random_data.psv

INFO: Creating/Opening New /tmp/random_data.psv

INFO: Generating Random Data and Writing to /tmp/random_data.psv

INFO: Data Written Successfully to /tmp/random_data.psv !!
```

### Generating tab separated data file having 10 Records
```
[root@apache-spark]$ python random_data_generator.py -f tsv -s $'\t' 10

INFO: Validating Arguments

INFO: Calling gen_data() to Create tsv File /tmp/random_data.tsv having 10 '    ' Delimited Records

INFO: Removing Old /tmp/random_data.tsv

INFO: Creating/Opening New /tmp/random_data.tsv

INFO: Generating Random Data and Writing to /tmp/random_data.tsv

INFO: Data Written Successfully to /tmp/random_data.tsv !!
```

## Contact

nrsh13@gmail.com

