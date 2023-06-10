#!/usr/bin/env python
"""
This program takes 3 arguments from the command line, Format(default csv), Seperator(default ',') and Number of Records.  The program
then takes these arguments to generated required data file.

Project: Random Data Generator
Purpose: To generate a data file in JSON or any other format having N number of records separated by any delimter like TAB, PIPE, COMMA etc.
Date: 15-Feb-2018

Inputs:
- format: csv,tsv,psv or json
- seperator: comma, tab, pipe or na(for json)
- records: Number of records to be generated
Outputs:
- /tmp/random_data.{format} file
"""

import os, random, argparse
import datetime
from dateutil.relativedelta import relativedelta
import struct, socket


def gen_data(**kwargs):

        ## assigning variables to arguments
        seperator = kwargs["seperator"]
        formatting = kwargs["formatting"]
        records = kwargs["records"]

        print "\nINFO: Removing Old /tmp/random_data."+formatting

        os.system("rm -rf /tmp/random_data."+formatting)

        print "\nINFO: Creating/Opening New /tmp/random_data."+formatting

        file = open("/tmp/random_data."+formatting,"w")

        ## Generating Header

        if ( formatting == "csv" ) or ( formatting == "psv" ) or ( formatting == "tsv" ):

                header = "fname{seperator}lname{seperator}principal{seperator}emailid{seperator}mobile{seperator}passport_make{seperator}passport_expire{seperator}ipaddress".format(seperator=seperator)
                file.write(header)
                file.write("\n")

        print "\nINFO: Generating Random Data and Writing to /tmp/random_data." + formatting

        for i in range(records):

                ## Creating Random Data

                fnames = ["James","John","Robert","Michael","William","David","Richard","Joseph","Thomas","Charles","Christopher","Daniel","Matthew","Anthony","Donald","Mark","Paul","Steven","Andrew","Kenneth","George","Joshua","Kevin","Brian","Edward","Ronald","Timothy","Jason","Jeffrey","Ryan","Gary","Jacob","Nicholas","Eric","Stephen","Jonathan","Larry","Justin","Scott","Frank","Brandon","Raymond","Gregory","Benjamin","Samuel","Patrick","Alexander","Jack","Dennis","Jerry","Tyler","Aaron","Henry","Douglas","Jose","Peter","Adam","Zachary","Nathan","Walter","Harold","Kyle","Carl","Arthur","Gerald","Roger","Keith","Jeremy","Terry","Lawrence","Sean","Christian","Albert","Joe","Ethan","Austin","Jesse","Willie","Billy","Bryan","Bruce","Jordan","Ralph","Roy","Noah","Dylan","Eugene","Wayne","Alan","Juan","Louis","Russell","Gabriel","Randy","Philip","Harry","Vincent","Bobby","Johnny","Logan","naresh","ravi","Bhanu","akash","jane","gaurav","sailesh","tom","Andrea","Steve","Kris","Virender","Jason","stephen","Daemon","Elena","manu","nimisha","Bruce","michael","Akshay"]

                lnames = ["mith","ohnson","illiams","ones","rown","avis","iller","ilson","oore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins","Stewart","Sanchez","Morris","Rogers","Reed","Cook","Morgan","Bell","Murphy","Bailey","Rivera","Cooper","Richardson","Cox","Howard","Ward","Torres","Peterson","Gray","Ramirez","James","Watson","Brooks","Kelly","Sanders","Price","Bennett","Wood","Barnes","Ross","Henderson","Coleman","Jenkins","Perry","Powell","Long","jangra","verma","sharma","weign","nain","devgun","gaundar","dagarin","thomas","ramayna","bourne","salvator","gilbert","beniwal","kumar","khanna","khaneja","singh","bansal","gupta","kaushik"]

                emails = ["@gmail.com","@@yahoo.com","@hotmail.com","@aol.com","@hotmail.co.uk","@rediffmail.com","@ymail.com","@outlook.com","@@gmail.com","hotmail.com","@hotmail.com","@bnz.co.nz","@nbc.com","@yahoo.com","#gmail.com","@hcl.com","@#tcs.com","@tcs.com","##hcl.com"]

                fname=random.choice(fnames)
                lname=random.choice(lnames)
                email=random.choice(emails)
                ipaddress = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
                mobile = random.randint(9800000000,9899999999)
                passport_expiry_date = (datetime.datetime.now() + datetime.timedelta(random.randint(1,100)*365/12))
                passport_make_date = (passport_expiry_date - relativedelta(years=10))

                if i%5 == 0:
                        mobile = "9"+str(mobile)

                if ( formatting == "csv" ) or ( formatting == "psv" ) or ( formatting == "tsv" ) :

                        csv_data = """{fname}{seperator}{lname}{seperator}{fname}@EXAMPLE.COM{seperator}{fname}_{lname}{email}{seperator}{mobile}{seperator}{passport_make_date}{seperator}{passport_expiry_date}{seperator}{ipaddress}""".format(fname=fname,lname=lname,email=email,mobile=mobile,passport_expiry_date=passport_expiry_date,passport_make_date=passport_make_date,ipaddress=ipaddress,seperator=seperator)

                        file.write(csv_data)

                        file.write("\n")

                if formatting == "json":

                        json_data='{"fname" : "%s","lname" : "%s","email" : "%s_%s%s","principal" : "%s@EXAMPLE.COM","passport_make_date" : "%s","passport_expiry_date" : "%s","ipaddress" : "%s" , "mobile" : "%s"}' %(fname,lname, fname, lname,email, fname, passport_make_date, passport_expiry_date, ipaddress, mobile)
                        file.write(json_data)
                        file.write("\n")


        print "\nINFO: Data Written Successfully to /tmp/random_data.%s !!" %(formatting)

        file.close()

        #print "\nINFO: Below are some sample Rows \n"
        #os.system("head -5 /tmp/random_data."+formatting)
        print "\n"


if __name__ == "__main__":

        print " PROJECT: \t Random Data Generator \n AUTHOR: \t Naresh Jangra \n CREATED: \t 15th Feb, 2018 \n PURPOSE: \t To generate a data file in JSON or any other format having N number of records separated by any delimter like TAB, PIPE, COMMA etc."
        print "\n"
        parser = argparse.ArgumentParser()
        parser.add_argument("-f", "--format", default="csv", help="Format of Data to be Generated")
        parser.add_argument("records", help="Number of Records to be Generated", type=int)
        parser.add_argument("-s" , "--seperator", default="," , help="Field Delimiter")

        args = parser.parse_args()

        assert args.format in ['csv','tsv','psv','json'], "Invalid Format: %z" %args.format

        print "INFO: Validating Arguments "

        if ( args.format == 'csv') or ( args.format == 'tsv') or ( args.format == 'psv') :

                print "\nINFO: Calling gen_data() to Create %s File /tmp/random_data.%s having %s '%s' Delimited Records " %(args.format,args.format,args.records,args.seperator)

                gen_data(formatting=args.format,records=args.records,seperator=args.seperator)

        if ( args.format == 'json'):

                print "\nINFO: Calling gen_data() to Create JSON File /tmp/random_data.%s having %s Records " %(args.format,args.records)

                gen_data(formatting=args.format,seperator="|",records=args.records)
