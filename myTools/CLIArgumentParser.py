import argparse as agp
from getpass import getpass

import myTools.ContentObfuscation as ce


def processCLIArguments()-> dict:
    '''Takes the database connection arguments from the command line interface and returns them as a dictionary'''
    
    retParametersDictionary:dict = None
    
    dbpassword:str = ''
    obfuscator: ce.ContentObfuscation = ce.ContentObfuscation()

    try:
        argParser:agp.ArgumentParser = agp.ArgumentParser(add_help=True)

        argParser.add_argument("-n", "--DSN", dest="dsn", \
                                action='store', default= None, help="Sets the SQL Server DSN descriptor file - Take precedence over all access parameters", type=str)

        argParser.add_argument("-s", "--server", dest="dbserver", type= str, required= True, help="Database Server : dbserver")
        argParser.add_argument("-d", "--database", dest="dbname", type= str, required= True, help="Database name : dbname")

        #user is put by default to 'sa'
        argParser.add_argument("-u", "--username", dest="dbusername", type= str, default='sa', help="Database Authentication Username : dbusername")

        #take only one connection mode, either through Windows Authentification or using user/password
        groupAuthenticationMechanism = argParser.add_mutually_exclusive_group()
        groupAuthenticationMechanism.add_argument("-p", "--password", dest="dbuserpassword", type= str, action='store', help="Database Authentication Password : dbuserpassword")
        groupAuthenticationMechanism.add_argument("-t", "--trustedmode", dest="trustedmode", type= bool, action='store', help="Set True to use Windows Authentication ")

        argParser.add_argument("-v", "--viewname", dest="viewname", type= str, required= True, help="Database View Name to refresh when there's change in Survey Structure table")
        argParser.add_argument("-f", "--persistencfilepath", dest="persistencefilepath", type= str, required= True, help="Persistence File Path to save the last checked Survey Structure table")
        argParser.add_argument("-r", "--resultsfilepath", dest="resultsfilepath", type= str, required= True, help="Results File Path to save the Survey results in the view")


        argParsingResults = argParser.parse_args()

        #the user is prompted for a password without echoing if neither trusted mode nor password are explicitely specified
        if not argParsingResults.dbuserpassword and not argParsingResults.trustedmode:
            argParsingResults.dbuserpassword = getpass()

        #password is obfuscated in memory
        if argParsingResults.dbuserpassword:
            dbpassword = obfuscator.obfuscate(argParsingResults.dbuserpassword)

        retParametersDictionary = {
                    "dsn" : argParsingResults.dsn,        
                    "dbserver" : argParsingResults.dbserver,
                    "dbname" : argParsingResults.dbname,
                    "dbusername" : argParsingResults.dbusername,
                    "dbuserpassword" : dbpassword,
                    "trustedmode" : argParsingResults.trustedmode,
                    "viewname" : argParsingResults.viewname,
                    "persistencefilepath": argParsingResults.persistencefilepath,
                    "resultsfilepath" : argParsingResults.resultsfilepath
                }

    except Exception as e:
        print("Command Line arguments processing error: " + str(e))

    return retParametersDictionary
