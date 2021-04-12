import argparse as agp
from getpass import getpass
import os

from myTools import MSSQL_DBConnector as mssql
from myTools import DBConnector as dbc
import myTools.ContentObfuscation as ce
import myTools.ModuleInstaller as mi
import myTools.CLIArgumentParser as cli


try:
    import pandas as pd
except:
    mi.installModule("pandas")
    import pandas as pd

try:
    import pickle
except:
    mi.installModule("pickle")
    import pickle



####### SPLASH SCREEN


def printSplashScreen():
    print("*************************************************************************************************")
    print("\t THIS SCRIPT ALLOWS TO EXTRACT SURVEY DATA FROM THE SAMPLE SEEN IN SQL CLASS")
    print("\t IT REPLICATES THE BEHAVIOUR OF A STORED PROCEDURE & TRIGGER IN A PROGRAMMATIC WAY")
    print("\t COMMAND LINE OPTIONS ARE:")
    print("\t\t -h or --help: print the help content on the console")
    print("*************************************************************************************************\n\n")




####### FILE PERSISTENCE HANDLING 


def pickleDataFrame(persistenceFilePath:str, aDataFrame:pd.DataFrame) -> None:
    '''Saves a pandas dataframe as a pickle file at the specified persistence path'''

    filePathNoExtension = os.path.splitext(persistenceFilePath)[0]

    with open(filePathNoExtension + '.pickle', 'wb') as handle :
        pickle.dump(aDataFrame, handle, protocol=pickle.HIGHEST_PROTOCOL)


def unpickleDataFrame(persistenceFilePath:str) -> pd.DataFrame:
    '''Loads a pandas dataframe from a pickle file at the specified persistence path'''

    filePathNoExtension = os.path.splitext(persistenceFilePath)[0]

    with open(filePathNoExtension + '.pickle', 'rb') as handle :
        pickledSurveyStructure = pickle.load(handle)

    return pickledSurveyStructure


def doesPersistenceFileExist(persistenceFilePath: str)-> bool:
    '''Checks existence of a file with the specified file path'''
    return os.path.exists(persistenceFilePath)


def removeExistingPersistenceFile(persistenceFilePath:str) -> None:
    '''Removes pickle format of a file with the specified file path'''
    filePathNoExtension = os.path.splitext(persistenceFilePath)[0]
    os.remove(filePathNoExtension + '.pickle')



def isPersistenceFileDirectoryWritable(persistenceFilePath: str)-> bool:
    '''Checks if the directory of the specified persistence path is writable'''
    fileDirectoryPath = os.path.dirname(persistenceFilePath)
    return os.access(fileDirectoryPath, os.W_OK)




####### SQL QUERIES RELATED


def getSurveyStructure(connector: mssql.MSSQL_DBConnector) -> pd.DataFrame:
    '''Extracts all rows of the Survey Structure table from the given database connection and returns it as a pandas dataframe'''
    surveyStructResults = None
    surveyStructureQuery:str = 'SELECT * FROM SurveyStructure' 
    surveyStructResults = connector.ExecuteQuery_withRS(surveyStructureQuery)
    return surveyStructResults


def getAllSurveyDataQuery(connector: dbc.DBConnector) -> str:
    '''Returns a string containing the query for pivoting all survey answers data'''
 
    
    #DYNAMIC QUERY BUILDING BLOCKS
    ##define all string templates to use for assembling the dynamic query

    strQueryTemplateForAnswerColumn: str = """COALESCE( 
				( 
					SELECT a.Answer_Value 
					FROM Answer as a 
					WHERE 
						a.UserId = u.UserId 
						AND a.SurveyId = <SURVEY_ID> 
						AND a.QuestionId = <QUESTION_ID> 
				), -1) AS ANS_Q<QUESTION_ID> """ 

    strQueryTemplateForNullColumnn: str = ' NULL AS ANS_Q<QUESTION_ID> '

    strQueryTemplateOuterUnionQuery: str = """ 
			SELECT 
					UserId 
					, <SURVEY_ID> as SurveyId 
					, <DYNAMIC_QUESTION_ANSWERS> 
			FROM 
				[User] as u 
			WHERE EXISTS 
			( 
					SELECT * 
					FROM Answer as a 
					WHERE u.UserId = a.UserId 
					AND a.SurveyId = <SURVEY_ID> 
			) 
	"""

    strCurrentUnionQueryBlock: str = ''

    strFinalQuery: str = ''



    #QUESTIONS QUERY TEMPLATE
    #extract the questions corresponding to each survey

    questionsInSurveyQuery: str = """
            SELECT *
			FROM (
					SELECT
						SurveyId,
						QuestionId,
						1 as InSurvey
					FROM
						SurveyStructure
					WHERE
						SurveyId = <CURRENT_SURVEY_ID>
					UNION
					SELECT 
						<CURRENT_SURVEY_ID> as SurveyId,
						Q.QuestionId,
						0 as InSurvey
					FROM
						Question as Q
					WHERE NOT EXISTS
					(
						SELECT *
						FROM SurveyStructure as S
						WHERE S.SurveyId = <CURRENT_SURVEY_ID> AND S.QuestionId = Q.QuestionId
					)
				) as t
			ORDER BY QuestionId;"""



    #BUILDING DYNAMIC QUERY
    #outer loop: loop over each surveyId in the Survey Table
    #inner loop: loop over the questions in each survey in order to build the answer columns query

    surveyQuery:str = 'SELECT SurveyId FROM Survey ORDER BY SurveyId' 

    surveyQueryDF:pd.DataFrame = connector.ExecuteQuery_withRS(surveyQuery)

    for survey_ID in range(1, len(surveyQueryDF)+1):

        questionsInCurrentSurveyQuery:str = questionsInSurveyQuery.replace('<CURRENT_SURVEY_ID>', str(survey_ID))

        questionsInSurveyRS:pd.DataFrame = connector.ExecuteQuery_withRS(questionsInCurrentSurveyQuery)

        strColumnsQueryPart:str = ''

        for i in range(len(questionsInSurveyRS)):
            
            question_ID = questionsInSurveyRS.loc[i,'QuestionId']
            is_question_in_survey = questionsInSurveyRS.loc[i,'InSurvey']

            if is_question_in_survey:
                strColumnsQueryPart += strQueryTemplateForAnswerColumn.replace('<QUESTION_ID>', str(question_ID))
                strColumnsQueryPart = strColumnsQueryPart.replace('<SURVEY_ID>', str(survey_ID))
            else:
                strColumnsQueryPart += strQueryTemplateForNullColumnn.replace('<QUESTION_ID>', str(question_ID))

            if i < len(questionsInSurveyRS) - 1 :
                strColumnsQueryPart += ' , '

        strQueryOuterUnionQuery = strQueryTemplateOuterUnionQuery.replace('<SURVEY_ID>', str(survey_ID))
        strQueryOuterUnionQuery = strQueryOuterUnionQuery.replace('<DYNAMIC_QUESTION_ANSWERS>', strColumnsQueryPart)

        strFinalQuery += strQueryOuterUnionQuery

        if survey_ID < len(surveyQueryDF) :
            strFinalQuery += ' UNION '
   
    return strFinalQuery



def refreshViewInDB(connector: dbc.DBConnector, baseViewQuery:str, viewName:str)->None:
    '''Creates or refreshes the view table in the database using the specified query '''

    refreshViewQuery = ' CREATE OR ALTER VIEW <VIEW_NAME> AS '.replace('<VIEW_NAME>', viewName) + baseViewQuery

    connector.ExecuteQuery_view(refreshViewQuery)

     

def surveyResultsToDF(connector: dbc.DBConnector, viewName:str)->pd.DataFrame:
    '''Returns all rows from the view table in the database in a pandas dataframe'''

    getViewResultsQuery = ' SELECT * FROM <VIEW_NAME> '.replace('<VIEW_NAME>', viewName)

    return connector.ExecuteQuery_withRS(getViewResultsQuery)



####### MAIN FUNCTION

def main():
    
    cliArguments:dict = None

    printSplashScreen()    

    #take connection arguments from the CLI
    try:
        cliArguments = cli.processCLIArguments()
    except Exception as excp:
        print("Exiting")
        return

    if(cliArguments is not None):

        try:

            #define MSSQL connection with processed CLI arguments
            connector = mssql.MSSQL_DBConnector(DSN = cliArguments["dsn"], dbserver = cliArguments["dbserver"], \
                dbname = cliArguments["dbname"], dbusername = cliArguments["dbusername"], \
                dbpassword = cliArguments["dbuserpassword"], trustedmode = cliArguments["trustedmode"], \
                viewname = cliArguments["viewname"])

            #open MSSQL connection
            connector.Open()

            #extract Survey Structure table data into a pandas dataframe
            surveyStructureDF:pd.DataFrame = getSurveyStructure(connector)

            #check non-existence scenario of a previous persisted Survey Structure table 
            #this means first time running the script on the current environment
            if(doesPersistenceFileExist(cliArguments["persistencefilepath"]) == False):

                #check writability of file path to save the Survey Structure table content
                if(isPersistenceFileDirectoryWritable(cliArguments["persistencefilepath"]) == True):
                    
                    #persist content and confirm operation success
                    pickleDataFrame(cliArguments["persistencefilepath"], surveyStructureDF)
                    print("\nINFO - Content of SurveyResults table pickled in " + cliArguments["persistencefilepath"] + "\n")
                    
                    #create or refresh the view anyway because comparison is not yet possible in this scenario
                    refreshViewInDB(connector, getAllSurveyDataQuery(connector), cliArguments["viewname"])
                    print('INFO - View has been refreshed!') 
            
            #check existence scenario of a previous persisted Survey Structure table to perform comparison
            #compare the existing pickled Survey Structure file with the newly extracted surveyStructureDF      
            else:

                try:

                    #unpickle previously saved Survey Structure
                    unpickledSurveyStructureDF:pd.DataFrame = unpickleDataFrame(cliArguments["persistencefilepath"])

                    #check equality of pandas dataframes and save result into a boolean variable
                    existing_equals_new:bool = surveyStructureDF.equals(unpickledSurveyStructureDF)

                    if existing_equals_new: 
                        print('INFO - Survey Structure hasn''t been modified!') 
                    else :
                        print('INFO - Survey Structure has been modified!') 
                        #create or refresh view because survey structure has been modofied
                        refreshViewInDB(connector, getAllSurveyDataQuery(connector), cliArguments["viewname"])
                        print('INFO - View has been refreshed!') 

                        #remove existing saved Survey Structure table and save the updated one
                        removeExistingPersistenceFile(cliArguments["persistencefilepath"])
                        pickleDataFrame(cliArguments["persistencefilepath"], surveyStructureDF)

                except Exception as e:
                    
                    print('Problem with unpickling process, can''t proceed with comparing previous and current view states, will refresh view by default! \n')
                    #refresh the view in case there's a problem with persistence or comparison process
                    refreshViewInDB(connector, getAllSurveyDataQuery(connector), cliArguments["viewname"])

            
            #save the refreshed view content (updated pivoted survey answers data) to the given results path

            surveyResults:pd.DataFrame = surveyResultsToDF(connector, cliArguments["viewname"])

            try:
                surveyResults.to_csv(cliArguments["resultsfilepath"])
                print("\nINFO - Done! Results exported in " + cliArguments["resultsfilepath"] + "\n")
            except Exception as e:
                raise Exception('Cannot save results to resultsFilePath', e)
          
            #close MSSQL connection
            connector.Close()

        except Exception as excp:
            print(excp)
    else:
        print("Inconsistency: CLI argument dictionary is None. Exiting")
        return



if __name__ == '__main__':
    main()