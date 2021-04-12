from abc import ABC, abstractmethod
import platform

import myTools.ContentObfuscation as ce
import myTools.ModuleInstaller as mi

try:
    import pandas as pd
except:
    mi.installModule("pandas")
    import pandas as pd

try:
    import pyodbc
except:
    mi.installModule("pyodbc")
    import pyodbc




class DBConnector(ABC):
    """This abstract class, inheriting from the ABC class in abc package, allows to factor all the methods required to manage a DB connection, execute queries and retrieve resultsets in Pandas DataFrame.
    It has one abstract method _selectBestDBDriverAvailable, which needs implemented for each specific derived class. (see MSSQL_DBConnector)"""  
    
    def __init__(self: object, dbserver: str, dbname: str, dbusername: str, \
                 dbpassword: str, DSN: str = None, trustedmode: bool =  False, viewname: str = "", isPasswordObfuscated:bool = True):    
        try:
            #Instance of an ContentObfuscator object
            self._obfuscator: ce.ContentObfuscation = ce.ContentObfuscation()

            self._m_dbserver: str = str(dbserver)

            if(DSN is not None):
                self._m_DSN:str = str(DSN)
            else:
                self._m_DSN:str = None
            
            self._m_dbname: str = str(dbname)
            self._m_dbusername: str = str(dbusername)

            #User Password is obfuscated. The technique used is weak, it barely prevents anything but better than cleartext
            if(isPasswordObfuscated == False):
                self._m_dbpassword: str = str(self._obfuscator.obfuscate(dbpassword))
            else:
                self._m_dbpassword: str = str(dbpassword)

            self._m_trustedmode: bool = bool(trustedmode)

            self._m_viewname: str = str(viewname)

            self._m_isDBConnectionOpen: bool = False
            self._m_conduit:pyodbc.Connection = None
            self._m_dbDriver:str = 'undef'

            self._selectBestDBDriverAvailable()

        except Exception as excp:
            raise excp

    
    @abstractmethod
    def _selectBestDBDriverAvailable(self: object) -> None:
        """
        This "pure virtual method", makes the class abstract and is required to be implemented in children classes.
        The purpose is to implement specific behaviour with respect to identifying the correct drivers for pyodbc and the target RDBMS Server
        """
        pass
    
    
    
    #series of properties to have "getters" and "setters" wherever appropriate

    @property
    def dbServer(self: object)-> str:
        return self._m_dbserver
    @dbServer.setter
    def dbServer(self: object, value:str)-> None:
        self._m_dbserver = str(value)


    @property
    def dbDSN(self: object)-> str:
        return self._m_DSN
    @dbDSN.setter
    def dbDSN(self: object, value:str)-> None:
        self._m_DSN = str(value)


    @property
    def dbName(self: object)-> str:
        return self._m_dbname
    @dbName.setter
    def dbName(self: object, value:str)-> None:
        self._m_dbname = str(value)


    @property
    def dbUserName(self: object)-> str:
        return self._m_dbusername
    @dbUserName.setter
    def dbUserName(self: object, value:str)-> None:
        self._m_dbusername = str(value)


    @property
    def _dbUserPassword(self: object)-> str:
        return self._m_dbpassword
    @_dbUserPassword.setter
    def _dbUserPassword(self: object, value:str)-> None:
        """BEWARE: the setter on password makes the assumption that the password is NOT obfuscated"""
        self._m_dbpassword = str(self._obfuscator.obfuscate(dbpassword))


    @property
    def dbIsTrustedMode(self: object)-> bool:
        return self._m_trustedmode
    @dbIsTrustedMode.setter
    def dbIsTrustedMode(self: object, value:str)-> None:
        self._m_trustedmode = bool(value)


    @property
    def selectedDriver(self: object)-> str:
        return self._m_dbDriver
    @selectedDriver.setter
    def selectedDriver(self: object, value:str)-> None:
        self._m_dbDriver = str(value)


    @property
    def _dbConduit(self: object)-> pyodbc.Connection:
        return self._m_conduit 

    @property
    def IsConnected(self: object)-> bool:
        return self._m_isDBConnectionOpen


   


    def Open(self: object):
        '''Opens database connection with the DBConnector object connection properties depending on the identified OS'''
        if(self.IsConnected == False):
            if(self._dbConduit is None):
                
                try:
                    identifiedOS: str = platform.system()

                    if('windows' in identifiedOS.lower()):

                        if(self.dbDSN is not None and dbDSN != ''):
                                self._m_conduit = \
                                    pyodbc.connect('DSN=' + self.dbDSN + \
                                    ';UID=' + self.dbUserName + \
                                    ';PWD=' + self._obfuscator.deObfuscate(self._dbUserPassword) + ';')

                        if(self.dbIsTrustedMode == False and (self.dbDSN is None or dbDSN == '')):
                            self._m_conduit = \
                                pyodbc.connect('DRIVER=' + self.selectedDriver + ';SERVER=' + self.dbServer + \
                                ';DATABASE=' + self.dbName + ';UID=' + self.dbUserName + \
                                ';PWD=' + self._obfuscator.deObfuscate(self._dbUserPassword) + ';')
                        else:
                            self._m_conduit = \
                                pyodbc.connect('DRIVER=' + self.selectedDriver + ';SERVER=' + self.dbServer + \
                                ';DATABASE=' + self.dbName + ';Trusted_Connection=yes;')

                    elif(('linux' or 'darwin') in identifiedOS.lower()):
                        if(self.dbDSN is None or dbDSN == ''):
                            raise Exception("Missing DSN for Linux / MacOS, cannot create a DB connection")
                        self._m_conduit = \
                                pyodbc.connect('DSN=' + self.dbDSN + \
                                ';UID=' + self.dbUserName + \
                                ';PWD=' + self._obfuscator.deObfuscate(self._dbUserPassword) + ';')
        
                    self._m_isDBConnectionOpen = True
                
                except Exception as excp:
                    raise Exception('Couldn''t connect to the DB').with_traceback(excp.__traceback__)
            else:
                raise Exception('Internal DBConnector object inconsistency - Internal flag says ''Not Connected'' but pyodbc Connector object is not none')



    def Close(self:object)-> None:
        '''Closes database connection and resets the DBConnector object conduit to None'''
        if(self.IsConnected == True):
            if(self._dbConduit is not None):
                try:
                    self._dbConduit.close()

                    self._m_isDBConnectionOpen = False
                    self._m_conduit = None

                except Exception as excp:
                    raise Exception('Couldn''t close the DB connection').with_traceback(excp.__traceback__)
            else:
                raise Exception('Internal DBConnector object inconsistency - Internal flag says ''Connected'' but pyodbc Connector is none')


    Close.__doc__ = """This function closes the conduit connection to the database (if already connected)"""



    def ExecuteQuery_withRS(self: object, query: str)-> pd.DataFrame:
        '''Executes a Data Query Language statement on the database connection with the given query and returns the result set as a pandas dataframe'''
        if(query is not None and self.IsConnected == True):
            if (type(query) is str):
                if(query):
                    try:
                        df:pd.DataFrame = pd.read_sql(query, self._dbConduit)
                        return df
                    except Exception as excp:
                        raise Exception('Couldn''t execute SQL query').with_traceback(excp.__traceback__)
                else:
                    raise Exception('Empty SQL query to be executed')
            else:
                raise Exception('SQL query couldn''t be casted as a string')
        else:
            raise ('SQL query object is None')


    def ExecuteQuery_view(self: object, query: str)-> None:
        '''Executes a Data Definition Language statement on the database connection with the given query and commits changes'''
        if(query is not None and self.IsConnected == True):
            if (type(query) is str):
                if(query):
                    try:
                        cursor = self._dbConduit.cursor()
                        cursor.execute(query) 
                        self._dbConduit.commit()
                    except Exception as excp:
                        raise Exception('Couldn''t execute SQL query').with_traceback(excp.__traceback__)
                else:
                    raise Exception('Empty SQL query to be executed')
            else:
                raise Exception('SQL query couldn''t be casted as a string')
        else:
            raise ('SQL query object is None')