import platform

from myTools import DBConnector as db
import myTools.ModuleInstaller as mi

try:
    import pyodbc
except:
    mi.installModule("pyodbc")
    import pyodbc


class MSSQL_DBConnector(db.DBConnector):
    """This class inherits from the abstract class _DBConnector and implements 
       _selectBestDBDriverAvailable for a MSSQL server connection"""

    def __init__(self: object, DSN, dbserver: str, dbname: str, dbusername: str, \
                 dbpassword: str, trustedmode: bool =  False, viewname: str = "", \
                 isPasswordObfuscated:bool = True):
        
        super().__init__(DSN = DSN, 
                         dbserver = dbserver, 
                         dbname= dbname, 
                         dbusername = dbusername,
                         dbpassword = dbpassword, 
                         trustedmode = trustedmode, 
                         viewname = viewname, 
                         isPasswordObfuscated = isPasswordObfuscated)

        self._selectBestDBDriverAvailable()




    def _selectBestDBDriverAvailable(self: object) -> None:
        '''Selects best available MSSQL odbc driver in the current system giving priority to latest versions'''

        MSSQL_odbc_drivers = ['ODBC Driver 17 for SQL Server',
                              'ODBC Driver 13.1 for SQL Server',
                              'ODBC Driver 13 for SQL Server',
                              'ODBC Driver 11 for SQL Server',
                              'SQL Server Native Client 11.0',
                              'SQL Server Native Client 10.0',
                              'SQL Native Client',
                              'SQL Server']
              
        getAvailableDrivers:list[str] = pyodbc.drivers()
        
        identifiedOS: str = platform.system()

        if (getAvailableDrivers is not None):
            
            if(len(getAvailableDrivers) > 0):
               
                if('windows' in identifiedOS.lower()):

                    for driver in MSSQL_odbc_drivers:
                        if driver in getAvailableDrivers:
                            self.selectedDriver = driver
                            break

                if(self.selectedDriver == 'undef'):
                    raise Exception('no suitable DB drivers found on the system')

            else:
                raise Exception('pyobdc cannot find any DB drivers installed on the system')
        else:
            raise Exception('pyodbc fails to extract the DB drivers installed on the system')


