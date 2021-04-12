# fresh-pivoted-survey-answers
This is a project to verify one's ability to combine clean software engineering using Python for implementation and advanced data wrangling with SQL and T-SQL (SQL Server).

## Scope Statement
The project aims to enforce an **“always fresh” data policy** in a database view which consists of survey answers data in usable format for analysis. There are 2 different scenarios to implement; a scenario in which we can directly create a stored function and trigger to update the view whenever changes have been made to the survey data, and another scanerio where we are restricted and can only have programmatic access from outside the database. In the latter, we'll have to implement a 'degraded' version of a trigger which updates the view whenever the python script is run.

The database has the following diagram:
<img src="https://user-images.githubusercontent.com/54726923/114434722-9d9a5f80-9bc3-11eb-853f-afb76b3e1f71.jpg" width="700" height="500">


### Scenario 1 : we have privileges for creating stored functions and triggers in the database

In this scenario, we are can build the following design:
- A stored function *dbo.fn_GetAllSurveyDataSQL()* which generates and returns a dynamic SQL query string for extracting the pivoted survey answer data;
- A trigger *dbo.trg_refreshSurveyView* that fires on INSERT, DELETE and UPDATE upon the table *dbo.SurveyStructure*, in which case it executes a "CREATE OR ALTER VIEW vw_AllSurveyData AS" + the string returned by *dbo.fn_GetAllSurveyDataSQL*.

With this design, we have enforced an “always fresh” data policy in the view *vw_AllSurveyData*. this solution is “ideal” as it respects the principle of data locality. But it 
requires to have privileges for creating stored procedures/functions and triggers. If the former may be rare, the latter is often heavily restricted. Which is why there's a need to explore the second scenario.

### Scenario 2 : we don't have privileges for creating stored functions and triggers in the database
We are now in a scenario where the only databases operations allowed are:
1. to select data from tables.
2. to create/alter views.

In this case, we can use **programmatic access** to the database server via an ODBC library and proceed as follow:
1. Gracefully handle the connection to the database server, taking CLI arguments and obfuscating sensitive information.
2. Replicate the algorithm of the *dbo.fn_GetAllSurveyDataSQL* stored function.
3. Replicate the algorithm of the trigger *dbo.trg_refreshSurveyView* for creating/altering the view vw_AllSurveyData whenever applicable.
4. Extract the “always-fresh” pivoted survey data, in a CSV file, adequately named.

For achieving (3) above, a persistence component (Pickle, CSV, XML, JSON, etc.), storing the last known surveys’ structures should be in place. It is not acceptable to just recreate the view every time since **the Python code replacing the trigger behaviour must be as close as it can be, from “outside” the database.**

