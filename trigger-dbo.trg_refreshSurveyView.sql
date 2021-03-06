USE [Survey_A20]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER TRIGGER [dbo].[trg_refreshSurveyView] 
   ON  [dbo].[SurveyStructure] 
   AFTER  INSERT,DELETE,UPDATE
AS 
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @strSQLSurveyData nvarchar(max);
    -- Insert statements for trigger here
	SET @strSQLSurveyData = ' CREATE OR ALTER VIEW vw_AllSurveyData AS ';
	SET @strSQLSurveyData = @strSQLSurveyData
	 + ( SELECT [dbo].[fn_GetAllSurveyDataSQL] () );

	 exec sp_executesql @strSQLSurveyData;
END
