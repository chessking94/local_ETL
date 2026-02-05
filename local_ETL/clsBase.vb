Imports Microsoft.Data
Imports System.Data
Imports System.IO
Imports System.Reflection
Imports Utilities_NetCore

Friend MustInherit Class clsBase
#Region "Protected"
    Protected Property strm_ProgramName As String = Assembly.GetExecutingAssembly().GetName().Name  'TODO: should this be moved to Program.vb?
    Protected Property objm_myConfig As New clsConfig
    Protected Property objm_LogMethod As eLogMethod
    Protected Property objm_CMD As SqlClient.SqlCommand
    Protected Property boom_Abort As Boolean = False

    ''' <returns>
    ''' ChessWarehouse.dbo.FileTypes.FileExtension
    ''' </returns>
    Protected ReadOnly Property FileExtension As String
        Get
            'strip out a leading period, only want the characters after the period
            If Left(objm_FileExtension, 1) <> "." Then
                objm_FileExtension = "." & objm_FileExtension
            End If
            Return objm_FileExtension
        End Get
    End Property
    Private Property objm_FileExtension As String

    ''' <summary>
    ''' This Enum should always mirror database table ChessWarehouse.dbo.FileTypes
    ''' </summary>
    Protected Enum enumFileTypes
        'FileType = FileTypeID
        ChessGameAnalysis = 1
        LichessEvaluations = 2
        UnanalyzedChessGames = 3
    End Enum
#End Region

#Region "MustOverride"
    ''' <returns>
    ''' Type of file being processed
    ''' </returns>
    Protected MustOverride Function FileType() As enumFileTypes

    ''' <summary>
    ''' Contains the base functionality for each individual process
    ''' </summary>
    Protected MustOverride Async Function Go_Child() As Task
#End Region

#Region "Overridable"
    ''' <returns>
    ''' Directory to import files from, taken from appsettings.json for "ImportPath_{ProcessName}"
    ''' </returns>
    Protected Overridable Function ImportDirectory() As String
        Return Path.Combine(objm_myConfig.getConfig("fileProcessingRoot"), "Import", Name())
    End Function

    Protected Overridable Function ExportDirectory() As String
        Return Path.Combine(objm_myConfig.getConfig("fileProcessingRoot"), "Export", Name())
    End Function

    ''' <summary>
    ''' Handler for any process-specific actions that need to occur before the actual process runs
    ''' </summary>
    Protected Overridable Sub PreProcessing()

    End Sub

    ''' <summary>
    ''' Handler for any process-specific actions that need to occur after the actual process runs
    ''' </summary>
    Protected Overridable Sub PostProcessing()

    End Sub
#End Region

#Region "Base functionality"
    ''' <returns>
    ''' ChessWarehouse.dbo.FileTypes.FileType
    ''' </returns>
    Protected Function Name() As String
        Return FileType().ToString
    End Function

    ''' <returns>
    ''' ChessWarehouse.dbo.FileTypes.FileTypeID
    ''' </returns>
    Protected Function FileTypeID() As Integer
        Return CInt(FileType())
    End Function

    Protected Sub ArchiveFile(strv_File As String)
        If Not File.Exists(strv_File) Then
            modLogging.AddLog(strm_ProgramName, "VB", "clsBase.ArchiveFile", eLogLevel.ERROR, $"Unable to archive file '{strv_File}', does not exist", objm_LogMethod)
        Else
            Dim strl_Filename = Path.GetFileName(strv_File)
            Dim strl_ArchiveDirectory As String = Path.Combine(ImportDirectory, "Archive")

            Try
                If Not Directory.Exists(strl_ArchiveDirectory) Then Directory.CreateDirectory(strl_ArchiveDirectory)
                File.Move(strv_File, Path.Combine(strl_ArchiveDirectory, strl_Filename))
            Catch ex As Exception
                modLogging.AddLog(strm_ProgramName, "VB", "clsBase.ArchiveFile", eLogLevel.ERROR, $"Unable to archive file '{strv_File}', {ex.Message}", objm_LogMethod)
            End Try
        End If
    End Sub
#End Region

#Region "Privates"
    ''' <summary>
    ''' Handler for required pre-processing elements
    ''' </summary>
    Private Sub Initialize()
#If DEBUG Then
        Dim projectDir As String = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..\\..\\.."))
        Dim connectionString As String = Environment.GetEnvironmentVariable("ConnectionStringDebug")
        objm_LogMethod = eLogMethod.CONSOLE
        Dim configFile As String = Path.Combine(projectDir, "appsettings.Development.json")
#Else
        Dim projectDir As String = Path.GetFullPath(AppDomain.CurrentDomain.BaseDirectory)
        Dim connectionString As String = Environment.GetEnvironmentVariable("ConnectionStringRelease")
        objm_LogMethod = eLogMethod.DATABASE
        Dim configFile As String = Path.Combine(projectDir, "appsettings.json")
#End If
        objm_myConfig.configFile = configFile

        If connectionString Is Nothing Then
            Throw New Exception("Unable to read connection string")
        Else
            objm_CMD = New SqlClient.SqlCommand
            objm_CMD.Connection = modDatabase.Connection(connectionString)
            objm_CMD.CommandTimeout = 14400  '4 hours
        End If

        objm_CMD.Parameters.Clear()
        objm_CMD.CommandType = CommandType.Text
        objm_CMD.CommandText = "SELECT ISNULL(FileExtension, '') AS FileExtension FROM ChessWarehouse.dbo.FileTypes WHERE FileTypeID = @FileTypeID"
        objm_CMD.Parameters.AddWithValue("@FileTypeID", FileTypeID())

        Dim objl_Result As Object = objm_CMD.ExecuteScalar()
        If objl_Result Is Nothing Then
            Throw New Exception($"No record found for FileTypeID = {FileTypeID()}")
        ElseIf String.IsNullOrWhiteSpace(objl_Result) Then
            Throw New Exception($"No extension defined for FileTypeID = {FileTypeID()}")
        Else
            objm_FileExtension = objl_Result
        End If
    End Sub

    ''' <summary>
    ''' Wrapper sub to handle any pre-processing actions for the process
    ''' </summary>
    Private Sub BasePreProcessing()
        'end the process early if there are no applicable files
        'no need to have a try/catch if the directory is not accessible, would be caught by existing exception handling
        Dim objl_Files As String() = Directory.GetFiles(ImportDirectory(), $"*{FileExtension}")
        If objl_Files.Length = 0 Then
            boom_Abort = True
        End If

        If Not boom_Abort Then PreProcessing()
    End Sub

    ''' <summary>
    ''' Wrapper sub to handle any post-processing actions for the process
    ''' </summary>
    Private Sub BasePostProcessing()
        PostProcessing()
    End Sub

    Private Sub Abort()
        'TODO: do I want to do anything else here?
        Exit Sub
    End Sub
#End Region

#Region "Core functionality"
    Friend Sub Go()
        Try
            Initialize()

            BasePreProcessing()
            If boom_Abort Then Abort()

            Go_Child().GetAwaiter().GetResult()

            BasePostProcessing()

        Catch ex As Exception
            modLogging.AddLog(strm_ProgramName, "VB", "clsBase.Go", eLogLevel.CRITICAL, $"Catastrophic ETL error '{ex.Message}': {ex.StackTrace}", objm_LogMethod)

        Finally
            Try
                objm_CMD.Connection.Close()
                objm_CMD.Dispose()

            Catch
                'ok to do nothing

            End Try

        End Try
    End Sub
#End Region
End Class
