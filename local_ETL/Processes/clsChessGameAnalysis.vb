Imports Microsoft.Data.SqlClient
Imports System.Data
Imports System.IO
Imports Utilities_NetCore

Friend Class clsChessGameAnalysis : Inherits clsBase
    Protected Overrides Function FileType() As enumFileTypes
        Return enumFileTypes.ChessGameAnalysis
    End Function

    Protected Overrides Sub Go_Child()
        For Each strl_File In Directory.GetFiles(ImportDirectory(), $"*{FileExtension}")
            Dim strl_Filename As String = Path.GetFileName(strl_File)
            'TODO: is there any point in implementing a future method to validate the layout?

            'perform core import
            objm_CMD.Parameters.Clear()
            objm_CMD.CommandType = CommandType.StoredProcedure
            objm_CMD.CommandText = "ChessWarehouse.dbo.LoadGameData"
            objm_CMD.Parameters.AddWithValue("@piFileName", strl_Filename)

            Dim objl_Output As New SqlParameter("@poErrorMessage", SqlDbType.NVarChar)
            objl_Output.Direction = ParameterDirection.Output
            objl_Output.Size = 100
            objm_CMD.Parameters.Add(objl_Output)

            Dim objl_Return As New SqlParameter("@ReturnValue", SqlDbType.Int)
            objl_Return.Direction = ParameterDirection.ReturnValue
            objm_CMD.Parameters.Add(objl_Return)

            objm_CMD.ExecuteNonQuery()

            If Not IsDBNull(objl_Output.Value) Then
                Dim strl_LogMessage As String = $"File = '{strl_Filename}' | {objl_Output.Value}"
                modLogging.AddLog(strm_ProgramName, "VB", "clsChessGameAnalysis.Go_Child", eLogLevel.ERROR, strl_LogMessage, objm_LogMethod)
            End If

            'archive file if core import completes successfully
            If Convert.ToInt32(objl_Return.Value) = 0 Then
                ArchiveFile(strl_File)

                Dim strl_LogMessage As String = $"Successfully processed file '{strl_Filename}'"
                modLogging.AddLog(strm_ProgramName, "VB", "clsChessGameAnalysis.Go_Child", eLogLevel.INFO, strl_LogMessage, objm_LogMethod)
            End If
        Next
    End Sub
End Class
