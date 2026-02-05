Imports System.Data
Imports System.IO
Imports System.Text
Imports Microsoft.Data.SqlClient
Imports RabbitMQ.Client
Imports Utilities_NetCore

Friend Class clsUnanalyzedChessGames : Inherits clsBase
    Protected Overrides Function FileType() As enumFileTypes
        Return enumFileTypes.UnanalyzedChessGames
    End Function

    Protected Overrides Async Function Go_Child() As Task
        For Each strl_File In Directory.GetFiles(ImportDirectory(), $"*{FileExtension}")
            Dim strl_Filename As String = Path.GetFileName(strl_File)
            'TODO: is there any point in implementing a future method to validate the layout?

            'perform core import
            objm_CMD.Parameters.Clear()
            objm_CMD.CommandType = CommandType.StoredProcedure
            objm_CMD.CommandText = "ChessWarehouse.dbo.LoadUnanalyzedGameData"
            objm_CMD.Parameters.AddWithValue("@piFileName", strl_Filename)

            Dim objl_Output As New SqlParameter("@poErrorMessage", SqlDbType.NVarChar)
            objl_Output.Direction = ParameterDirection.Output
            objl_Output.Size = -1
            objm_CMD.Parameters.Add(objl_Output)

            Dim objl_Return As New SqlParameter("@ReturnValue", SqlDbType.Int)
            objl_Return.Direction = ParameterDirection.ReturnValue
            objm_CMD.Parameters.Add(objl_Return)

            objm_CMD.ExecuteNonQuery()

            If Not IsDBNull(objl_Output.Value) Then
                Dim strl_LogMessage As String = $"File = '{strl_Filename}' | {objl_Output.Value}"
                modLogging.AddLog(strm_ProgramName, "VB", "clsUnanalyzedChessGames.Go_Child", eLogLevel.ERROR, strl_LogMessage, objm_LogMethod)
            End If

            'archive file if core import completes successfully
            If Convert.ToInt32(objl_Return.Value) >= 0 Then
                ArchiveFile(strl_File)

                Await QueueMessages(Convert.ToInt32(objl_Return.Value))  'depending on how much I use a message queue in the future, I may want to generalize this some and move it to Utilities_NetCore

                Dim strl_LogMessage As String = $"Successfully processed file '{strl_Filename}'"
                modLogging.AddLog(strm_ProgramName, "VB", "clsUnanalyzedChessGames.Go_Child", eLogLevel.INFO, strl_LogMessage, objm_LogMethod)
            Else
                'process failed, dump the contents of stage.Games and stage.Moves to text files
                Dim gameErrorName As String = $"{Path.GetFileNameWithoutExtension(strl_Filename)}_GameErrors_{DateTime.Now:yyyyMMddHHmmss}.txt"
                objm_CMD.Parameters.Clear()
                objm_CMD.CommandType = CommandType.Text
                objm_CMD.CommandText = "SELECT * FROM ChessWarehouse.stage.Games WHERE Errors IS NOT NULL"

                With objm_CMD.ExecuteReader
                    If .HasRows Then
                        Using writer As New StreamWriter(Path.Combine(ExportDirectory(), gameErrorName), False)
                            'headers
                            For i As Integer = 0 To .FieldCount - 1
                                writer.Write(.GetName(i))
                                If i < .FieldCount - 1 Then writer.Write(vbTab)
                            Next
                            writer.WriteLine()

                            'data
                            While .Read()
                                For i As Integer = 0 To .FieldCount - 1
                                    Dim fieldValue As String = If(.IsDBNull(i), "", .GetValue(i).ToString())
                                    writer.Write(fieldValue)
                                    If i < .FieldCount - 1 Then writer.Write(vbTab)
                                Next
                                writer.WriteLine()
                            End While
                        End Using
                    End If
                    .Close()
                End With

                Dim moveErrorName As String = $"{Path.GetFileNameWithoutExtension(strl_Filename)}_MoveErrors_{DateTime.Now:yyyyMMddHHmmss}.txt"
                objm_CMD.Parameters.Clear()
                objm_CMD.CommandType = CommandType.Text
                objm_CMD.CommandText = "SELECT * FROM ChessWarehouse.stage.Moves WHERE Errors IS NOT NULL"

                With objm_CMD.ExecuteReader
                    If .HasRows Then
                        Using writer As New StreamWriter(Path.Combine(ExportDirectory(), moveErrorName), False)
                            'headers
                            For i As Integer = 0 To .FieldCount - 1
                                writer.Write(.GetName(i))
                                If i < .FieldCount - 1 Then writer.Write(vbTab)
                            Next
                            writer.WriteLine()

                            'data
                            While .Read()
                                For i As Integer = 0 To .FieldCount - 1
                                    Dim fieldValue As String = If(.IsDBNull(i), "", .GetValue(i).ToString())
                                    writer.Write(fieldValue)
                                    If i < .FieldCount - 1 Then writer.Write(vbTab)
                                Next
                                writer.WriteLine()
                            End While
                        End Using
                    End If
                    .Close()
                End With
            End If
        Next
    End Function

    Private Async Function QueueMessages(intv_FileID As Integer) As Task
        Dim factory As New ConnectionFactory() With {
            .HostName = objm_myConfig.getConfig("rabbitmqHost"),
            .UserName = objm_myConfig.getConfig("rabbitmqUser"),
            .Password = objm_myConfig.getConfig("rabbitmqPass")
        }

        Dim connection As IConnection = Await factory.CreateConnectionAsync()
        Dim channel As IChannel = Await connection.CreateChannelAsync()
        Dim exchange As String = objm_myConfig.getConfig("rabbitmqExchange-Unanalyzed")  'can get away without using an exchange, but best practice is to use one
        Dim route As String = objm_myConfig.getConfig("rabbitmqRoute-Unanalyzed")

        objm_CMD.Parameters.Clear()
        objm_CMD.CommandType = CommandType.Text
        objm_CMD.CommandText = "SELECT GameID FROM ChessWarehouse.lake.Games WHERE FileID = @FileID"
        objm_CMD.Parameters.AddWithValue("@FileID", intv_FileID)
        With objm_CMD.ExecuteReader
            While .Read()
                Try
                    Dim objl_Message As New Dictionary(Of String, Integer) From {
                        {"GameID", .GetInt32(0)}
                    }
                    Dim strl_Json As String = Json.JsonSerializer.Serialize(objl_Message)

                    Await channel.BasicPublishAsync(exchange, route, Encoding.UTF8.GetBytes(strl_Json))

                Catch ex As Exception
                    Console.WriteLine($"Error queuing message: {ex.Message}")  'TODO: proper exception handling

                End Try
            End While
            .Close()
        End With

        connection.Dispose()
    End Function
End Class
