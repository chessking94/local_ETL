Module Program
    Private objm_Process As clsBase

    Sub Main(args As String())
        'TODO: maybe consider using modCommandLine.ParseCommandLineArguments in the future
        If args.Length = 0 Then
            Console.WriteLine("No arguments passed, application terminating")
            Environment.Exit(1)
        ElseIf args.Length > 1 Then
            Console.WriteLine("Too many arguments passed, application terminating")
            Environment.Exit(-1)
        End If

        Dim strl_Process As String = args(0).ToUpper
        Select Case strl_Process
            Case "CHESSGAMEANALYSIS"
                objm_Process = New clsChessGameAnalysis
            Case Else
                Throw New Exception($"Invalid process name '{strl_Process}' or process not currently supported")
        End Select

        objm_Process.Go()
    End Sub
End Module
