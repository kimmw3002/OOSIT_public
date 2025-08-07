Set objFSO = CreateObject("Scripting.FileSystemObject")
strPath = objFSO.GetParentFolderName(WScript.ScriptFullName)
Set WshShell = CreateObject("WScript.Shell") 
WshShell.Run chr(34) & strPath & "\marketwatch_scheduler_windows.bat" & Chr(34) & " AUTO-STARTED", 0
Set WshShell = Nothing
