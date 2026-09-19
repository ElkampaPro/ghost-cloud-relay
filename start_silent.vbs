Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "c:\Users\User\Desktop\Epro\ghost-cloud-server"
WshShell.Run """C:\Program Files\nodejs\node.exe"" server.js", 0, False
