[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CargoArgs
)

$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
if (-not (Test-Path $vswhere)) {
    throw 'Visual Studio Installer vswhere.exe was not found.'
}

$vsRoot = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($vsRoot)) {
    throw 'A Visual Studio installation with MSVC C++ tools was not found.'
}

$vsDevCmd = Join-Path $vsRoot 'Common7\Tools\VsDevCmd.bat'
if (-not (Test-Path $vsDevCmd)) {
    throw "VsDevCmd.bat was not found under $vsRoot."
}

$command = @(
    "call `"$vsDevCmd`" -arch=x64 -host_arch=x64 >nul"
    'set CC='
    'set AR='
    'set CXX='
    "cargo $($CargoArgs -join ' ')"
) -join ' && '

cmd.exe /d /s /c $command
exit $LASTEXITCODE
