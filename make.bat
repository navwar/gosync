@echo off

rem # =================================================================
rem #
rem # Work of the U.S. Department of Defense, Defense Digital Service.
rem # Released as open source under the MIT License.  See LICENSE file.
rem #
rem # =================================================================

rem isolate changes to local environment
setlocal

rem update PATH to include local bin folder
PATH=%~dp0bin;%~dp0scripts;%PATH%


rem set common variables for targets

set "USAGE=Usage: %~n0 [bin\gosync.exe|clean|fmt|help|imports|staticcheck|tidy]"

rem if no target, then print usage and exit
if [%1]==[] (
  echo|set /p="%USAGE%"
  exit /B 1
)

if %1%==bin\gox.exe (

  rem create local bin folder if it doesn't exist
  if not exist "%~dp0bin" (
    mkdir %~dp0bin
  )

  go build -o bin/gox.exe github.com/mitchellh/gox

  exit /B 0
)

if %1%==bin\gosync.exe (

  rem create local bin folder if it doesn't exist
  if not exist "%~dp0bin" (
    mkdir %~dp0bin
  )

  go build -o bin/gosync.exe github.com/deptofdefense/gosync/cmd/gosync

  exit /B 0
)

if %1%==build_release (

  if not exist "%~dp0bin\gox.exe" (
      .\make.bat bin\gox.exe
  )

  powershell .\powershell\build-release.ps1

  exit /B 0
)

REM remove bin directory

if %1%==clean (

  if exist %~dp0bin (
    rd /s /q %~dp0bin
  )

  exit /B 0
)

if %1%==fmt (

  go fmt ./cmd/... ./pkg/...

  exit /B 0
)

if %1%==help (
  echo|set /p="%USAGE%"
  exit /B 1
)

if %1%==imports (

  rem create local bin folder if it doesn't exist
  if not exist "%~dp0bin" (
   mkdir %~dp0bin
  )

  if not exist "%~dp0bin\goimports.exe" (
    go build -o bin/goimports.exe golang.org/x/tools/cmd/goimports
  )

  .\bin\goimports.exe -w ^
  -local github.com/gruntwork-io/terratest,github.com/aws/aws-sdk-go,github.com/deptofdefense ^
  -l ^
  .\cmd\gosync ^
  .\pkg\fs ^
  .\pkg\log ^
  .\pkg\server ^
  .\pkg\template ^
  .\pkg\tools

  exit /B 0
)

if %1%==sync_example (

    rem create local bin folder if it doesn't exist
    if not exist "%~dp0bin" (
      mkdir %~dp0bin
    )

    if not exist "%~dp0bin\gosync.exe" (
        .\make.bat bin\gosync.exe
    )

    .\bin\gosync.exe sync ^
    testdata ^
    temp

  exit /B 0
)

if %1%==staticcheck (

  rem create local bin folder if it doesn't exist
  if not exist "%~dp0bin" (
    mkdir %~dp0bin
  )

  if not exist "%~dp0bin\staticcheck.exe" (
    go build -o bin/staticcheck.exe honnef.co/go/tools/cmd/staticcheck
  )

  .\bin\staticcheck.exe -checks all ./test

  exit /B 0
)

if %1%==tidy (

  go mod tidy

  exit /B 0
)

if %1%==vet (

  go vet github.com/deptofdefense/gosync/pkg/...
  go vet github.com/deptofdefense/gosync/cmd/...

  exit /B 0
)

echo|set /p="%USAGE%"
exit /B 1
