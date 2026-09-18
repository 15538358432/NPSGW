@echo off
setlocal
cd /d "%~dp0"

set GOROOT=F:\Go
set PATH=%GOROOT%\bin;%PATH%

set GOPROXY=https://goproxy.cn,direct
set CGO_ENABLED=0

set AMD_OUT=nps_amd
set ARM_OUT=nps_arm

echo ====================================================
echo   NPS Service - Build ALL (AMD64 + ARM64)
echo ====================================================
echo.

echo ==^> go vet...
go vet ./...
if errorlevel 1 (
    echo [ERROR] go vet failed, fix code first
    exit /b 1
)

echo.
echo ==^> Building Linux AMD64 (x86_64) ...
set GOOS=linux
set GOARCH=amd64
go build -trimpath -ldflags="-s -w" -o %AMD_OUT% .
if errorlevel 1 (
    echo [ERROR] AMD64 build failed
    exit /b 1
)
echo     OK  -^> %cd%\%AMD_OUT%

echo.
echo ==^> Building Linux ARM64 (aarch64) ...
set GOOS=linux
set GOARCH=arm64
go build -trimpath -ldflags="-s -w" -o %ARM_OUT% .
if errorlevel 1 (
    echo [ERROR] ARM64 build failed
    exit /b 1
)
echo     OK  -^> %cd%\%ARM_OUT%

set GOOS=
set GOARCH=

echo.
echo ====================================================
echo   Build Done:
echo     - %AMD_OUT%    (x86_64 Linux)
echo     - %ARM_OUT%    (ARM64 Linux)
echo ====================================================
echo.
echo Deploy / Update:
echo   AMD server:  scp %AMD_OUT% user@x86-host:/opt/pcs/nps/
echo   ARM server:  scp %ARM_OUT% user@arm-host:/opt/pcs/nps/
echo   On server:   chmod +x ^<binary^>  ^&^&  systemctl restart nps
endlocal