@echo off
set SCRIPT_DIR=%~dp0
"%JAVA_HOME%\bin\java" -Dio.netty.tryReflectionSetAccessible=false -Dio.netty.noUnsafe=true -Dorg.apache.logging.log4j.simplelog.StatusLogger.level=OFF -cp "%SCRIPT_DIR%\..\..\search-guard-ssl\*;%SCRIPT_DIR%\..\deps\*;%SCRIPT_DIR%\..\*;%SCRIPT_DIR%\..\..\..\lib\*" com.floragunn.searchguard.tools.SearchGuardAdmin %*
