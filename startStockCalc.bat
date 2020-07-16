echo "-------"
rem L:
rem cd /D \\ipython\\DailyprocessPy3

chcp 932

set LOGPATH=l:\\ipython\\DailyprocessPy3\\logs\\batch.log

echo $PATH 

set T=%TIME:~0,8%
set T=%T: =0%
 
echo %DATE% %T% %1 %~2
echo %DATE% %T% %1 %~2 > %LOGPATH% 2>&1


c:\ProgramData\Anaconda3\python.exe l:\\ipython\\DailyprocessPy3\\stockCalc.py >> %LOGPATH% 2>&1

c:\ProgramData\Anaconda3\python.exe l:\\ipython\\DailyprocessPy3\\DeadStock.py >> %LOGPATH% 2>&1


REM pause

REM ====== StockCalculation data loader



