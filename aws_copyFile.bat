echo "-------"
D:
cd ReceptyN
cd Text

chcp 932

REM ====== copy file from D:\receptyN\Text


c:\programdata\anaconda3\scripts\aws s3 sync . s3://recepty-text/ --exclude "*" --include "*.csv" --delete


REM ====== dead stock