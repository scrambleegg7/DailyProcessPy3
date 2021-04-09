# -*- coding: utf-8 -*-

from MyDateClass import MyDateClass
from RackDataClass import RackDataClass
from StockMasterClass import StockMasterClass
from OutputDataClass import OutputDataClass

import pandas as pd

import sys
import os
import shutil
import glob


def process(argv):

    #
    # following process generate all files about 
    # 1. daily stock movement
    # 2. only limited stock movement
    # 3. limited stock movement excluding target client 
    # 4. only Tablets medical drug.

    myDateObj = MyDateClass()
    YYYYMMDD = myDateObj.strYYYYMMDD()
    YYYY = myDateObj.strYYYY()

    #print("length of arg:",len(argv)  )
    #for arg in argv:
    #    print("arg:",arg)

    print("YYYYMMDD", YYYYMMDD)
    # main directory 
    #mainDir = "/home/ec2-user/s3/recepty-text"
    #resultDir = "/home/ec2-user/s3/recepty-text/result"

    mainDir = u"\\\\EMSCR01\\ReceptyN\\TEXT\\"
    resultDir = u"\\\\EMSCR01\\ReceptyN\\TEXT\\result"
    newresultTargetDir = u"C:\\ReceptyN\\TEXT\\result"
    newresultTargetDirD = u"D:\\ReceptyN\\TEXT\\result"
    
    
    wildcard_dailyoutput = os.path.join( resultDir, "daily*.csv"  )
    
    inventorylist = u"棚卸CSV%s*.CSV" % (YYYY)
    #udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\棚卸CSV%s*.CSV" % (YYYY)
    udirPath = os.path.join( mainDir, inventorylist )
    rackCls = RackDataClass(udirPath,test=True)
    
        
    #udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\在庫一覧%s*.CSV" % (YYYYMMDD)
    stocklist =u"在庫一覧%s*.CSV" % (YYYYMMDD)
    udirPath = os.path.join( mainDir, stocklist  )
    stockCls = StockMasterClass(udirPath,test=True)


    dailyoutput = u"出庫%s*.csv" % (YYYYMMDD)
    udirPath = os.path.join( mainDir, dailyoutput  )    
    #udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\出庫%s*.csv" % (YYYYMMDD)
    
    # 1. dialy stock movement
    outputCls = OutputDataClass(udirPath, test=True)
    ret = outputCls.groupByDrugName(stockCls.getStock(),YYYYMMDD)
    if ret:
        df_merge = outputCls.mergeRackData(rackCls.getDrugCodeAndRackNo())
        result_dailyoutput = os.path.join( resultDir, "dailystockmove.csv"  )
        df_merge.to_csv( result_dailyoutput ,encoding='cp932',index=False)

    # 2. dialy stock only movement    
    outputCls = OutputDataClass(udirPath,False,"only")
    ret = outputCls.groupByDrugName(stockCls.getStock(),YYYYMMDD)
    if ret:
        df_merge = outputCls.mergeRackData(rackCls.getDrugCodeAndRackNo())
        result_dailyoutput = os.path.join( resultDir, "dailyOnly.csv"  )
        df_merge.to_csv( result_dailyoutput ,encoding='cp932',index=False)
    
    # 3. dialy stock exclude movement    
    outputCls = OutputDataClass(udirPath,False,"excl")
    ret = outputCls.groupByDrugName(stockCls.getStock(),YYYYMMDD)
    if ret:
        df_merge = outputCls.mergeRackData(rackCls.getDrugCodeAndRackNo())
        result_dailyoutput = os.path.join( resultDir, "dailyExclude.csv"  )
        df_merge.to_csv( result_dailyoutput ,encoding='cp932',index=False)

    # 4. dialy stock tablets movement
    outputCls = OutputDataClass(udirPath,False,"bu")
    ret = outputCls.groupByDrugName(stockCls.getStock(),YYYYMMDD)
    if ret:
        df_merge = outputCls.mergeRackData(rackCls.getDrugCodeAndRackNo())
        result_dailyoutput = os.path.join( resultDir, "dailyTablets.csv"  )
        df_merge.to_csv( result_dailyoutput ,encoding='cp932',index=False)
    

    print("")
    print("")
    files = glob.glob(wildcard_dailyoutput)
    print("All wildcard files under directory", files)
    for file in files:
        print("<<File -- %s -- is copying from %s into %s" % (file, resultDir, newresultTargetDir) )
        shutil.copy(file,newresultTargetDir)

    
def main(argv):
    
    process(argv)




if __name__ == "__main__":
    main(sys.argv[1:])