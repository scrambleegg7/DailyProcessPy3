# -*- coding: utf-8 -*-

from MyDateClass import MyDateClass
from RackDataClass import RackDataClass
from StockMasterClass import StockMasterClass
from OutputDataClass import OutputDataClass

import pandas as pd

import sys
import os



def process(argv):

    myDateObj = MyDateClass()
    YYYYMMDD = myDateObj.strYYYYMMDD()
    YYYY = myDateObj.strYYYY()
    #YYYYMMDD = '20160630'


    
    print "length of arg:",len(argv)
    for arg in argv:
        print "arg:",arg


    udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\出庫%s*.csv" % (YYYYMMDD)
    
    if len(argv) > 0:
        if argv[0] == "only" or argv[0] == "excl":
            outputCls = OutputDataClass(udirPath,False,argv[0])
        elif argv[0] == "bu":
            outputCls = OutputDataClass(udirPath,False,argv[0])
        else:
            print "parameter should be only or excl or bu, otherwise blank....."
            sys.exit(0)            
    
    if len(argv) == 0:     
        print "-- directory path", udirPath
        outputCls = OutputDataClass(udirPath)
        
    udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\棚卸CSV%s*.CSV" % (YYYY)
    rackCls = RackDataClass(udirPath)
    
        
    udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\在庫一覧%s*.CSV" % (YYYYMMDD)
    stockCls = StockMasterClass(udirPath)

    ret = outputCls.groupByDrugName(stockCls.getStock(),YYYYMMDD)
    if ret:
        df_merge = outputCls.mergeRackData(rackCls.getDrugCodeAndRackNo())
        df_merge.to_csv('\\\\EMSCR01\\ReceptyN\\TEXT\\test.csv',encoding='cp932',index=False)

    
def main(argv):
    
    process(argv)




if __name__ == "__main__":
    main(sys.argv[1:])