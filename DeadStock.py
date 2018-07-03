# -*- coding: utf-8 -*-
from MyDateClass import MyDateClass
from RackDataClass import RackDataClass
from DeadMasterClass import DeadMasterClass
from OutputDataClass import OutputDataClass


import pandas as pd

import sys
import os



def process():

    myDateObj = MyDateClass()
    YYYYMMDD = myDateObj.strYYYYMMDD()
    #YYYYMMDD = '20160630'
    YYYY = myDateObj.strYYYY()

    #udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\棚卸.CSV"
    #rackCls = RackDataClass(udirPath)

    udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\棚卸CSV%s*.CSV" % (YYYY)
    rackCls = RackDataClass(udirPath)


    udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\デッドストックＣＳＶ*.CSV"
    deadCls = DeadMasterClass(udirPath)

    mainDir = u"\\\\EMSCR01\\ReceptyN\\TEXT\\"
    resultDir = u"\\\\EMSCR01\\ReceptyN\\TEXT\\result"

    dead_file = os.path.join( resultDir, "dead.csv" )

    df_merge = deadCls.mergeRackData(rackCls.getDrugCodeAndRackNo())
    df_merge.to_csv(dead_file ,encoding='cp932',index=False)

    print( df_merge.head() )

def main():

    process()




if __name__ == "__main__":
    main()
