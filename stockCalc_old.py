# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 14:58:35 2015

@author: hiromi
"""
# importing system directory
import sys
import glob
import time

# importing 
import numpy as np
import pandas as pd
from MyDateClass import MyDateClass

start = time.time()

def get_first(iterable,default=None):
    if iterable:
        for item in iterable:
            return item
    return default

def elapsed():
    return time.time() - start

def processRack(d):

    YYYYMMDD = d
    
    udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\棚卸.CSV"
    #print "-- user directory path for wildcard %s:" % udirPath
    targets = glob.glob(udirPath)
    if not targets:
        print "-- Files are no longer existed : ---"
        sys.exit()
    
    targets.reverse()
    
    for target in targets:
        print target

    f = get_first(targets)
    print "first item of output file : %s" % f
    print 'INPUT File Name : %s' % f
    #print '%.3fs: Started: ' % elapsed()
    df_rack = pd.read_csv(f)
    df_rack.columns = ["rack","drugname","0","standard","1","2","3", 
                         "4","5","6","7","8","9","10"]
                        
    df_rack = df_rack.drop(["0","1","2","3","4","5","6","7",
                                "8","9","10"],axis=1)
    df_rack["standard"] = df_rack.loc[:,2].str.decode('cp932')
    df_rack["drugname"] = df_rack.loc[:,1].str.decode('cp932')
    df_rack["rack"] = df_rack.loc[:,0].str.decode('cp932')
    df_rack["newcode"]= df_rack["drugname"] + df_rack["standard"]
    df_rack = df_rack.set_index("newcode")

    df_rack = df_rack.drop_duplicates()
    df_rack = df_rack[pd.notnull(df_rack['rack'])]
    #print df_rack.head()
    
    return df_rack

def processMaster(d):

    YYYYMMDD = d
    
    udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\在庫一覧%s*.CSV" % (YYYYMMDD)
    print "-- user directory path for wildcard %s:" % udirPath
    #f = "\\\\EMSCR01\ReceptyN\TEXT\%s%s.CSV" % (uStr.encode('shift-jis') , YYYYMMDD)  
    targets = glob.glob(udirPath)
    if not targets:
        print "-- Files are no longer existed : ---"
        sys.exit()
    
    targets.reverse()
    
    for target in targets:
        print target

    f = get_first(targets)
    print "first item of output file : %s" % f
    print 'INPUT File Name : %s' % f
    print '%.3fs: Started: ' % elapsed()
    df_master = pd.read_csv(f)
    df_master.columns = ["drcode","yjcode","drugname","3","4","standard","6","stock","8", 
                         "9","10","11","12","13","14","15","16","17","18","19","20","21",
                         "22","23","24","25"]    
    df_master = df_master.drop(["3","4","6","8","9","10","11","12","13","14","15","16","17",
                                "18","19","20","21","22","23","24","25"],axis=1)
    #print "column name after dropping unnecessary columns:\n", df_master.columns
    df_master["standard"] = df_master.loc[:,3].str.decode('cp932')
    df_master["drugname"] = df_master.loc[:,2].str.decode('cp932')
    df_master["newcode"]= df_master["drugname"] + df_master["standard"]
    df_master = df_master.set_index("newcode")
    return df_master    
    
def process(d):
    
    YYYYMMDD = d
    udirPath = u"\\\\EMSCR01\\ReceptyN\\TEXT\\出庫%s*.csv" % (YYYYMMDD)
    print "-- user directory path for wildcard %s:" % udirPath
    #f = "\\\\EMSCR01\ReceptyN\TEXT\%s%s.CSV" % (uStr.encode('shift-jis') , YYYYMMDD)  
    targets = glob.glob(udirPath)
    if not targets:
        print "-- Files are no longer existed : ---"
        sys.exit()
    
    targets.reverse()
    
    for target in targets:
        print target

    f = get_first(targets)
    print "first item of output file : %s" % f
    
    print 'INPUT File Name : %s' % f
    print '%.3fs: Started: ' % elapsed()
    df_out = pd.read_csv(f)
    df_out.columns = ["0","outdate","drcode","hcode","standard","5","num","7","8","instname","10","drugname","12","yjcode","14","15","16","17"]    
    df_out = df_out.drop(["0","5","7","8","10","12","14","15","16","17"],axis=1)
    #print "column name after dropping unnecessary columns:\n", df_out.columns
    
    df_out["standard"] = df_out.loc[:,3].str.decode('cp932')
    df_out["instname"] = df_out.loc[:,5].str.decode('cp932')
    df_out["drugname"] = df_out.loc[:,6].str.decode('cp932')
    df_out["outdate"] = pd.to_datetime(df_out["outdate"])    
    df_out["newcode"]= df_out["drugname"] + df_out["standard"]
    df_out = df_out.set_index("newcode")
    
    return df_out
def main(argv):
    
    print "length of arg:",len(argv)
    for arg in argv:
        print "arg:",arg
		
    tobusw = False
    if len(argv) > 0:
    	if argv[0] == "excl":
	    tobusw = True
	    tobu = u"東武練馬ｸﾘﾆｯｸ"
		
    myDateObj = MyDateClass()
    YYYYMMDD = myDateObj.strYYYYMMDD()
    YYYYMMDD = "20160701"

    stock_ = process(YYYYMMDD)
    master_ = processMaster(YYYYMMDD)
    rack_ = processRack(YYYYMMDD)
      
    cols = ["outdate","drugname","standard","instname","num"]
    df_ = stock_
    df_ = df_.loc[:,cols]

    df_.reset_index(level=0,inplace=True)
    if tobusw == True:
	df_ = df_[df_["instname"] != tobu]
    
    grouped = df_.groupby("newcode")
    index = [gp_keys[0] for gp_keys in grouped.groups.values()]
    unique_df = pd.DataFrame(df_["newcode"].reindex(index)).sort("newcode")
    unique_df = unique_df.set_index("newcode")
    
    print "-- unique df head -- ", unique_df.head()     
    
    cur_df = unique_df.join(master_["stock"],how="inner")
    
    print cur_df.head()
    
    sys.exit()
    
    #
    # details to ship drug forward date
    #
    df_ = df_[df_["outdate"] > pd.to_datetime(YYYYMMDD)]

    sum_ = df_.groupby("newcode")["num"].sum()
    
    df_sum = pd.DataFrame(sum_)
    #print df_sum.head()
    stock_df = cur_df.join(df_sum)
    stock_df = stock_df.join(rack_,how='inner')
    
    stock_df['num'] = stock_df['num'].fillna(0)
    stock_df.loc[:,'final'] = pd.Series(stock_df.loc[:,0] - stock_df.loc[:,1])
    stock_df = stock_df.reset_index(drop=True)
    cols = ["rack","drugname","standard","final"]
    stock_df = stock_df.loc[:,cols]
    stock_df = stock_df.sort(["rack","drugname"])
    
    
    # stock_df.to_csv('\\\\EMSCR01\\ReceptyN\\TEXT\\test.csv',encoding='cp932',index=False)
    
    #print stock_df
    
if __name__ == "__main__":
    main(sys.argv[1:])
