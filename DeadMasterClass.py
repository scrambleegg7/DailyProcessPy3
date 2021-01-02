# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np

import sys
import codecs
import glob

from MasterDataClass import MasterDataClass

class DeadMasterClass(MasterDataClass):


    def __init__(self,udir,test=False):
        
        super(DeadMasterClass,self).__init__(udir,test)
        
    def loadData(self):

        print("deadsock filename:", self.first)

        #
        # initialization for python 3
        #
        with codecs.open(self.first, "r", "Shift-JIS", "ignore") as file:        
            df_master = pd.read_csv(file)

        df_master.columns = ["drcode","yjcode","type","4","drugname_","housou","standard_","stock","Price", 
                         "LastdayOfEntry","daysSinceLastEntry","LastdayOfOut","daysSinceLastOut","14","wholesale","unit","17","18","expiry"]    
        df_master = df_master.drop(["4","14","17","18"],axis=1)
        #print "column name after dropping unnecessary columns:\n", df_master.columns
        #df_master["standard_"] = df_master.loc[:,"standard_"].str.decode('cp932')
        #df_master["drugname_"] = df_master.loc[:,"drugname_"].str.decode('cp932')
        #df_master["type"] = df_master.loc[:,"type"].str.decode('cp932')
        #df_master["unit"] = df_master.loc[:,"unit"].str.decode('cp932')
        #df_master["wholesale"] = df_master.loc[:,"wholesale"].str.decode('cp932')
        
                
        df_master["newcode"] = df_master.loc[:,"drcode"].astype(str) + df_master.loc[:,"housou"].astype(str)
        #df_master["newcode"] = df_master.loc[:,"newcode"].astype(long)
        
        df_master["drugcode"]= df_master.loc[:,"drugname_"] + df_master.loc[:,"standard_"]

        #        
        #self.df_masterData = df_master.set_index("newcode")
        #
        self.df_masterData = df_master
        
        self.printHeader()
    
    def mergeRackData(self,df_rack):

        df_merge = pd.merge(self.df_masterData,df_rack,on='drugcode',how='left')

        #cols = ["rack","drcode","type","drugname_","housou","standard_","stock","Price", 
        #                 "LastdayOfEntry","daysSinceLastEntry","LastdayOfOut","daysSinceLastOut","wholesale","unit","expiry"]    

        cols = ["rack","type","drugname_","housou","standard_","stock","Price", 
                         "LastdayOfOut","daysSinceLastOut","wholesale","unit","expiry"]    

        
        df_merge = df_merge.loc[:,cols]
        df_merge = df_merge.sort_values(by=["LastdayOfOut"],ascending=False)
            
        return df_merge

