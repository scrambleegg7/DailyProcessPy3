# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

import sys
import codecs
import glob

from MasterDataClass import MasterDataClass

class RackDataClass(MasterDataClass):
    
    def __init__(self,udir,test=False):
        
        super(RackDataClass,self).__init__(udir,test)
        
        
    def loadData(self):
        
            print("-" * 40)
            print(" Rackdata filename", self.first)
            #file = self.first

            #df_rack = pd.read_csv(file)


            with codecs.open(self.first, "r", "Shift-JIS", "ignore") as file:
            #with codecs.open(self.first, "r", "utf-8", "ignore") as file:
                df_rack = pd.read_csv(file)

            df_rack.columns = ["rack","drugname","0","standard","1","2","3", 
                         "4","5","6","7","8","9","10"]
                        
            df_rack = df_rack.drop(["0","1","2","3","4","5","6","7",
                                "8","9","10"],axis=1)
            #df_rack["standard"] = df_rack.ix[:,2].str.decode('cp932')
            #df_rack["drugname"] = df_rack.ix[:,1].str.decode('cp932')
            #df_rack["rack"] = df_rack.ix[:,0].str.decode('cp932')
            df_rack["drugcode"]= df_rack.ix[:,"drugname"] + df_rack.ix[:,"standard"]
            
            #df_rack = df_rack.set_index("newcode")
             
                   
            df_rack = df_rack.drop_duplicates()
            self.df_masterData = df_rack[pd.notnull(df_rack['rack'])]
            
            
            
            self.printHeader()
            
    def getDrugCodeAndRackNo(self):
        cols = ["drugcode","rack","drugname","standard"]
        return self.df_masterData.ix[:,cols]


