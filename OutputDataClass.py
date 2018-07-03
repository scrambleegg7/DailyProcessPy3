# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np

import sys
import codecs
import glob

from MasterDataClass import MasterDataClass


class OutputDataClass(MasterDataClass):
    
    def __init__(self,udir,test=False,tobuSw=False):
        
        super(OutputDataClass,self).__init__(udir,test)
        
        self.tobuclinic =  u"東武練馬ｸﾘﾆｯｸ"
        #self.packmachine = u"分包機-1-"
        self.packmachine = u"分包機"

        self.df_reindexed = None
        
        self.tobuSw = tobuSw
        
    def loadData(self):
        
        with codecs.open(self.first, "r", "Shift-JIS", "ignore") as file:
            df_out = pd.read_csv(file)

        df_out.columns = ["0","outdate","drcode","housou","standard","5","num","7","8","instname","10","drugname","12","yjcode","14","15","16","17"]    
        df_out = df_out.drop(["0","5","7","8","10","12","14","15","16","17"],axis=1)
        #print "column name after dropping unnecessary columns:\n", df_out.columns
    
        #df_out["standard"] = df_out.ix[:,3].str.decode('cp932')
        #df_out["instname"] = df_out.ix[:,5].str.decode('cp932')
        #df_out["drugname"] = df_out.ix[:,6].str.decode('cp932')
        df_out["outdate"] = pd.to_datetime(df_out["outdate"])    
        #df_out["newcode"]= df_out["drugname"] + df_out["standard"]
        
        df_out["newcode"] = df_out.ix[:,"drcode"].astype(str) + df_out.ix[:,"housou"].astype(str) 
        #df_out["newcode"] = df_out["newcode"].astype(np.long)
        
        df_out["drugcode"]= df_out.ix[:,"drugname"] + df_out.ix[:,"standard"]
        
        #
        #   self.df_masterData = df_out.set_index("newcode")
        #
        self.df_masterData = df_out
        
        self.printHeader()
        
    def groupByDrugName(self,df_othermaster,YYYYMMDD):
        
        
        cols = ["newcode","outdate","drugname","standard","instname","num","drugcode"]
        df_ = self.df_masterData.ix[:,cols]
        

        if self.tobuSw == "only":
            print( df_.head()  )
            #print(  df_["instname"] )
            print(  self.tobuclinic  )
            df_select = df_[df_["instname"] == self.tobuclinic]
        
        elif self.tobuSw == "excl":
            df_select = df_[df_["instname"] != self.tobuclinic]
            #df_ = df_[df_["outdate"] > pd.to_datetime(YYYYMMDD)]
        else:
            df_select = df_
            #df_ = df_[df_["outdate"] > pd.to_datetime(YYYYMMDD)]
        
        # calculate total output number from forward date YYYYMMDD (eg. 20161201 )
        df_forward_data = df_[  df_["outdate"] > pd.to_datetime(YYYYMMDD)  ] 
        sum_ = df_forward_data.groupby('newcode')['num'].sum()        
        df_summary = pd.DataFrame(sum_)
        # move index key into column field and reindex        
        
        df_summary.reset_index(inplace=True)
        #print(df_summary.head())
        #df_summary['newcode'] = df_summary.index

 
        # only group by drugcode = drugname + housou from all output data        
        drugcode_group = [   l[0] for l in df_select.groupby('drugcode')      ]
        df_drugcode_only = pd.DataFrame( drugcode_group,columns=["drugcode"]  )
        
        
        cur_df = pd.merge(df_drugcode_only,df_othermaster,on='drugcode',how='left')    
        #cur_df.to_csv('\\\\EMSCR01\\ReceptyN\\TEXT\\mytest.csv',encoding='cp932',index=False)
        
        # merge with forward data if exists
        df_sum = pd.merge(cur_df,df_summary,on='newcode',how='left')    
        df_sum['num'] = df_sum['num'].fillna(0)
        df_sum['final'] = df_sum['stock'] - df_sum['num']

        
        df_sum.to_csv('\\\\EMSCR01\\ReceptyN\\TEXT\\checkStockData.csv',encoding='cp932',index=False)
        
        self.df_sum = df_sum
        
        return True if len(df_sum) > 0 else False
        
    
    def mergeRackData(self,df_rack):


        df_merge = pd.merge(self.df_sum,df_rack,on='drugcode',how='left')
        
        cols = ["rack","drugname","standard","final"]
        df_merge = df_merge.ix[:,cols]
        df_merge = df_merge.sort_values(by=["rack","drugname"])

        if self.tobuSw == "bu":
            #df_merge = df_merge[  df_merge["rack"] == self.packmachine ] 
            df_merge = df_merge[  df_merge["rack"].str.contains(self.packmachine,na=False) ] 
            

        return df_merge
        
         
        
    def dataSummerize(self):
        
        cols = ["outdate","drugname","standard","instname","num"]
        self.df_masterData = self.df_masterData.ix[:,cols]

        self.df_masterData.reset_index(level=0,inplace=True)
        
        
        # grouped by newcode == drugname + standard        
        grouped = self.df_masterData.groupby("newcode")
        
        #print "-- grouped:", grouped.head()
        index = [gp_keys[0] for gp_keys in grouped.groups.values()]
        print( "--- index ---", index )        
        
        #print "--- reindexed : ", self.df_masterData["newcode"].reindex(index).head()
        #print "--- column names", pd.DataFrame( self.df_masterData["newcode"].reindex(index) ).columns
        
        # reindexed with grouped newcode                
        tempdf = pd.DataFrame( self.df_masterData["newcode"].reindex(index) )
        tempdf = tempdf.sort_values(by="newcode")
#        tempdf = tempdf.set_index("newcode")
        print(  "-- temp df", tempdf.head()   )      
        
        self.df_reindexed = tempdf
        
        
        #self.df_masterData = unique_df.set_index("newcode")



        #self.printHeader()

    def mergeWithOther(self,df_othermaster):
        
        
        cur_df = self.df_reindexed.join(df_othermaster["stock"],how="inner")
        print( "-- merged --" )
        print( cur_df.head() )
        pass