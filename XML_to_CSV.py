import pandas as pd 
import xml.etree.ElementTree as et 
import os
df_cols_main = ["filename","ACC_DT","ACCT_DESIG"]
out_df_main = pd.DataFrame(columns = df_cols_main)
files = [f for f in os.listdir('.') if os.path.isfile(f)]
for f in files:
    if f.endswith(".xml"):
        xtree = et.parse(f)
        xroot = xtree.getroot()
        for node in xroot: 
            s_Trade = node.attrib.get("RECORDS")
            s_accrual_dt = node.find("ACCRUAL_DT").text if node.find("s_acc")  is not None else "Node-Missing" 
            s_acct_desig = node.find("ACCT_DESIG").text if node.find("s_desig")  is not None else "Node-Missing" 
            

            out_df_main = out_df_main.append(pd.Series([ f,s_acc, s_desig], 
                                             index = df_cols_main), 
                                   ignore_index = True)
export_csv = out_df_main.to_csv ('Parsed_File.csv', index = None, header=True) 

