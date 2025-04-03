import Taiwan_Stocks as TS

# If you don't have the MySQL database, just simply set  <------ IMPORTANT MESSAGE
# db_settings = None                                     <------ IMPORTANT MESSAGE
# MySQL_flag = False                                     <------ IMPORTANT MESSAGE
# Fetch_stock_statistics_flag = False                    <------ IMPORTANT MESSAGE


db_settings = { "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
                "password": "19970730",
                "db": "sql_stock",
                "charset": "utf8" }

# 更新資料庫
# stocks = TS.Taiwan_Stocks( db_settings = db_settings, Crawl_flag = True, MySQL_flag = True,
#                            Fetch_stock_statistics_flag = False, Flag_sub_category=False, Flag_twse = True,  timesleep = 20)

# Flag_sub_category 更新細產業


# stocks = TS.Taiwan_Stocks( db_settings = db_settings, Crawl_flag = False, MySQL_flag = True,
#                            Fetch_stock_statistics_flag = False,  timesleep = 20)

#漲跌停家數測試
# stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
#                           Crawl_flag = True, 
#                           MySQL_flag = True,
#                           Fetch_stock_statistics_flag = False, 
#                           Flag_sub_category=False, # 不用一直開著
#                           Flag_twse = False, 
#                           Flag_tpe_stocks = False, 
#                           Flag_tsw_stocks = False,  
#                           Flag_updown = True, 
#                           Flag_tx = False,
#                           Flag_tif = False,  
#                           timesleep = 20)


# 設定開關
switch = '1'

if (switch == '1'):
    # 一般使用參數
    stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
                            Crawl_flag = True, 
                            MySQL_flag = True,
                            Fetch_stock_statistics_flag = False, 
                            Flag_sub_category=False, # 不用一直開著
                            Flag_twse = True, 
                            Flag_tpe_stocks = True, 
                            Flag_tsw_stocks = True,  
                            Flag_updown = True,   # 計算每天漲跌停家數
                            Flag_tx = True,
                            Flag_tif = True, 
                            Flag_pc_ratio = True, 
                            timesleep = 20)

elif (switch == '2'):
    # 只跑大盤數據
    stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
                            Crawl_flag = True, 
                            MySQL_flag = True,
                            Fetch_stock_statistics_flag = False, 
                            Flag_sub_category=False, # 不用一直開著
                            Flag_twse = True, 
                            Flag_tpe_stocks = False, 
                            Flag_tsw_stocks = False,  
                            Flag_updown = False, 
                            Flag_tx = False,
                            Flag_tif = False, 
                            timesleep = 20)  

elif (switch == '3'):
    # 只跑台指跟小外資數據
    stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
                            Crawl_flag = True, 
                            MySQL_flag = True,
                            Fetch_stock_statistics_flag = False, 
                            Flag_sub_category=False, # 不用一直開著
                            Flag_twse = False, 
                            Flag_tpe_stocks = False, 
                            Flag_tsw_stocks = False,  
                            Flag_updown = False,  
                            Flag_tx = True,
                            Flag_tif = True,
                            timesleep = 20)  
    
elif (switch == '4'):
    # 只跑產業類別
    stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
                            Crawl_flag = True, 
                            MySQL_flag = True,
                            Fetch_stock_statistics_flag = False, 
                            Flag_sub_category=True, # 不用一直開著
                            Flag_twse = False, 
                            Flag_tpe_stocks = False, 
                            Flag_tsw_stocks = False,  
                            Flag_updown = False,  
                            Flag_tx = False,
                            Flag_tif = False,
                            timesleep = 20)  
    
elif (switch == '5'):
    # 只跑舊資料
    stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
                            Crawl_flag = True, 
                            MySQL_flag = True,
                            Fetch_stock_statistics_flag = False, 
                            Flag_sub_category=False, # 不用一直開著
                            Flag_twse = False, 
                            Flag_tpe_stocks = True, 
                            Flag_tsw_stocks = True,  
                            Flag_updown = False,   # 計算每天漲跌停家數
                            Flag_tx = False,
                            Flag_tif = False, 
                            timesleep = 20)
    

elif (switch == '6'):
    # 只跑 PUT CALL RATIO
    stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
                            Crawl_flag = True, 
                            MySQL_flag = True,
                            Fetch_stock_statistics_flag = False, 
                            
                            Flag_sub_category=False, # 不用一直開著
                            Flag_twse = False, 
                            Flag_tpe_stocks = False, 
                            Flag_tsw_stocks = False,  
                            Flag_updown = False,   # 計算每天漲跌停家數
                            Flag_tx = False,
                            Flag_tif = False, 
                            Flag_pc_ratio = True, 
                            timesleep = 20)
    
elif (switch == '7'):
    # 只跑 大盤
    stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
                            Crawl_flag = True, 
                            MySQL_flag = True,
                            Fetch_stock_statistics_flag = False, 
                            
                            Flag_sub_category=False, # 不用一直開著
                            Flag_twse = True, 
                            Flag_tpe_stocks = False, 
                            Flag_tsw_stocks = False,  
                            Flag_updown = False,   # 計算每天漲跌停家數
                            Flag_tx = False,
                            Flag_tif = False, 
                            Flag_pc_ratio = False, 
                            timesleep = 20)
elif (switch == '8'):
    # 只跑 每天上漲下跌家數
    stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
                            Crawl_flag = True, 
                            MySQL_flag = True,
                            Fetch_stock_statistics_flag = False, 
                            
                            Flag_sub_category=False, # 不用一直開著
                            Flag_twse = False, 
                            Flag_tpe_stocks = False, 
                            Flag_tsw_stocks = False,  
                            Flag_updown = True,   # 計算每天漲跌停家數
                            Flag_tx = False,
                            Flag_tif = False, 
                            Flag_pc_ratio = False, 
                            timesleep = 20)
    

elif (switch == '9'):
    # insert old data
    stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
                            Crawl_flag = True, 
                            MySQL_flag = True,
                            Fetch_stock_statistics_flag = False, 
                            
                            Flag_sub_category=False, # 不用一直開著
                            Flag_twse = False, 
                            Flag_tpe_stocks = True, 
                            Flag_tsw_stocks = True,  
                            Flag_updown = False,   # 計算每天漲跌停家數
                            Flag_tx = False,
                            Flag_tif = False, 
                            Flag_pc_ratio = False, 
                            timesleep = 20)
    
