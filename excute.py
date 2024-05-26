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
#                            Fetch_stock_statistics_flag = False, Flag_sub_category=False, Flag_twse = True,  timesleep = 5)

# Flag_sub_category 更新細產業


# stocks = TS.Taiwan_Stocks( db_settings = db_settings, Crawl_flag = False, MySQL_flag = True,
#                            Fetch_stock_statistics_flag = False,  timesleep = 5)


stocks = TS.Taiwan_Stocks( db_settings = db_settings, 
                          Crawl_flag = True, 
                          MySQL_flag = True,
                          Fetch_stock_statistics_flag = False, 
                          Flag_sub_category=False, # 不用一直開著
                          Flag_twse = True, 
                          Flag_tpe_stocks = True, 
                          Flag_tsw_stocks = True,  
                          timesleep = 5)


