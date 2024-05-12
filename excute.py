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


stocks = TS.Taiwan_Stocks( db_settings = db_settings, Crawl_flag = True, MySQL_flag = True,
                           Fetch_stock_statistics_flag = True, table_name = "stock_tsw",Flag_tpe_stocks = False, Flag_tsw_stocks = True,  timesleep = 5)


