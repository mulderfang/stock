from flask import Flask, request, abort
import shioaji as sj
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import re
import schedule
import time

# from mysql import connector
from linebot import LineBotApi
from linebot.models import TextSendMessage
from linebot.v3 import (
    WebhookHandler
)
from linebot.v3.exceptions import (
    InvalidSignatureError
)
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage,
    TemplateMessage,
    ButtonsTemplate,
    PostbackAction
)
from linebot.v3.webhooks import (
    MessageEvent,
    FollowEvent,
    PostbackEvent,
    TextMessageContent
)

# 設置MySQL資料庫連接
db_user = 'root'
db_password = '19970730'
db_host = '127.0.0.1'
db_name = 'sql_stock'

# 創建資料庫連接引擎
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')

app = Flask(__name__)

configuration = Configuration(access_token='6KPKKbmV9E3zgOJ+R4aAw5735CEQQuLFMWrda62hRXXsP0T+qgEs/0OxxfR2YAnwbJihvO3KVBwRt4L/Vd9YJ6AtrvZwD1W0+aqik1+L28K61FwTm8F8AoJpr2jzct5ElZu3su0PLxor1Juw569lIAdB04t89/1O/w1cDnyilFU=')
handler = WebhookHandler('b752004c8547752b0a6a3692bda06249')

# 定義核心邏輯函數
def get_filtered_stock_data():
        # 設置日期範圍
    today = datetime.now().date()
    one_year_ago = today - timedelta(days=30)

    # 格式化日期
    today_str = today.strftime('%Y%m%d')
    one_year_ago_str = one_year_ago.strftime('%Y%m%d')

    # SQL 查詢個股
    query_stock = f"""
    SELECT *
    FROM daily_price
    WHERE date >= '{one_year_ago_str}' AND date <= '{today_str}'
    """
    stock_df_raw = pd.read_sql(query_stock, engine)

    # 登入永豐金 API
    api = sj.Shioaji(simulation=True)
    accounts = api.login(
        api_key="CDvUcvSaB6auVUQfmE8inMnRdZgBqL2NCb54b8JCU6VC", 
        secret_key="DDFEny7zxmCTamgQiMnPRhsut32oDjh3PHz93NQiZ3cg"
    )

    # 篩選股票數據
    scanners = api.scanners(
        scanner_type=sj.constant.ScannerType.ChangePercentRank, 
        ascending=True,  # 由小到大
        count=200  # 上限
    )
    df = pd.DataFrame(s.__dict__ for s in scanners)
    df.ts = pd.to_datetime(df.ts)
    df_up7 = df[df['rank_value'] > 7].sort_values(by='volume_ratio', ascending=False)
    df_up7 = df_up7.rename(columns={"code": "證券代號"})

    # 新增一個空列來存放 5 日最高價
    df_up7['5日最高價'] = None  # 初始化列

    # 計算並寫回
    for index, stock_id in df_up7['證券代號'].items():  # 使用 .items() 而非 .iteritems()
        high_price_5day = stock_df_raw[stock_df_raw['證券代號'] == stock_id]['最高價'].tail(5).max()
        df_up7.at[index, '5日最高價'] = high_price_5day

    # 只取目前價格大於五日最高價    
    df_up7 = df_up7[df_up7['close'] > df_up7['5日最高價']]

    stock_df_raw['Date'] = pd.to_datetime(stock_df_raw['Date'])
    stock_df_yesterday = stock_df_raw[stock_df_raw['Date'] == stock_df_raw['Date'].unique().tolist()[-1].strftime('%Y-%m-%d')]

    # 新增濾網
    merged_df = pd.merge(df_up7, stock_df_yesterday, on=['證券代號'], how='inner')

    # filter_df = merged_df[(merged_df['close'] > merged_df['5MA']) & 
    #                     (merged_df['5MA'] > merged_df['10MA']) & 
    #                     (merged_df['10MA'] > merged_df['20MA']) & 
    #                     (merged_df['20MA'] > merged_df['60MA']) &
    #                     (merged_df['成交筆數'] > 1000)]
    merged_df['各均線之上'] = ((merged_df['close'] > merged_df['5MA']) &
                              (merged_df['5MA'] > merged_df['10MA']) &
                              (merged_df['10MA'] > merged_df['20MA']) &
                              (merged_df['20MA'] > merged_df['60MA']) &
                              (merged_df['成交筆數'] > 1000))
    merged_df.loc[:, 'rank_value'] = merged_df['rank_value'].round(2)

    merged_df = merged_df.rename(columns={"證券代號": "code",
                                          "rank_value": "漲幅"})
    # filter_df = merged_df

    # 格式化回應訊息
    if not merged_df.empty:
        response = merged_df[['code','name','漲幅','各均線之上']].sort_values(by=['各均線之上','漲幅'],ascending=[False,False])
        return  response.to_string(index=False)
    else:
        return  "無符合條件的股票資料"  

def push_message():
    # 自動推波標的
    message_text = get_filtered_stock_data()

    line_bot_api2 = LineBotApi('6KPKKbmV9E3zgOJ+R4aAw5735CEQQuLFMWrda62hRXXsP0T+qgEs/0OxxfR2YAnwbJihvO3KVBwRt4L/Vd9YJ6AtrvZwD1W0+aqik1+L28K61FwTm8F8AoJpr2jzct5ElZu3su0PLxor1Juw569lIAdB04t89/1O/w1cDnyilFU=')
    line_bot_api2.push_message('Ce1e2d08c12c4004358b959251e12f882', TextSendMessage(text = message_text))

def push_message2():
    # 自動推波標的
    message_text = 'test test'

    line_bot_api2 = LineBotApi('6KPKKbmV9E3zgOJ+R4aAw5735CEQQuLFMWrda62hRXXsP0T+qgEs/0OxxfR2YAnwbJihvO3KVBwRt4L/Vd9YJ6AtrvZwD1W0+aqik1+L28K61FwTm8F8AoJpr2jzct5ElZu3su0PLxor1Juw569lIAdB04t89/1O/w1cDnyilFU=')
    line_bot_api2.push_message('Ce1e2d08c12c4004358b959251e12f882', TextSendMessage(text = message_text))



# 設定排程
schedule.every().day.at("09:01").do(push_message)
schedule.every().day.at("09:30").do(push_message)
schedule.every().day.at("10:00").do(push_message)
schedule.every().day.at("10:30").do(push_message)
schedule.every().day.at("10:48").do(push_message)
schedule.every().day.at("10:51").do(push_message2)
# schedule.every().day.at("11:22").do(push_message)


@app.route("/callback", methods=['POST'])
def callback():
    # get X-Line-Signature header value
    signature = request.headers['X-Line-Signature']

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)

    # handle webhook body
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        app.logger.info("Invalid signature. Please check your channel access token/channel secret.")
        abort(400)

    return 'OK'

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    if re.match('隔日沖標的', event.message.text):
        message_text = get_filtered_stock_data()

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            
            # reply message
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text=message_text)]
                )
            )

# 持續執行排程
while True:
    schedule.run_pending()
    time.sleep(1)




if __name__ == "__main__": 
    app.run()