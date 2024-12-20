"""币安账户监控相关函数"""

import json
import time
from datetime import datetime
from datetime import timedelta

import pandas as pd
from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from sqlalchemy import create_engine

from config import API_DICT, INITIAL_CASH
from config import database_config as dc_
from utils.exchange import BinanceTrade

pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


class Account(object):
    """账户资金记录"""

    def __init__(self, _account_name):
        self.account_name = _account_name  # 账户名称
        self.trade = BinanceTrade(self.account_name)  # 初始化API,与交易所建立链接

    def account_asset(self, coin):
        """
        返回账户总资产
        :param coin: 币种类型 BNB or USDT
        :return: float 币种的数量
        """

        balance = self.trade.future_balance()  # 获取账户余额

        df = pd.DataFrame.from_dict(balance)

        df.set_index('asset', inplace=True)

        trade_amount = df.loc[coin.upper(), 'balance']

        return float(trade_amount)

    def account_margin_used(self):
        """
        获取账户已经使用的保证金

        :return: float USDT的数量

        """
        margin = self.trade.future_account()  # 获取账户的全部资产以及持仓
        position = pd.DataFrame(margin['positions'])
        used_amount = sum(abs(position['positionAmt'].astype(float) * position['entryPrice'].astype(float)))

        return used_amount

    def totalMarginBalance(self):
        """
        获取账户实时保证金余额

        :return: float USDT的数量

        """
        margin = self.trade.future_account()  # 获取账户的全部资产以及持仓

        position = pd.DataFrame(margin['positions'])

        used_amount = sum(abs(position['positionAmt'].astype(float) * position['entryPrice'].astype(float)))
        account_name = self.account_name

        df = pd.DataFrame([[account_name, float(margin['totalMarginBalance']), used_amount]],
                          columns=['account_name', 'total_margin', 'used_amount'])
        df['time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return df

    def account_position(self):
        """
        获取账户持仓情况

        :return: Dataframe

        """
        margin = self.trade.future_account()  # 获取账户的全部资产以及持仓
        df = pd.DataFrame(margin['positions'])

        df = df[df['positionAmt'].astype(float) != 0]
        df = df[['symbol', 'unrealizedProfit', 'entryPrice', 'notional']]
        df.reset_index(inplace=True, drop=True)
        df['time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        df['account'] = self.account_name
        df['side'] = df['notional'].map(lambda x: (float(x) < 0 and -1) or (float(x) >= 0 and 1))
        df.loc[0, 'all_cash'] = margin['totalMarginBalance']
        return df

    def account_equity(self):
        """
        账户权益
        资产余额以及保证金使用
        """

        cash = self.account_asset('usdt')
        margin_used = self.account_margin_used()
        account_name = self.account_name

        df = pd.DataFrame([[account_name, cash, margin_used]], columns=['account_name', 'cash', 'margin_used'])
        df['time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return df

    def account_balance(self, deposit_cash=0.0, withdraw_cash=0.0):
        """
        账户权益
        资产余额以及保证金使用
        """

        balance = float(self.account_asset('usdt'))
        margin_used = float(self.account_margin_used())
        account_name = self.account_name
        initial_cash = INITIAL_CASH[self.account_name]

        df = pd.DataFrame([[account_name, balance, margin_used, initial_cash, deposit_cash, withdraw_cash]],
                          columns=['account_name', 'balance', 'margin_used', 'initial_cash',
                                   'deposit_cash', 'withdraw_cash'])
        df['time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        return df


class Order(object):
    """账户资金记录"""

    def __init__(self, _account_name):

        self.account_name = _account_name  # 账户名称
        self.api_key = API_DICT[self.account_name]['apiKey']
        self.api_secret = API_DICT[self.account_name]['secret']

    @staticmethod
    def print_stream_buffer_data(binance_websocket_api_manager, stream_id, name):
        """
        :param name:
        :param binance_websocket_api_manager:
        :param stream_id:
        """
        while True:
            if binance_websocket_api_manager.is_manager_stopping():
                exit(0)
            oldest_stream_data_from_stream_buffer = binance_websocket_api_manager.pop_stream_data_from_stream_buffer(
                stream_id)
            if oldest_stream_data_from_stream_buffer is False:
                time.sleep(0.01)
            else:
                database_connect = create_engine(
                    f"mysql://{dc_['user']}:{dc_['passwd']}@{dc_['host']}:{dc_['port']}/{dc_['database']}")
                table_name = f"{name}_order"
                data = json.loads(oldest_stream_data_from_stream_buffer)
                if data['e'] == 'ORDER_TRADE_UPDATE':
                    df = pd.DataFrame(data['o'], index=[0])
                    df['notional'] = float(df['ap']) * float(df['z'])
                    df = df[['s', 'S', 'ap', 'p', 'q', 'z', 'X', 'T', 'rp', 'notional']]
                    df.columns = ['symbol', 'side', 'filled_price', 'order_price', 'order_quantity', 'filled_quantity',
                                  'order_status', 'order_time', 'profit', 'notional']
                    df['order_time'] = pd.to_datetime(df['order_time'], unit='ms') + timedelta(hours=8)
                    df['time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    df.to_sql(name=table_name, con=database_connect, if_exists='append')
                database_connect.dispose()  # 释放数据库连接

    def print_stream_buffer_data_full(self, _, message):
        """
        :param message:
        """
        while True:
            oldest_stream_data_from_stream_buffer = message
            if oldest_stream_data_from_stream_buffer is False:
                time.sleep(0.01)
            else:
                data = json.loads(oldest_stream_data_from_stream_buffer)
                if 'result' in data and data['result'] is None:
                    return
                database_connect = create_engine(
                    f"mysql://{dc_['user']}:{dc_['passwd']}@{dc_['host']}:{dc_['port']}/{dc_['database']}")
                table_name = f"{self.account_name}_order"
                if data['e'] == 'ORDER_TRADE_UPDATE':
                    df = pd.DataFrame(data['o'], index=[0])
                    df['notional'] = float(df['ap']) * float(df['z'])
                    df = df[['s', 'S', 'ap', 'p', 'q', 'z', 'X', 'T', 'rp', 'notional']]
                    df.columns = ['symbol', 'side', 'filled_price', 'order_price', 'order_quantity', 'filled_quantity',
                                  'order_status', 'order_time', 'profit', 'notional']
                    df['order_time'] = pd.to_datetime(df['order_time'], unit='ms') + timedelta(hours=8)
                    df['time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    df.to_sql(name=table_name, con=database_connect, if_exists='append')
                database_connect.dispose()  # 释放数据库连接

    def print_stream_buffer_data_full_account(self, _, message):
        """
        :param message_item:
        """
        while True:
            oldest_stream_data_from_stream_buffer = message
            if oldest_stream_data_from_stream_buffer is False:
                time.sleep(0.01)
            else:
                # database_connect = create_engine(
                #     f"mysql://{dc_['user']}:{dc_['passwd']}@{dc_['host'}/:{dc_['port'{dc_['database']}")
                # table_name = f"{name}_order"
                data = json.loads(oldest_stream_data_from_stream_buffer)
                if 'result' in data and data['result'] is None:
                    return
                if data['e'] == 'ACCOUNT_UPDATE':

                    database_connect = create_engine(
                        f"mysql://{dc_['user']}:{dc_['passwd']}@{dc_['host']}:{dc_['port']}/{dc_['database']}")

                    try:
                        sql = f"select * from {self.account_name}_balance"

                        old_ = pd.read_sql(sql, con=database_connect)

                        dd = old_.tail(1)
                        dd.reset_index(inplace=True)

                        deposit = dd.loc[0, 'deposit_cash']
                        withdraw = dd.loc[0, 'withdraw_cash']

                    except:
                        withdraw = 0.0
                        deposit = 0.0

                    if data['a']['m'] == 'WITHDRAW':
                        # print('取出金额', data['a']['B'][0]['bc'])
                        withdraw += float(data['a']['B'][0]['bc'])

                        psy_ns_account = Account(self.account_name)
                        df = psy_ns_account.account_balance(withdraw_cash=withdraw, deposit_cash=deposit)
                        df.to_sql(name=f'{self.account_name}_balance', if_exists='append', con=database_connect,
                                  index=False)

                    if data['a']['m'] == 'DEPOSIT':
                        # print('存入金额', data['a']['B'][0]['bc'])

                        deposit += float(data['a']['B'][0]['bc'])
                        psy_ns_account = Account(self.account_name)
                        df = psy_ns_account.account_balance(withdraw_cash=withdraw, deposit_cash=deposit)
                        df.to_sql(name=f'{self.account_name}_balance', if_exists='append', con=database_connect,
                                  index=False)
                    database_connect.dispose()  # 释放数据库连接
                    # df['notional'] = float(df['ap']) * float(df['z'])
                    # df = df[['s', 'S', 'ap', 'p', 'q', 'z', 'X', 'T', 'rp', 'notional']]
                    # df.columns = ['symbol', 'side', 'filled_price', 'order_price', 'order_quantity', 'filled_quantity',
                    #               'order_status', 'order_time', 'profit', 'notional']
                    # df['order_time'] = pd.to_datetime(df['order_time'], unit='ms') + timedelta(hours=8)
                    # df['time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    # df.to_sql(name=table_name, con=database_connect, if_exists='append')

    def order_record_booking_rest(self):
        """
        使用REST API获取账户订单信息
        """
        client = UMFutures(self.api_key, self.api_secret)
        
        # 获取所有交易对的信息
        exchange_info = client.exchange_info()
        symbols = [s['symbol'] for s in exchange_info['symbols']]
        
        all_trades = []
        for symbol in symbols:
            try:
                # 获取每个交易对的最近成交记录
                trades = client.get_account_trades(symbol=symbol)
                if trades:
                    all_trades.extend(trades)
            except Exception as e:
                print(f"获取 {symbol} 交易记录时出错: {e}")
                continue
        
        if all_trades:
            database_connect = create_engine(
                f"mysql://{dc_['user']}:{dc_['passwd']}@{dc_['host']}:{dc_['port']}/{dc_['database']}")
            
            df = pd.DataFrame(all_trades)
            df['notional'] = df['price'].astype(float) * df['qty'].astype(float)
            
            # 重命名和选择需要的列
            df = df.rename(columns={
                'symbol': 'symbol',
                'side': 'side',
                'price': 'filled_price',
                'qty': 'filled_quantity',
                'realizedPnl': 'profit',
                'time': 'order_time'
            })
            
            # 处理时间格式
            df['order_time'] = pd.to_datetime(df['order_time'], unit='ms') + timedelta(hours=8)
            df['time'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
            # 保存到数据库
            df.to_sql(name=f"{self.account_name}_order", con=database_connect, if_exists='append', index=False)
            database_connect.dispose()

    def account_record_booking_rest(self):
        """
        使用REST API获取账户信息
        """
        client = UMFutures(self.api_key, self.api_secret)
        
        # 获取账户信息
        account_info = client.account()
        
        if account_info:
            database_connect = create_engine(
                f"mysql://{dc_['user']}:{dc_['passwd']}@{dc_['host']}:{dc_['port']}/{dc_['database']}")
                
            try:
                # 获取历史存取款记录
                sql = f"select * from {self.account_name}_balance"
                old_ = pd.read_sql(sql, con=database_connect)
                dd = old_.tail(1)
                dd.reset_index(inplace=True)
                deposit = dd.loc[0, 'deposit_cash']
                withdraw = dd.loc[0, 'withdraw_cash']
            except:
                withdraw = 0.0
                deposit = 0.0
                
            # 更新账户余额信息
            psy_ns_account = Account(self.account_name)
            df = psy_ns_account.account_balance(withdraw_cash=withdraw, deposit_cash=deposit)
            df.to_sql(name=f'{self.account_name}_balance', if_exists='append', con=database_connect, index=False)
            
            database_connect.dispose()

    def order_record_booking(self):
        client = UMFutures(self.api_key, self.api_secret)
        response = client.new_listen_key()
        ws_client = UMFuturesWebsocketClient(on_message=self.print_stream_buffer_data_full)

        ws_client.user_data(
            listen_key=response["listenKey"],
            id=1,
        )

    def account_record_booking(self):
        client = UMFutures(self.api_key, self.api_secret)
        response = client.new_listen_key()

        ws_client = UMFuturesWebsocketClient(on_message=self.print_stream_buffer_data_full_account)

        ws_client.user_data(
            listen_key=response["listenKey"],
            id=1,
        )
