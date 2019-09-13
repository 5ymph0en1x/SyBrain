from datetime import datetime as dt, timezone
from datetime import timedelta
from stocks import Main  # Machine Learning Module
import pandas as pd
from time import sleep
from utils import bitmex_http_conn2 as bitmex
from utils.bitmex_websocket_com import BitMEXWebsocket

#########################################

API_path_bmex_test = "wss://www.bitmex.com/realtime"
API_key_bmex_test = "key"
API_secret_bmex_test = "secret"

instrument_bmex = "XBTUSD"  # XBTUSD or ETHUSD
pos_size = 1  # Number of contracts traded

#########################################

matrix_bmex_ticker = [None] * 3
matrix_bmex_trade = [None] * 5

matrix_bmex_fairPrice = [None] * 10
matrix_bmex_fairPrice_var = [None] * 10
tick_count = 0
tick_ok = False

ws_bmex = BitMEXWebsocket(endpoint=API_path_bmex_test, symbol=instrument_bmex,
                          api_key=API_key_bmex_test, api_secret=API_secret_bmex_test)

client = bitmex.bitmex(test=False, api_key=API_key_bmex_test, api_secret=API_secret_bmex_test)

pos_taken = 0


def check_order_book(direction):
    print("Checking OrderBook...")
    obk = ws_bmex.market_depth()
    obk_bids = obk[0]['bids'][0:5]
    obk_asks = obk[0]['asks'][0:5]
    # print("Bids:", obk_bids)
    # print("Asks:", obk_asks)
    obk_buy_cum = 0
    obk_sell_cum = 0
    for obk_bid_unit in obk_bids:
        obk_buy_cum += obk_bid_unit[1]
    for obk_ask_unit in obk_asks:
        obk_sell_cum += obk_ask_unit[1]
    print("Sell Side: %s - Buy Side: %s" % (str(obk_sell_cum), str(obk_buy_cum)))
    if direction == 1 and obk_buy_cum > obk_sell_cum * 10:
        print("Go Buy !")
        return obk[0]['bids'][1][0]
    if direction == 0 and obk_sell_cum > obk_buy_cum * 10:
        print("Go Sell !")
        return obk[0]['asks'][1][0]
    return 0


def covering_fee(init, current, direction):
    if direction == 'buy':
        target = init + (init / 100 * 0.15)
        if current > target:
            return True
    if direction == 'sell':
        target = init - (init / 100 * 0.15)
        if current < target:
            return True
    return False


def fire_buy(departure):
    counter = 0
    print("Balance before:", ws_bmex.wallet_balance())
    '''launch_order(definition='stop_limit', direction='buy',
                 price=departure, stoplim=matrix_bmex_ticker[1], size=pos_size)'''
    '''launch_order(definition='stop_limit', direction='buy',
                 price=matrix_bmex_ticker[1], stoplim=departure, size=pos_size)'''
    launch_order(definition='market', direction='buy', size=pos_size)
    # launch_order(definition='limit', direction='buy', price=departure, size=pos_size)
    while ws_bmex.open_positions() == 0:
        sleep(1)
        counter += 1
        if counter >= 120:
            client.Order.Order_cancelAll().result()
            return 0
        if ws_bmex.open_stops() == 0:
            return 0
        continue
    print("BUY @", matrix_bmex_ticker[1])
    print("Balance - Step 1 of 2:", ws_bmex.wallet_balance())
    buyPos = 1
    buyPos_init = ws_bmex.get_instrument()['askPrice']
    buyPos_working_cached = buyPos_init
    ts_cached = [None]
    tick_buy_count = 0
    buyPos_final = 0
    trailing = False
    sl_ord_number = 0
    while buyPos > 0:
        datetime_cached = ws_bmex.get_instrument()['timestamp']
        dt2ts = dt.strptime(datetime_cached, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp()
        matrix_bmex_ticker[0] = int(dt2ts * 1000)
        matrix_bmex_ticker[1] = ws_bmex.get_instrument()['askPrice']
        matrix_bmex_ticker[2] = ws_bmex.get_instrument()['bidPrice']
        if ts_cached != matrix_bmex_ticker[0]:
            tick_buy_count += 1
            ts_cached = matrix_bmex_ticker[0]
            if tick_buy_count >= 500 and trailing is False:
                print("Cutting buy loss !")
                if len(ws_bmex.open_stops()) != 0:
                    client.Order.Order_cancelAll().result()
                if ws_bmex.open_positions() != 0:
                    # launch_order(definition='market', direction='sell', price=None, size=pos_size)
                    launch_order(definition='stop_loss', direction='sell', price=matrix_bmex_ticker[1], size=pos_size)
                while ws_bmex.open_positions() != 0:
                    sleep(0.1)
                    continue
                buyPos_final = matrix_bmex_ticker[2]
                break
            if buyPos_working_cached < matrix_bmex_ticker[2] and ws_bmex.open_positions() != 0:
                if len(ws_bmex.open_stops()) is not 0:
                    client.Order.Order_amend(orderID=sl_ord_number, stopPx=matrix_bmex_ticker[2]).result()
                    print("Trailing buy position to %s..." % str(matrix_bmex_ticker[2]))
                if len(ws_bmex.open_stops()) is 0 and covering_fee(buyPos_init, matrix_bmex_ticker[2], 'buy'):
                    print("Buy stop loss to BE...")
                    sl_ord_number = launch_order(definition='stop_loss', direction='sell', price=matrix_bmex_ticker[2],
                                                 size=pos_size)
                    trailing = True
                if trailing is False:
                    continue
                buyPos_working_cached = matrix_bmex_ticker[2]
                print("Buy stop loss modified at %s..." % str(buyPos_working_cached))
            if ws_bmex.open_positions() == 0 and trailing is True:
                if ws_bmex.open_positions() != 0:
                    print(sl_ord_number)
                    if len(ws_bmex.open_stops()) is not 0:
                        client.Order.Order_amend(orderID=sl_ord_number, orderQty=0).result()  # cancel stop order
                    launch_order(definition='market', direction='sell', price=None, size=pos_size)
                print("Finishing buy trail...")
                buyPos_final = matrix_bmex_ticker[2]
                break
    walletBal = ws_bmex.wallet_balance()
    print("Closed @", str(buyPos_final), ". Balance:", str(walletBal))
    return walletBal


def fire_sell(departure):
    counter = 0
    print("Balance before:", ws_bmex.wallet_balance())
    '''launch_order(definition='stop_limit', direction='sell',
                 price=departure, stoplim=matrix_bmex_ticker[2], size=pos_size)'''
    '''launch_order(definition='stop_limit', direction='sell',
                 price=matrix_bmex_ticker[2], stoplim=departure, size=pos_size)'''
    launch_order(definition='market', direction='sell', size=pos_size)
    # launch_order(definition='limit', direction='sell', price=departure, size=pos_size)
    while ws_bmex.open_positions() == 0:
        sleep(1)
        counter += 1
        if counter >= 120:
            client.Order.Order_cancelAll().result()
            return 0
        if ws_bmex.open_stops() == 0:
            return 0
        continue
    print("SELL @", matrix_bmex_ticker[2])
    print("Balance - Step 1 of 2:", ws_bmex.wallet_balance())
    sellPos = 1
    sellPos_init = ws_bmex.get_instrument()['bidPrice']
    sellPos_working_cached = sellPos_init
    ts_cached = [None]
    tick_sell_count = 0
    sellPos_final = 0
    trailing = False
    sl_ord_number = 0
    while sellPos > 0:
        datetime_cached = ws_bmex.get_instrument()['timestamp']
        dt2ts = dt.strptime(datetime_cached, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp()
        matrix_bmex_ticker[0] = int(dt2ts * 1000)
        matrix_bmex_ticker[1] = ws_bmex.get_instrument()['askPrice']
        matrix_bmex_ticker[2] = ws_bmex.get_instrument()['bidPrice']
        if ts_cached != matrix_bmex_ticker[0]:
            tick_sell_count += 1
            ts_cached = matrix_bmex_ticker[0]
            if tick_sell_count >= 500 and trailing is False:
                print("Cutting sell loss !")
                if len(ws_bmex.open_stops()) != 0:
                    client.Order.Order_cancelAll().result()
                if ws_bmex.open_positions() != 0:
                    # launch_order(definition='market', direction='buy', price=None, size=pos_size)
                    launch_order(definition='stop_loss', direction='buy', price=matrix_bmex_ticker[2], size=pos_size)
                while ws_bmex.open_positions() != 0:
                    sleep(0.1)
                    continue
                sellPos_final = matrix_bmex_ticker[1]
                break
            if sellPos_working_cached > matrix_bmex_ticker[1] and ws_bmex.open_positions() != 0:
                if len(ws_bmex.open_stops()) is not 0:
                    client.Order.Order_amend(orderID=sl_ord_number, stopPx=matrix_bmex_ticker[1]).result()
                    print("Trailing sell position to %s..." % str(matrix_bmex_ticker[1]))
                if len(ws_bmex.open_stops()) is 0 and covering_fee(sellPos_init, matrix_bmex_ticker[1], 'sell'):
                    print("Sell stop loss to BE...")
                    sl_ord_number = launch_order(definition='stop_loss', direction='buy', price=matrix_bmex_ticker[1],
                                                 size=pos_size)
                    trailing = True
                if trailing is False:
                    continue
                sellPos_working_cached = matrix_bmex_ticker[1]
                print("Sell stop loss modified at %s..." % str(sellPos_working_cached))
            if ws_bmex.open_positions() == 0 and trailing is True:
                if ws_bmex.open_positions() != 0:
                    print(sl_ord_number)
                    if len(ws_bmex.open_stops()) is not 0:
                        client.Order.Order_amend(orderID=sl_ord_number, orderQty=0).result()  # cancel stop order
                    launch_order(definition='market', direction='buy', price=None, size=pos_size)
                print("Finishing sell trail...")
                sellPos_final = matrix_bmex_ticker[1]
                break
    walletBal = ws_bmex.wallet_balance()
    print("Closed @", str(sellPos_final), ". Balance:", str(walletBal))
    return walletBal


def launch_order(definition, direction, price=None, size=None, stoplim=None):
    resulted = 0
    if definition == 'market':
        if direction == 'sell':
            size *= -1
        resulted = client.Order.Order_new(symbol=instrument_bmex, orderQty=size, ordType='Market').result()
        return resulted[0]['orderID']
    if definition == 'limit':
        if direction == 'sell':
            size *= -1
        resulted = client.Order.Order_new(symbol=instrument_bmex, orderQty=size, ordType='Limit', price=price,
                                          execInst='ParticipateDoNotInitiate, LastPrice').result()
        return resulted[0]['orderID']
    if definition == 'stop_limit':
        if direction == 'sell':
            size *= -1
        resulted = client.Order.Order_new(symbol=instrument_bmex, orderQty=size, ordType='StopLimit',
                                          execInst='LastPrice',
                                          stopPx=stoplim, price=price).result()
        return resulted[0]['orderID']
    if definition == 'stop_loss':
        if direction == 'sell':
            size *= -1
        resulted = client.Order.Order_new(symbol=instrument_bmex, orderQty=size, ordType='Stop',
                                          execInst='Close, LastPrice',
                                          stopPx=price).result()
        return resulted[0]['orderID']
    if definition == 'take_profit':
        if direction == 'sell':
            size *= -1
        resulted = client.Order.Order_new(symbol=instrument_bmex, orderQty=size, ordType='Limit',
                                          execInst='Close, LastPrice',
                                          price=price).result()
        return resulted[0]['orderID']


def bmex():
    global matrix_bmex_ticker
    global matrix_bmex_trade
    global matrix_bmex_fairPrice
    global matrix_bmex_fairPrice_var
    global pos_taken
    global tick_count
    global tick_ok
    fP_value_sum = 0
    fP_var_value_sum = 0
    fP_var_value_av = 0
    fairPrice_var_actual = 0
    results = 0
    p_verdict = 0
    datetime_minute_cached = None
    ts_cached = [None]
    fP_cached = [None] * 2
    while ws_bmex.ws.sock.connected:
        try:
            if datetime_minute_cached != dt.now().minute:
                time_starter = dt.utcnow() - timedelta(minutes=750)
                print(time_starter)
                data = client.Trade.Trade_getBucketed(symbol=instrument_bmex, binSize="1m", count=750,
                                                      startTime=time_starter).result()
                df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'])
                j = 0
                for i in data[0]:
                    df.loc[j] = pd.Series({'Date': i['timestamp'], 'Open': i['open'], 'High': i['high'],
                                           'Low': i['low'], 'Close': i['close'], 'Volume': i['volume'],
                                           'Adj Close': i['close']})
                    j += 1
                df = df[::-1]
                df.to_csv(r'pair_m1.csv', index=False)
                print('Launching Machine learning Module...')
                start_ts = (dt.utcnow()+timedelta(minutes=0)).strftime("%Y-%m-%d %H:%M:00")
                end_ts = (dt.utcnow()+timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:00")
                print('Start:', start_ts, '/ End:', end_ts)
                p = Main(['pair_m1.csv', start_ts, end_ts, 'm'])
                p_open = p.loc[p.shape[0]-1, 'Open']
                p_close = p.loc[p.shape[0]-1, 'Close']
                p_verdict = p_close - p_open
                if p_verdict > 0:
                    print('Machine learning : UP !')
                if p_verdict < 0:
                    print('Machine learning : DOWN !')
                datetime_minute_cached = dt.now().minute
            datetime_cached = ws_bmex.get_instrument()['timestamp']
            dt2ts = dt.strptime(datetime_cached, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc).timestamp()
            matrix_bmex_ticker[0] = int(dt2ts * 1000)  # (dt2ts - dt(1970, 1, 1)) / timedelta(seconds=1000)
            matrix_bmex_ticker[1] = ws_bmex.get_instrument()['askPrice']
            matrix_bmex_ticker[2] = ws_bmex.get_instrument()['bidPrice']
            if ts_cached != matrix_bmex_ticker[0] and fP_cached[0] != ws_bmex.get_instrument()['fairPrice']:
                ts_cached = matrix_bmex_ticker[0]
                fP_cached[1] = fP_cached[0]
                fP_cached[0] = ws_bmex.get_instrument()['fairPrice']
                if fP_cached[0] is not None and fP_cached[1] is not None:
                    matrix_bmex_fairPrice_var[tick_count] = (fP_cached[0] - fP_cached[1]) / 100
                    fairPrice_var_actual = matrix_bmex_fairPrice_var[tick_count]
                    tick_count += 1
                if tick_count >= 10:
                    tick_count = 0
                    if tick_ok is False:
                        tick_ok = True
                        print('Caching Complete !')
                if tick_ok is True:
                    fP_var_value_sum = 0
                    for fP_var_value in matrix_bmex_fairPrice_var:
                        fP_var_value_sum += fP_var_value
                    fP_var_value_av = fP_var_value_sum / 10
                    print("Average Fair Price Variation: %s - Last Variation: %s - Last Fair Price: %s - Position Taken: %s - Balance: %s" %
                          (str(fP_var_value_av), str(fairPrice_var_actual), str(ws_bmex.get_instrument()['fairPrice']), str(pos_taken), str(ws_bmex.wallet_balance())))
                    if fP_var_value_av > 0 and fairPrice_var_actual > fP_var_value_av * 2 and p_verdict > 0:
                        buy_departure = check_order_book(1)
                        if buy_departure != 0:
                            results = fire_buy(buy_departure)
                            if results == 0:
                                print("Resetting...")
                            else:
                                print("Total Balance:", str(results))
                            if results != 0:
                                pos_taken += 1
                            tick_ok = False
                            tick_count = 0
                    if fP_var_value_av < 0 and fairPrice_var_actual < fP_var_value_av * 2 and p_verdict < 0:
                        sell_departure = check_order_book(0)
                        if sell_departure != 0:
                            results = fire_sell(sell_departure)
                            if results == 0:
                                print("Resetting...")
                            else:
                                print("Total Balance:", str(results))
                            if results != 0:
                                pos_taken += 1
                            tick_ok = False
                            tick_count = 0

        except Exception as e:
            print(str(e))
            ws_bmex.exit()

        except KeyboardInterrupt:
            ws_bmex.exit()

    print("This is the end !")


if __name__ == '__main__':
    print("It began in Africa...")
    bmex()
    reboot = 0
    while reboot < 5:
        bmex()
        print("Rebooting...")
        reboot += 1
