""""""
from datetime import datetime
from typing import List
import pandas as pd

from arctic import CHUNK_STORE, Arctic

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    convert_tz
)


class ArcticMongoDatabase(BaseDatabase):
    """基于Arctic的MongoDB数据库接口"""

    def __init__(self) -> None:
        """"""
        # 连接接口
        store = Arctic("localhost")
        # 当数据库不存在时创建数据库
        store.initialize_library("vnpy.vnpy", lib_type=CHUNK_STORE)
        # 获取数据库
        self.library = store["vnpy.vnpy"]

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """保存K线数据"""
        # 读取主键参数
        bar = bars[0]
        symbol = bar.symbol
        exchange = bar.exchange.value
        interval = bar.interval.value

        key = [i for i in bar.__dict__]

        key.remove("gateway_name")
        key.remove("vt_symbol")
        key.remove("datetime")
        key.append("date")

        # 将BarData转化为DafaFrame，并调整时区，存入数据库
        test_dict = {i: [] for i in key}
        for bar in bars:
            test_dict["symbol"].append(bar.symbol)
            test_dict["exchange"].append(bar.exchange.value)
            test_dict["date"].append(convert_tz(bar.datetime))
            test_dict["interval"].append(bar.interval.value)
            test_dict["volume"].append(bar.volume)
            test_dict["turnover"].append(bar.turnover)
            test_dict["open_interest"].append(bar.open_interest)
            test_dict["open_price"].append(bar.open_price)
            test_dict["high_price"].append(bar.high_price)
            test_dict["low_price"].append(bar.low_price)
            test_dict["close_price"].append(bar.close_price)
        data_frame = pd.DataFrame(test_dict)

        # 使用update操作将数据更新到数据库中
        bar_symbol = symbol + "-" + exchange + "-" + interval + "-" + "bar"
        self.library.update(bar_symbol, data_frame, upsert=True)

        # 更新K线汇总数据
        overview_symbol = symbol + "-" + exchange + "-" + interval + "-" + "overview"
        ccc = [i for i in self.library.iterator(bar_symbol)]
        count = 0
        for i in ccc:
            count += len(i)
        start_time = ccc[0].iloc[0]["date"]
        end_time = ccc[-1].iloc[-1]["date"]

        df = pd.DataFrame([{"exchange": exchange, "symbol": symbol, "interval": interval, "date": start_time, "start": start_time, "end": end_time, "count": count}])
        self.library.update(overview_symbol, df, upsert=True)

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        """保存TICK数据"""
        tick = ticks[0]
        symbol = tick.symbol
        exchange = tick.exchange.value

        key = [i for i in tick.__dict__]

        key.remove("gateway_name")
        key.remove("vt_symbol")
        key.remove("datetime")
        key.append("date")

        # 将TickData转化为DafaFrame，并调整时区，存入数据库
        test_dict = {i: [] for i in key}
        for tick in ticks:
            test_dict["symbol"].append(tick.symbol)
            test_dict["exchange"].append(tick.exchange.value)
            test_dict["date"].append(convert_tz(tick.datetime))

            test_dict["name"].append(tick.name)
            test_dict["volume"].append(tick.volume)
            test_dict["turnover"].append(tick.turnover)
            test_dict["open_interest"].append(tick.open_interest)
            test_dict["last_price"].append(tick.last_price)
            test_dict["last_volume"].append(tick.last_volume)
            test_dict["limit_up"].append(tick.limit_up)
            test_dict["limit_down"].append(tick.limit_down)

            test_dict["open_price"].append(tick.open_price)
            test_dict["high_price"].append(tick.high_price)
            test_dict["low_price"].append(tick.low_price)
            test_dict["pre_close"].append(tick.pre_close)

            test_dict["bid_price_1"].append(tick.bid_price_1)
            test_dict["bid_price_2"].append(tick.bid_price_2)
            test_dict["bid_price_3"].append(tick.bid_price_3)
            test_dict["bid_price_4"].append(tick.bid_price_4)
            test_dict["bid_price_5"].append(tick.bid_price_5)

            test_dict["ask_price_1"].append(tick.ask_price_1)
            test_dict["ask_price_2"].append(tick.ask_price_2)
            test_dict["ask_price_3"].append(tick.ask_price_3)
            test_dict["ask_price_4"].append(tick.ask_price_4)
            test_dict["ask_price_5"].append(tick.ask_price_5)

            test_dict["bid_volume_1"].append(tick.bid_volume_1)
            test_dict["bid_volume_2"].append(tick.bid_volume_2)
            test_dict["bid_volume_3"].append(tick.bid_volume_3)
            test_dict["bid_volume_4"].append(tick.bid_volume_4)
            test_dict["bid_volume_5"].append(tick.bid_volume_5)

            test_dict["ask_volume_1"].append(tick.ask_volume_1)
            test_dict["ask_volume_2"].append(tick.ask_volume_2)
            test_dict["ask_volume_3"].append(tick.ask_volume_3)
            test_dict["ask_volume_4"].append(tick.ask_volume_4)
            test_dict["ask_volume_5"].append(tick.ask_volume_5)

            test_dict["localtime"].append(convert_tz(tick.localtime))
        data_frame = pd.DataFrame(test_dict)

        # 使用update操作将数据更新到数据库中
        bar_symbol = symbol + "-" + exchange + "-" + "tick"
        self.library.update(bar_symbol, data_frame, upsert=True)

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """读取K线数据"""
        bar_symbol = symbol + "-" + exchange.value + "-" + interval.value + "-" + "bar"
        if bar_symbol in self.library.list_symbols():
            bars: List[BarData] = []
            df = self.library.read(bar_symbol, chunk_range=pd.date_range(start, end))
            for date, symbol, exchange_d, interval, volume, turnover, open_interest, open_price, high_price, low_price, close_price\
                in zip(df["date"], df["symbol"], df["exchange"], df["interval"], df["volume"], df["turnover"],
                       df["open_interest"], df["open_price"], df["high_price"], df["low_price"], df["close_price"]):
                bar_datetime = datetime.fromtimestamp(date.timestamp(), DB_TZ)
                bar = BarData("DB", symbol, exchange, bar_datetime)
                bar.volume = volume
                bar.turnover = turnover
                bar.interval = Interval(interval)
                bar.open_interest = open_interest
                bar.open_price = open_price
                bar.high_price = high_price
                bar.low_price = low_price
                bar.close_price = close_price
                bars.append(bar)
            return bars
        else:
            return "bar data does not exit, please save data first."

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[TickData]:
        """"""
        tick_symbol = symbol + "-" + exchange.value + "-" + "tick"
        if tick_symbol in self.library.list_symbols():
            tick_symbol = symbol + "-" + exchange.value + "-" + "tick"
            df = self.library.read(tick_symbol, chunk_range=pd.date_range(start, end))
            ticks: List[TickData] = []
            for symbol, exchange_t, date, name, volume, turnover,\
                open_interest, last_price, last_volume, limit_up, limit_down,\
                open_price, high_price, low_price, pre_close,\
                bid_price_1, bid_price_2, bid_price_3, bid_price_4, bid_price_5,\
                ask_price_1, ask_price_2, ask_price_3, ask_price_4, ask_price_5,\
                bid_volume_1, bid_volume_2, bid_volume_3, bid_volume_4, bid_volume_5,\
                ask_volume_1, ask_volume_2, ask_volume_3, ask_volume_4, ask_volume_5, localtime\
                in zip(df["symbol"], df["exchange"], df["date"], df["name"], df["volume"], df["turnover"],
                       df["last_price"], df["last_volume"], df["limit_up"], df["limit_down"],
                       df["open_price"], df["high_price"], df["low_price"], df["pre_close"],
                       df["bid_price_1"], df["bid_price_2"], df["bid_price_3"], df["bid_price_4"], df["bid_price_5"],
                       df["ask_price_1"], df["ask_price_2"], df["ask_price_3"], df["ask_price_4"], df["ask_price_5"],
                       df["bid_volume_1"], df["bid_volume_2"], df["bid_volume_3"], df["bid_volume_4"], df["bid_volume_5"],
                       df["ask_volume_1"], df["ask_volume_2"], df["ask_volume_3"], df["ask_volume_4"], df["ask_volume_5"],
                       df["localtime"]):
                tz_time = datetime.fromtimestamp(date.timestamp(), DB_TZ)
                tick = TickData("DB", symbol, exchange, tz_time)

                tick.name = name
                tick.volume = volume
                tick.turnover = turnover
                tick.open_interest = open_interest
                tick.last_price = last_price
                tick.last_volume = last_volume
                tick.limit_up = limit_up
                tick.limit_down = limit_down

                tick.open_price = open_price
                tick.high_price = high_price
                tick.low_price = low_price
                tick.pre_close = pre_close

                tick.bid_price_1 = bid_price_1
                tick.bid_price_2 = bid_price_2
                tick.bid_price_3 = bid_price_3
                tick.bid_price_4 = bid_price_4
                tick.bid_price_5 = bid_price_5

                tick.ask_price_1 = ask_price_1
                tick.ask_price_2 = ask_price_2
                tick.ask_price_3 = ask_price_3
                tick.ask_price_4 = ask_price_4
                tick.ask_price_5 = ask_price_5

                tick.bid_volume_1 = bid_volume_1
                tick.bid_volume_2 = bid_volume_2
                tick.bid_volume_3 = bid_volume_3
                tick.bid_volume_4 = bid_volume_4
                tick.bid_volume_5 = bid_volume_5

                tick.ask_volume_1 = ask_volume_1
                tick.ask_volume_2 = ask_volume_2
                tick.ask_volume_3 = ask_volume_3
                tick.ask_volume_4 = ask_volume_4
                tick.ask_volume_5 = ask_volume_5

                tick.localtime = localtime
                
                ticks.append(tick)
            return ticks
        else:
            return "bar data does not exit, please save data first."

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        bar_symbol = symbol + "-" + exchange.value + "-" + interval.value + "-" + "bar"
        overview_symbol = symbol + "-" + exchange.value + "-" + interval.value + "-" + "overview"
        if bar_symbol in self.library.list_symbols():
            info = self.library.get_info(bar_symbol)
            self.library.delete(bar_symbol)
        else:
            return "bar data does not exit, please save data first."
        # 删除K线汇总数据
        if overview_symbol in self.library.list_symbols():
            self.library.delete(overview_symbol)
        return info["len"]

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除Tick数据"""
        tick_symbol = symbol + "-" + exchange.value + "-" + "tick"
        if tick_symbol in self.library.list_symbols():
            info = self.library.get_info(tick_symbol)
            self.library.delete(tick_symbol)
        else:
            return "tick data does not exit, please save data first."
        return info["len"]

    def get_bar_overview(self) -> List[BarOverview]:
        """"查询数据库中的K线汇总信息"""
        symbols = self.library.list_symbols()
        overviews = []
        for symbol in symbols:
            if symbol.split("-")[-1] == "overview":
                overviews.append(symbol)
        dataframe_overview = [self.library.read(overview) for overview in overviews]
        for i in dataframe_overview:
            bar_overview = BarOverview()
            bar_overview.exchange = Exchange(i["exchange"].values[0])
            bar_overview.symbol = i["symbol"].values[0]
            bar_overview.interval = Interval(i["interval"].values[0])
            bar_overview.start = datetime.fromtimestamp(pd.Timestamp(i["start"].values[0]).timestamp(), DB_TZ)
            bar_overview.end = datetime.fromtimestamp(pd.Timestamp(i["end"].values[0]).timestamp(), DB_TZ)
            bar_overview.count = i["count"].values[0]
            overviews.append(bar_overview)
        return overviews[1:]
