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
    """"""

    def __init__(self) -> None:
        """"""
        # Connect to the mongo-host
        store = Arctic("localhost")

        # Create a library if not exist
        store.initialize_library('vnpy.vnpy', lib_type=CHUNK_STORE)

        # Get a library
        self.library = store['vnpy.vnpy']

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """"""
        # Store key parameters
        bar = bars[0]
        symbol = bar.symbol
        exchange = bar.exchange.value
        interval = bar.interval.value

        key = [i for i in bar.__dict__]

        key.remove("gateway_name")
        key.remove("vt_symbol")
        key.remove("datetime")
        key.append("date")

        # Convert bar data to dataframe
        test_dict = {i: [] for i in key}
        for bar in bars:
            test_dict["symbol"].append(bar.symbol)
            test_dict["exchange"].append(bar.exchange.value)
            test_dict["date"].append(convert_tz(bar.datetime))
            test_dict["interval"].append(bar.interval.value)
            test_dict["volume"].append(bar.volume)
            test_dict["open_interest"].append(bar.open_interest)
            test_dict["open_price"].append(bar.open_price)
            test_dict["high_price"].append(bar.high_price)
            test_dict["low_price"].append(bar.low_price)
            test_dict["close_price"].append(bar.close_price)
        data_frame = pd.DataFrame(test_dict)

        # Upsert data into database
        bar_symbol = symbol + "-" + exchange + "-" + interval + "-" + "bar"
        self.library.update(bar_symbol, data_frame, upsert=True)

        # Update bar overview
        overview_symbol = symbol + "-" + exchange + "-" + interval + "-" + "overview"
        bar_overview = BarOverview()
        ccc = [i for i in self.library.iterator(bar_symbol)]
        count = 0
        for i in ccc:
            count += len(i)
        bar_overview.exchange = exchange
        bar_overview.symbol = symbol
        bar_overview.interval = interval
        bar_overview.start = ccc[0].iloc[0]["date"]
        bar_overview.end = ccc[-1].iloc[-1]["date"]
        bar_overview.count = count

        df = pd.DataFrame([{"exchange": exchange, "symbol": symbol, "interval": interval, "date": bar_overview.start, "start": bar_overview.start, "end": bar_overview.end, "count": bar_overview.count}])
        self.library.update(overview_symbol, df, upsert=True)

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        # Store key parameters
        tick = ticks[0]
        symbol = tick.symbol
        exchange = tick.exchange.value

        key = [i for i in tick.__dict__]

        key.remove("gateway_name")
        key.remove("vt_symbol")
        key.remove("datetime")
        key.append("date")

        # Convert tick data to dataframe
        test_dict = {i: [] for i in key}
        for tick in ticks:
            test_dict["symbol"].append(tick.symbol)
            test_dict["exchange"].append(tick.exchange.value)
            test_dict["date"].append(convert_tz(tick.datetime))

            test_dict["name"].append(tick.name)
            test_dict["volume"].append(tick.volume)
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
        data_frame = pd.DataFrame(test_dict)

        # Upsert data into database
        bar_symbol = symbol + "-" + exchange + "-" + "tick"
        self.library.update(bar_symbol, data_frame, upsert=True, chunk_size='S')

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """"""
        bar_symbol = symbol + "-" + exchange.value + "-" + interval.value + "-" + "bar"
        if bar_symbol in self.library.list_symbols():
            bars: List[BarData] = []
            frequence = "T"
            b = self.library.read(bar_symbol, chunk_range=pd.date_range(start, end, freq=frequence))
            f = b.to_dict(orient='index')
            for d in f.values():
                bar_datetime = d["date"].value // 1000000000
                bar_datetime = datetime.utcfromtimestamp(bar_datetime).astimezone(DB_TZ)
                bar = BarData("DB", d["symbol"], Exchange(d["exchange"]), d["date"])
                bar.symbol = d["symbol"]
                bar.volume = d["volume"]
                bar.open_price = d["open_price"]
                bar.high_price = d["high_price"]
                bar.low_price = d["low_price"]
                bar.close_price = d["close_price"]
                bar.open_interest = d["open_interest"]
                bar.interval = Interval(d["interval"])
                bars.append(bar)
            return bars
        else:
            return "bar data does not exit, please save data first"

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
            frequence = "L"
            b = self.library.read(tick_symbol, chunk_range=pd.date_range(start, end, freq=frequence))
            ticks: List[TickData] = []
            f = b.to_dict(orient='index')
            for d in f.values():
                tick = TickData("DB", d["symbol"], Exchange(d["exchange"]), d["date"])
                tick.datetime = d["date"]
                tick.symbol = d["symbol"]

                tick.name = d["name"]
                tick.volume = d["volume"]
                tick.open_interest = d["open_interest"]
                tick.last_price = d["last_price"]
                tick.last_volume = d["last_volume"]
                tick.limit_up = d["limit_up"]
                tick.limit_down = d["limit_down"]

                tick.open_price = d["open_price"]
                tick.high_price = d["high_price"]
                tick.low_price = d["low_price"]
                tick.pre_close = d["pre_close"]

                tick.bid_price_1 = d["bid_price_1"]
                tick.bid_price_2 = d["bid_price_2"]
                tick.bid_price_3 = d["bid_price_3"]
                tick.bid_price_4 = d["bid_price_4"]
                tick.bid_price_5 = d["bid_price_5"]

                tick.ask_price_1 = d["ask_price_1"]
                tick.ask_price_2 = d["ask_price_2"]
                tick.ask_price_3 = d["ask_price_3"]
                tick.ask_price_4 = d["ask_price_4"]
                tick.ask_price_5 = d["ask_price_5"]

                tick.bid_volume_1 = d["bid_volume_1"]
                tick.bid_volume_2 = d["bid_volume_2"]
                tick.bid_volume_3 = d["bid_volume_3"]
                tick.bid_volume_4 = d["bid_volume_4"]
                tick.bid_volume_5 = d["bid_volume_5"]

                tick.ask_volume_1 = d["ask_volume_1"]
                tick.ask_volume_2 = d["ask_volume_2"]
                tick.ask_volume_3 = d["ask_volume_3"]
                tick.ask_volume_4 = d["ask_volume_4"]
                tick.ask_volume_5 = d["ask_volume_5"]
                ticks.append(tick)
            return ticks
        else:
            return "bar data does not exit, please save data first"

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """"""
        bar_symbol = symbol + "-" + exchange.value + "-" + interval.value + "-" + "bar"
        overview_symbol = symbol + "-" + exchange.value + "-" + interval.value + "-" + "overview"
        if bar_symbol in self.library.list_symbols():
            info = self.library.get_info(bar_symbol)
            self.library.delete(bar_symbol)
        else:
            return "bar data does not exit, please save data first"
        # Delete baroverview
        if overview_symbol in self.library.list_symbols():
            self.library.delete(overview_symbol)
        return info["len"]

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """"""
        tick_symbol = symbol + "-" + exchange.value + "-" + "tick"
        if tick_symbol in self.library.list_symbols():
            info = self.library.get_info(tick_symbol)
            self.library.delete(tick_symbol)
        else:
            return "tick data does not exit, please save data first"
        return info["len"]

    def get_bar_overview(self) -> List[BarOverview]:
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
            bar_overview.start = pd.Timestamp(i["start"].values[0]).to_pydatetime()
            bar_overview.end = pd.Timestamp(i["end"].values[0]).to_pydatetime()
            bar_overview.count = i["count"].values[0]
            overviews.append(bar_overview)
        # the first item is symbol default which should be removed
        return overviews[1:]
