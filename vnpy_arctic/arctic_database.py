from datetime import datetime
from typing import Any, List
from arctic.chunkstore.chunkstore import ChunkStore

import pandas as pd
from arctic import Arctic, CHUNK_STORE
from arctic.date import DateRange

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS


class ArcticDatabase(BaseDatabase):
    """基于Arctic的MongoDB数据库接口"""

    def __init__(self) -> None:
        """"""
        # 初始化连接
        self.connection: Arctic = Arctic(SETTINGS["database.host"])

        # 初始化实例
        self.connection.initialize_library("bar_data", CHUNK_STORE)
        self.connection.initialize_library("tick_data", CHUNK_STORE)

        # 获取数据库
        self.bar_library: ChunkStore = self.connection["bar_data"]
        self.tick_library: ChunkStore = self.connection["tick_data"]

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """保存K线数据"""
        # 读取主键参数
        bar = bars[0]
        symbol = bar.symbol
        exchange = bar.exchange.value
        interval = bar.interval.value

        # 转换数据为DataFrame
        data: List[dict] = []

        for bar in bars:
            d = {
                "date": convert_tz(bar.datetime),
                "open_price": bar.open_price,
                "high_price": bar.high_price,
                "low_price": bar.low_price,
                "close_price": bar.close_price,
                "volume": bar.volume,
                "turnover": bar.turnover,
                "open_interest": bar.open_interest,
            }

            data.append(d)

        df: pd.DataFrame = pd.DataFrame.from_records(data)

        # 使用update操作将数据更新到数据库中
        table_name = generate_table_name(symbol, exchange, interval)
        self.bar_library.update(table_name, df, upsert=True)

        # 更新K线汇总数据
        overview_symbol = symbol + "-" + exchange + "-" + interval + "-" + "overview"
        ccc = [i for i in self.bar_library.iterator(bar_symbol)]
        count = 0
        for i in ccc:
            count += len(i)
        start_time = ccc[0].iloc[0]["date"]
        end_time = ccc[-1].iloc[-1]["date"]

        df = pd.DataFrame([{"exchange": exchange, "symbol": symbol, "interval": interval, "date": start_time, "start": start_time, "end": end_time, "count": count}])
        self.bar_library.update(overview_symbol, df, upsert=True)

        return True

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        """保存TICK数据"""
        # 读取主键参数
        tick = ticks[0]
        symbol = tick.symbol
        exchange = tick.exchange.value

        # 转换数据为DataFrame
        data: List[dict] = []

        for tick in ticks:
            d = {
                "date": convert_tz(tick.datetime),
                "name": tick.name,
                "volume": tick.volume,
                "turnover": tick.turnover,
                "open_interest": tick.open_interest,
                "last_price": tick.last_price,
                "last_volume": tick.last_volume,
                "limit_up": tick.limit_up,
                "limit_down": tick.limit_down,
                "open_price": tick.open_price,
                "high_price": tick.high_price,
                "low_price": tick.low_price,
                "pre_close": tick.pre_close,
                "bid_price_1": tick.bid_price_1,
                "bid_price_2": tick.bid_price_2,
                "bid_price_3": tick.bid_price_3,
                "bid_price_4": tick.bid_price_4,
                "bid_price_5": tick.bid_price_5,
                "ask_price_1": tick.ask_price_1,
                "ask_price_2": tick.ask_price_2,
                "ask_price_3": tick.ask_price_3,
                "ask_price_4": tick.ask_price_4,
                "ask_price_5": tick.ask_price_5,
                "bid_volume_1": tick.bid_volume_1,
                "bid_volume_2": tick.bid_volume_2,
                "bid_volume_3": tick.bid_volume_3,
                "bid_volume_4": tick.bid_volume_4,
                "bid_volume_5": tick.bid_volume_5,
                "ask_volume_1": tick.ask_volume_1,
                "ask_volume_2": tick.ask_volume_2,
                "ask_volume_3": tick.ask_volume_3,
                "ask_volume_4": tick.ask_volume_4,
                "ask_volume_5": tick.ask_volume_5,
                "localtime": tick.localtime,
            }
            data.append(d)

        df: pd.DataFrame = pd.DataFrame.from_records(data)

        # 使用update操作将数据更新到数据库中
        table_name = generate_table_name(symbol, exchange)
        self.tick_library.update(table_name, df, upsert=True)

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """读取K线数据"""
        table_name = generate_table_name(symbol, exchange, interval)
        df = self.bar_library.read(table_name, chunk_range=DateRange(start, end))
            
        bars: List[BarData] = []

        for tp in df.itertuples():
            dt = datetime.fromtimestamp(tp.date.timestamp(), DB_TZ)

            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=tp.volume,
                turnover=tp.turnover,
                open_interest=tp.open_interest,
                open_price=tp.open_price,
                high_price=tp.high_price,
                low_price=tp.low_price,
                close_price=tp.close_price,
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[TickData]:
        """读取Tick数据"""
        table_name = generate_table_name(symbol, exchange)
        df = self.tick_library.read(table_name, chunk_range=DateRange(start, end))
            
        ticks: List[TickData] = []

        for tp in df.itertuples():
            dt = datetime.fromtimestamp(tp.date.timestamp(), DB_TZ)

            tick = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                name=tp.name,
                volume=tp.volume,
                turnover=tp.turnover,
                open_interest=tp.open_interest,
                last_price=tp.last_price,
                last_volume=tp.last_volume,
                limit_up=tp.limit_up,
                limit_down=tp.limit_down,
                open_price=tp.open_price,
                high_price=tp.high_price,
                low_price=tp.low_price,
                pre_close=tp.pre_close,
                bid_price_1=tp.bid_price_1,
                bid_price_2=tp.bid_price_2,
                bid_price_3=tp.bid_price_3,
                bid_price_4=tp.bid_price_4,
                bid_price_5=tp.bid_price_5,
                ask_price_1=tp.ask_price_1,
                ask_price_2=tp.ask_price_2,
                ask_price_3=tp.ask_price_3,
                ask_price_4=tp.ask_price_4,
                ask_price_5=tp.ask_price_5,
                bid_volume_1=tp.bid_volume_1,
                bid_volume_2=tp.bid_volume_2,
                bid_volume_3=tp.bid_volume_3,
                bid_volume_4=tp.bid_volume_4,
                bid_volume_5=tp.bid_volume_5,
                ask_volume_1=tp.ask_volume_1,
                ask_volume_2=tp.ask_volume_2,
                ask_volume_3=tp.ask_volume_3,
                ask_volume_4=tp.ask_volume_4,
                ask_volume_5=tp.ask_volume_5,
                localtime=tp.localtime,
                gateway_name="DB"
            )
            ticks.append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        # 生成表名
        table_name = generate_table_name(symbol, exchange, interval)
    
        # 查询总数据量
        info = self.bar_library.get_info(table_name)
        count = info["len"]
    
        # 删除数据
        self.bar_library.delete(table_name)

        # 删除K线汇总数据
        overview_symbol = symbol + "-" + exchange.value + "-" + interval.value + "-" + "overview"
        if overview_symbol in self.bar_library.list_symbols():
            self.bar_library.delete(overview_symbol)

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除Tick数据"""
        # 生成表名
        table_name = generate_table_name(symbol, exchange)
    
        # 查询总数据量
        info = self.tick_library.get_info(table_name)
        count = info["len"]
    
        # 删除数据
        self.tick_library.delete(table_name)

        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """"查询数据库中的K线汇总信息"""
        symbols = self.bar_library.list_symbols()
        overviews = []
        for symbol in symbols:
            if symbol.split("-")[-1] == "overview":
                overviews.append(symbol)
        dataframe_overview = [self.bar_library.read(overview) for overview in overviews]
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


def generate_table_name(symbol: str, exchange: Exchange, interval: Interval = None) -> str:
    """生成数据表名"""
    if interval:
        return f"{symbol}_{exchange.value}_{interval.value}"
    else:
        return f"{symbol}_{exchange.value}"
