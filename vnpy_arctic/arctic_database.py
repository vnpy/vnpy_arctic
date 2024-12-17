from datetime import datetime
from pandas import DataFrame, Timestamp
from typing import List

import arcticdb as adb
from arcticdb.arctic import Arctic
from arcticdb.version_store.library import Library, SymbolDescription

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS


class ArcticDatabase(BaseDatabase):
    """基于ArcticDB的数据库接口"""

    def __init__(self) -> None:
        """"""
        self.database_path: str = SETTINGS["database.path"]  # .vntrader 或 D:\SeaTurtle\.vntrader\ 自定义路径
        self.database_name: str = SETTINGS["database.name"]  # arcticdb
        self.map_size: str = SETTINGS["database.map_size"]  # 5GB

        if not self.database_path:
            self.database_path = ".vntrader"
        if not self.map_size:
            self.map_size = "5GB"

        # 初始化连接
        self.ac: Arctic = adb.Arctic(f"lmdb://{self.database_path}?map_size={self.map_size}")

        # 获取数据库(本地路径为.vntrader/arcticdb/bar_data/）
        self.bar_library: Library = self.ac.get_library(f"{self.database_name}.bar_data", create_if_missing=True)
        self.tick_library: Library = self.ac.get_library(f"{self.database_name}.tick_data", create_if_missing=True)
        
    def save_bar_data(self, bars: List[BarData], stream: bool = True) -> bool:
        """保存K线数据"""
        # 转换数据为DataFrame
        data: list = []

        for bar in bars:
            d: dict = {
                "datetime": convert_tz(bar.datetime),
                "open_price": bar.open_price,
                "high_price": bar.high_price,
                "low_price": bar.low_price,
                "close_price": bar.close_price,
                "volume": bar.volume,
                "turnover": bar.turnover,
                "open_interest": bar.open_interest,
            }

            data.append(d)

        df: DataFrame = DataFrame.from_records(data)
        df = df.set_index('datetime')

        # 生成数据表名
        bar: BarData = bars[0]
        symbol: str = bar.symbol
        table_name: str = generate_table_name(symbol, bar.exchange, bar.interval)

        # 将数据更新到数据库中
        self.bar_library.update(
            symbol=table_name,
            data=df, 
            upsert=True,
            prune_previous_versions=True
        )

        # 更新K线汇总数据
        info: SymbolDescription = self.bar_library.get_description(table_name)
        count: int = info.row_count
        start: str = info.date_range[0].strftime('%Y-%m-%d %H:%M:%S ') + bar.datetime.tzinfo.key
        end: str = info.date_range[1].strftime('%Y-%m-%d %H:%M:%S ') + bar.datetime.tzinfo.key
        
        metadata = {
                    "symbol": symbol,
                    "exchange": bar.exchange.value,
                    "interval": bar.interval.value,
                    "start": start,
                    "end": end,
                    "count": count
                }

        self.bar_library.write_metadata(
            symbol=table_name,
            metadata=metadata
        )

        return True

    def save_tick_data(self, ticks: List[TickData], stream: bool = False) -> bool:
        """保存TICK数据"""
        # 转换数据为DataFrame
        data: list = []

        for tick in ticks:
            d: dict = {
                "datetime": convert_tz(tick.datetime),
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

        df: DataFrame = DataFrame.from_records(data)
        df = df.set_index('datetime')

        # 生成数据表名
        tick: TickData = ticks[0]
        symbol: str = tick.symbol
        table_name: str = generate_table_name(symbol, tick.exchange)

        # 将数据更新到数据库中
        self.tick_library.update(
            table_name, 
            df, 
            upsert=True, 
            prune_previous_versions=True
        )

        # 更新Tick线汇总数据
        info: SymbolDescription = self.tick_library.get_description(table_name)
        count: int = info.row_count
        start: str = info.date_range[0].strftime('%Y-%m-%d %H:%M:%S ') + tick.datetime.tzinfo.key
        end: str = info.date_range[1].strftime('%Y-%m-%d %H:%M:%S ') + tick.datetime.tzinfo.key
        
        metadata = {
                    "symbol": symbol,
                    "exchange": tick.exchange.value,
                    "start": start,
                    "end": end,
                    "count": count
                }

        self.tick_library.write_metadata(
            symbol=table_name,
            metadata=metadata
        )

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """读取K线数据"""
        table_name: str = generate_table_name(symbol, exchange, interval)
        df: DataFrame = self.bar_library.read(
            table_name, 
            date_range=(Timestamp(convert_tz(start)),Timestamp(convert_tz(end)))
            ).data

        if df.empty:
            return []

        df.sort_index(inplace=True)
        df = df.tz_localize(DB_TZ.key)

        bars: List[BarData] = []

        for tp in df.itertuples():
            bar: BarData = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=tp.Index.to_pydatetime(),
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
        table_name: str = generate_table_name(symbol, exchange)
        df: DataFrame = self.tick_library.read(
            table_name, 
            date_range=(Timestamp(convert_tz(start)),Timestamp(convert_tz(end)))
            ).data

        if df.empty:
            return []

        df.sort_index(inplace=True)
        df = df.tz_localize(DB_TZ.key)

        ticks: List[TickData] = []

        for tp in df.itertuples():
            tick: TickData = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=tp.Index.to_pydatetime(),
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
        table_name: str = generate_table_name(symbol, exchange, interval)

        # 查询总数据量
        info: SymbolDescription = self.bar_library.get_description(table_name)
        count: int = info.row_count

        # 删除数据
        self.bar_library.delete(table_name)

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除Tick数据"""
        # 生成表名
        table_name: str = generate_table_name(symbol, exchange)

        # 查询总数据量
        info: SymbolDescription = self.tick_library.get_description(table_name)
        count: int = info.row_count

        # 删除数据
        self.tick_library.delete(table_name)

        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """"查询数据库中的K线汇总信息"""
        overviews: List[BarOverview] = []

        table_names: list = self.bar_library.list_symbols()
        for table_name in table_names:
            metadata: dict = self.bar_library.read_metadata(table_name).metadata
            start: datetime = datetime.strptime(metadata["start"].rsplit(' ',1)[0], '%Y-%m-%d %H:%M:%S')
            end: datetime = datetime.strptime(metadata["end"].rsplit(' ',1)[0], '%Y-%m-%d %H:%M:%S')

            overview: BarOverview = BarOverview(
                symbol=metadata["symbol"],
                exchange=Exchange(metadata["exchange"]),
                interval=Interval(metadata["interval"]),
                start=start,
                end=end,
                count=metadata["count"]
            )

            overviews.append(overview)

        return overviews

    def get_tick_overview(self) -> List[TickOverview]:
        """"查询数据库中的Tick汇总信息"""
        overviews = []

        table_names = self.tick_library.list_symbols()
        for table_name in table_names:
            metadata = self.tick_library.read_metadata(table_name).metadata
            start: datetime = datetime.strptime(metadata["start"].rsplit(' ',1)[0], '%Y-%m-%d %H:%M:%S')
            end: datetime = datetime.strptime(metadata["end"].rsplit(' ',1)[0], '%Y-%m-%d %H:%M:%S')

            overview = TickOverview(
                symbol=metadata["symbol"],
                exchange=Exchange(metadata["exchange"]),
                start=start,
                end=end,
                count=metadata["count"]
            )

            overviews.append(overview)

        return overviews


def generate_table_name(symbol: str, exchange: Exchange, interval: Interval = None) -> str:
    """生成数据表名"""
    if interval:
        return f"{symbol}_{exchange.value}_{interval.value}"
    else:
        return f"{symbol}_{exchange.value}"