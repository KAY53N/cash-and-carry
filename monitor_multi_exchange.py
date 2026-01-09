"""
多交易所现货与合约价差监控器 (支持Binance, Bybit, Gate.io)
使用WebSocket实时监控，支持HTTP/SOCKS5代理
支持跨交易所价差对比
"""

import asyncio
import json
import os
from datetime import datetime
from typing import List, Dict, Tuple
import aiohttp
from aiohttp_socks import ProxyConnector


class MultiExchangeSpreadMonitor:
    """多交易所现货和合约价差监控器"""

    def __init__(self):
        # 存储实时价格数据 - 格式: {exchange: {symbol: price}}
        self.spot_prices = {
            'binance': {},
            'bybit': {},
            'gate': {}
        }
        self.futures_prices = {
            'binance': {},
            'bybit': {},
            'gate': {}
        }
        self.price_lock = asyncio.Lock()

        # 获取代理设置
        self.http_proxy = os.environ.get('https_proxy') or os.environ.get('http_proxy')
        self.socks_proxy = os.environ.get('all_proxy')

        if self.http_proxy:
            print(f"检测到HTTP代理: {self.http_proxy}", flush=True)
        if self.socks_proxy:
            print(f"检测到SOCKS代理: {self.socks_proxy}", flush=True)
        
    async def get_top_pairs_binance(self, limit: int = 100) -> List[str]:
        """从Binance API获取交易量最大的USDT交易对"""
        stablecoins = {'USDCUSDT', 'FDUSDUSDT', 'USD1USDT', 'TUSDUSDT', 'BUSDUSDT', 'USDPUSDT'}

        try:
            connector = None
            proxy = None

            if self.socks_proxy:
                connector = ProxyConnector.from_url(self.socks_proxy)
            elif self.http_proxy:
                connector = aiohttp.TCPConnector()
                proxy = self.http_proxy

            async with aiohttp.ClientSession(connector=connector) as session:
                url = 'https://api.binance.com/api/v3/ticker/24hr'
                async with session.get(url, proxy=proxy) as response:
                    if response.status == 200:
                        data = await response.json()
                        usdt_pairs = [
                            item['symbol'] for item in data
                            if item['symbol'].endswith('USDT')
                            and item['symbol'] not in stablecoins
                            and float(item['quoteVolume']) > 0
                        ]
                        # 按交易量排序
                        data_dict = {item['symbol']: float(item['quoteVolume']) for item in data}
                        usdt_pairs.sort(key=lambda x: data_dict.get(x, 0), reverse=True)
                        return usdt_pairs[:limit]
        except Exception as e:
            print(f"⚠ Binance获取交易对失败: {e}", flush=True)
        return []

    async def get_common_symbols(self, limit: int = 100) -> List[str]:
        """获取所有交易所共同支持的交易对"""
        print("正在获取交易对列表...", flush=True)
        
        # 从Binance获取热门交易对
        binance_symbols = await self.get_top_pairs_binance(limit)
        
        if not binance_symbols:
            # 备用列表
            binance_symbols = [
                'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'BNBUSDT',
                'DOGEUSDT', 'ADAUSDT', 'TRXUSDT', 'AVAXUSDT', 'LINKUSDT',
                'SUIUSDT', 'UNIUSDT', 'BCHUSDT', 'LTCUSDT', 'DOTUSDT',
                'ARBUSDT', 'OPUSDT', 'MATICUSDT', 'ATOMUSDT', 'FILUSDT'
            ]
        
        print(f"✓ 获取到{len(binance_symbols)}个交易对", flush=True)
        return binance_symbols

    def normalize_symbol(self, symbol: str, exchange: str) -> str:
        """标准化交易对符号"""
        # Binance: BTCUSDT
        # Bybit: BTCUSDT (现货和合约都一样)
        # Gate: BTC_USDT (现货) / BTC_USDT (合约)
        if exchange == 'gate':
            # Gate.io使用下划线
            if 'USDT' in symbol:
                base = symbol.replace('USDT', '')
                return f"{base}_USDT"
        return symbol

    def denormalize_symbol(self, symbol: str, exchange: str) -> str:
        """反标准化交易对符号（用于显示）"""
        if exchange == 'gate':
            return symbol.replace('_', '')
        return symbol

    async def watch_binance_spot(self, symbols: List[str]):
        """监听Binance现货价格"""
        try:
            # 分批处理，每批100个
            batch_size = 100
            for batch_id, i in enumerate(range(0, len(symbols), batch_size), 1):
                batch = symbols[i:i+batch_size]
                asyncio.create_task(self._watch_binance_spot_batch(batch, batch_id))
                await asyncio.sleep(0.5)
        except Exception as e:
            print(f"Binance现货WebSocket错误: {e}", flush=True)

    async def _watch_binance_spot_batch(self, symbols: List[str], batch_id: int):
        """Binance现货WebSocket批次"""
        streams = [f"{s.lower()}@ticker" for s in symbols]
        url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

        while True:
            try:
                connector = None
                if self.socks_proxy:
                    connector = ProxyConnector.from_url(self.socks_proxy)

                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.ws_connect(url, proxy=self.http_proxy if not self.socks_proxy else None) as ws:
                        print(f"✓ Binance现货批次{batch_id}已连接 ({len(symbols)}个)", flush=True)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if 'data' in data:
                                    ticker = data['data']
                                    symbol = ticker['s']
                                    price = float(ticker['c'])
                                    async with self.price_lock:
                                        self.spot_prices['binance'][symbol] = price
            except Exception as e:
                print(f"Binance现货批次{batch_id}断开，5秒后重连: {e}", flush=True)
                await asyncio.sleep(5)

    async def watch_binance_futures(self, symbols: List[str]):
        """监听Binance合约价格"""
        try:
            batch_size = 100
            for batch_id, i in enumerate(range(0, len(symbols), batch_size), 1):
                batch = symbols[i:i+batch_size]
                asyncio.create_task(self._watch_binance_futures_batch(batch, batch_id))
                await asyncio.sleep(0.5)
        except Exception as e:
            print(f"Binance合约WebSocket错误: {e}", flush=True)

    async def _watch_binance_futures_batch(self, symbols: List[str], batch_id: int):
        """Binance合约WebSocket批次"""
        streams = [f"{s.lower()}@ticker" for s in symbols]
        url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

        while True:
            try:
                connector = None
                if self.socks_proxy:
                    connector = ProxyConnector.from_url(self.socks_proxy)

                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.ws_connect(url, proxy=self.http_proxy if not self.socks_proxy else None) as ws:
                        print(f"✓ Binance合约批次{batch_id}已连接 ({len(symbols)}个)", flush=True)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if 'data' in data:
                                    ticker = data['data']
                                    symbol = ticker['s']
                                    price = float(ticker['c'])
                                    async with self.price_lock:
                                        self.futures_prices['binance'][symbol] = price
            except Exception as e:
                print(f"Binance合约批次{batch_id}断开，5秒后重连: {e}", flush=True)
                await asyncio.sleep(5)

    async def watch_bybit_spot(self, symbols: List[str]):
        """监听Bybit现货价格"""
        try:
            url = "wss://stream.bybit.com/v5/public/spot"

            while True:
                try:
                    connector = None
                    if self.socks_proxy:
                        connector = ProxyConnector.from_url(self.socks_proxy)

                    async with aiohttp.ClientSession(connector=connector) as session:
                        async with session.ws_connect(url, proxy=self.http_proxy if not self.socks_proxy else None) as ws:
                            # 订阅ticker
                            subscribe_msg = {
                                "op": "subscribe",
                                "args": [f"tickers.{s}" for s in symbols[:100]]  # Bybit限制
                            }
                            await ws.send_str(json.dumps(subscribe_msg))
                            print(f"✓ Bybit现货已连接 ({min(len(symbols), 100)}个)", flush=True)

                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    if data.get('topic', '').startswith('tickers.'):
                                        ticker = data.get('data', {})
                                        symbol = ticker.get('symbol', '')
                                        price = float(ticker.get('lastPrice', 0))
                                        if symbol and price > 0:
                                            async with self.price_lock:
                                                self.spot_prices['bybit'][symbol] = price
                except Exception as e:
                    print(f"Bybit现货断开，5秒后重连: {e}", flush=True)
                    await asyncio.sleep(5)
        except Exception as e:
            print(f"Bybit现货WebSocket错误: {e}", flush=True)

    async def watch_bybit_futures(self, symbols: List[str]):
        """监听Bybit合约价格"""
        try:
            url = "wss://stream.bybit.com/v5/public/linear"

            while True:
                try:
                    connector = None
                    if self.socks_proxy:
                        connector = ProxyConnector.from_url(self.socks_proxy)

                    async with aiohttp.ClientSession(connector=connector) as session:
                        async with session.ws_connect(url, proxy=self.http_proxy if not self.socks_proxy else None) as ws:
                            # 订阅ticker
                            subscribe_msg = {
                                "op": "subscribe",
                                "args": [f"tickers.{s}" for s in symbols[:100]]
                            }
                            await ws.send_str(json.dumps(subscribe_msg))
                            print(f"✓ Bybit合约已连接 ({min(len(symbols), 100)}个)", flush=True)

                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    if data.get('topic', '').startswith('tickers.'):
                                        ticker = data.get('data', {})
                                        symbol = ticker.get('symbol', '')
                                        price = float(ticker.get('lastPrice', 0))
                                        if symbol and price > 0:
                                            async with self.price_lock:
                                                self.futures_prices['bybit'][symbol] = price
                except Exception as e:
                    print(f"Bybit合约断开，5秒后重连: {e}", flush=True)
                    await asyncio.sleep(5)
        except Exception as e:
            print(f"Bybit合约WebSocket错误: {e}", flush=True)

    async def watch_gate_spot(self, symbols: List[str]):
        """监听Gate.io现货价格"""
        try:
            url = "wss://api.gateio.ws/ws/v4/"

            # 转换符号格式
            gate_symbols = [self.normalize_symbol(s, 'gate') for s in symbols]

            while True:
                try:
                    connector = None
                    if self.socks_proxy:
                        connector = ProxyConnector.from_url(self.socks_proxy)

                    async with aiohttp.ClientSession(connector=connector) as session:
                        async with session.ws_connect(url, proxy=self.http_proxy if not self.socks_proxy else None) as ws:
                            # 订阅ticker
                            subscribe_msg = {
                                "time": int(datetime.now().timestamp()),
                                "channel": "spot.tickers",
                                "event": "subscribe",
                                "payload": gate_symbols[:100]
                            }
                            await ws.send_str(json.dumps(subscribe_msg))
                            print(f"✓ Gate现货已连接 ({min(len(symbols), 100)}个)", flush=True)

                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    if data.get('channel') == 'spot.tickers' and data.get('event') == 'update':
                                        ticker = data.get('result', {})
                                        symbol = ticker.get('currency_pair', '')
                                        price = float(ticker.get('last', 0))
                                        if symbol and price > 0:
                                            # 转换回标准格式
                                            std_symbol = self.denormalize_symbol(symbol, 'gate')
                                            async with self.price_lock:
                                                self.spot_prices['gate'][std_symbol] = price
                except Exception as e:
                    print(f"Gate现货断开，5秒后重连: {e}", flush=True)
                    await asyncio.sleep(5)
        except Exception as e:
            print(f"Gate现货WebSocket错误: {e}", flush=True)

    async def watch_gate_futures(self, symbols: List[str]):
        """监听Gate.io合约价格"""
        try:
            url = "wss://fx-ws.gateio.ws/v4/ws/usdt"

            # 转换符号格式
            gate_symbols = [self.normalize_symbol(s, 'gate') for s in symbols]

            while True:
                try:
                    connector = None
                    if self.socks_proxy:
                        connector = ProxyConnector.from_url(self.socks_proxy)

                    async with aiohttp.ClientSession(connector=connector) as session:
                        async with session.ws_connect(url, proxy=self.http_proxy if not self.socks_proxy else None) as ws:
                            # 订阅ticker
                            subscribe_msg = {
                                "time": int(datetime.now().timestamp()),
                                "channel": "futures.tickers",
                                "event": "subscribe",
                                "payload": gate_symbols[:100]
                            }
                            await ws.send_str(json.dumps(subscribe_msg))
                            print(f"✓ Gate合约已连接 ({min(len(symbols), 100)}个)", flush=True)

                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    if data.get('channel') == 'futures.tickers' and data.get('event') == 'update':
                                        ticker = data.get('result', [])[0] if data.get('result') else {}
                                        symbol = ticker.get('contract', '')
                                        price = float(ticker.get('last', 0))
                                        if symbol and price > 0:
                                            # 转换回标准格式
                                            std_symbol = self.denormalize_symbol(symbol, 'gate')
                                            async with self.price_lock:
                                                self.futures_prices['gate'][std_symbol] = price
                except Exception as e:
                    print(f"Gate合约断开，5秒后重连: {e}", flush=True)
                    await asyncio.sleep(5)
        except Exception as e:
            print(f"Gate合约WebSocket错误: {e}", flush=True)

    async def get_all_spreads(self, symbols: List[str]) -> List[Dict]:
        """获取所有可能的价差组合（跨交易所）"""
        spreads = []

        async with self.price_lock:
            for symbol in symbols:
                # 收集所有交易所的现货和合约价格
                spot_data = {}
                futures_data = {}

                for exchange in ['binance', 'bybit', 'gate']:
                    if symbol in self.spot_prices[exchange]:
                        spot_data[exchange] = self.spot_prices[exchange][symbol]
                    if symbol in self.futures_prices[exchange]:
                        futures_data[exchange] = self.futures_prices[exchange][symbol]

                # 计算所有可能的价差组合
                for spot_ex, spot_price in spot_data.items():
                    for futures_ex, futures_price in futures_data.items():
                        if spot_price > 0 and futures_price > 0:
                            spread = futures_price - spot_price
                            spread_percent = (spread / spot_price) * 100

                            # 交易所标识
                            if spot_ex == futures_ex:
                                exchange_label = spot_ex.upper()
                            else:
                                exchange_label = f"{spot_ex.upper()}→{futures_ex.upper()}"

                            spreads.append({
                                'symbol': symbol,
                                'spot_exchange': spot_ex,
                                'futures_exchange': futures_ex,
                                'exchange_label': exchange_label,
                                'spot_price': spot_price,
                                'futures_price': futures_price,
                                'spread': spread,
                                'spread_percent': spread_percent,
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            })

        return spreads

    async def display_spreads(self, symbols: List[str], interval: int = 1):
        """显示价差数据"""
        await asyncio.sleep(5)  # 等待WebSocket连接并接收初始数据

        while True:
            try:
                # 获取所有价差组合
                all_spreads = await self.get_all_spreads(symbols)

                # 按价差百分比绝对值从大到小排序
                all_spreads.sort(key=lambda x: abs(x['spread_percent']), reverse=True)

                # 只显示前20个
                top_spreads = all_spreads[:20]

                # 清屏并显示结果
                print("\033[2J\033[H")  # 清屏
                print(f"🔴 多交易所价差监控 - 实时更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 150)
                print(f"{'排名':<6}{'交易对':<14}{'现货所':<10}{'合约所':<10}{'现货价格':<18}{'合约价格':<18}{'价差':<18}{'价差%':<16}")
                print("=" * 150)

                for idx, data in enumerate(top_spreads, 1):
                    spread_color = "\033[92m" if data['spread_percent'] > 0 else "\033[91m"
                    reset_color = "\033[0m"

                    # 格式化交易所名称（缩写）
                    spot_ex = data['spot_exchange'].upper()[:7]  # 最多7个字符
                    futures_ex = data['futures_exchange'].upper()[:7]

                    print(f"{idx:<6}"
                          f"{data['symbol']:<14}"
                          f"{spot_ex:<10}"
                          f"{futures_ex:<10}"
                          f"{data['spot_price']:<18.8f}"
                          f"{data['futures_price']:<18.8f}"
                          f"{spread_color}{data['spread']:<18.8f}{reset_color}"
                          f"{spread_color}{data['spread_percent']:>+14.4f}%{reset_color}")

                print("=" * 150)

                # 统计数据源
                async with self.price_lock:
                    stats = []
                    for ex in ['binance', 'bybit', 'gate']:
                        spot_count = len(self.spot_prices[ex])
                        futures_count = len(self.futures_prices[ex])
                        stats.append(f"{ex.upper()}(现货{spot_count}/合约{futures_count})")

                print(f"📊 数据源: {' | '.join(stats)} | 按Ctrl+C退出")

                await asyncio.sleep(interval)

            except KeyboardInterrupt:
                print("\n\n监控已停止")
                break
            except Exception as e:
                print(f"\n显示出错: {e}")
                await asyncio.sleep(interval)

    async def monitor_spreads(self, limit: int = 100):
        """启动监控"""
        print("=" * 80)
        print("多交易所现货与合约价差监控器")
        print("支持: Binance, Bybit, Gate.io")
        print("=" * 80)

        # 获取交易对
        symbols = await self.get_common_symbols(limit)

        print(f"\n监控的交易对数量: {len(symbols)}")
        print(f"前10个: {', '.join(symbols[:10])}")
        print("\n正在连接WebSocket...\n")

        # 启动所有WebSocket连接
        tasks = [
            # Binance
            asyncio.create_task(self.watch_binance_spot(symbols)),
            asyncio.create_task(self.watch_binance_futures(symbols)),
            # Bybit
            asyncio.create_task(self.watch_bybit_spot(symbols)),
            asyncio.create_task(self.watch_bybit_futures(symbols)),
            # Gate.io
            asyncio.create_task(self.watch_gate_spot(symbols)),
            asyncio.create_task(self.watch_gate_futures(symbols)),
            # 显示
            asyncio.create_task(self.display_spreads(symbols))
        ]

        await asyncio.gather(*tasks)


async def main():
    monitor = MultiExchangeSpreadMonitor()
    await monitor.monitor_spreads(limit=100)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序已退出")

