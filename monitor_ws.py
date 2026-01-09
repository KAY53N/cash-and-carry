"""
Binance 现货与合约价差监控器 (websockets库版本 - 支持代理)
使用websockets库实时监控，支持HTTP/SOCKS5代理
"""

import asyncio
import websockets
import json
import os
from datetime import datetime
from typing import List, Dict
import aiohttp
from aiohttp_socks import ProxyConnector


class BinanceSpreadMonitor:
    """Binance现货和合约价差监控器（websockets库版本 - 支持代理）"""

    def __init__(self):
        # 存储实时价格数据
        self.spot_prices = {}
        self.futures_prices = {}
        self.price_lock = asyncio.Lock()

        # 获取代理设置
        self.http_proxy = os.environ.get('https_proxy') or os.environ.get('HTTPS_PROXY') or os.environ.get('http_proxy') or os.environ.get('HTTP_PROXY')
        self.socks_proxy = os.environ.get('all_proxy') or os.environ.get('ALL_PROXY')

        if self.http_proxy:
            print(f"检测到HTTP代理: {self.http_proxy}", flush=True)
        if self.socks_proxy:
            print(f"检测到SOCKS代理: {self.socks_proxy}", flush=True)
        
    async def get_top_pairs(self, limit: int = 200) -> List[str]:
        """从Binance API获取交易量最大的USDT交易对"""
        import aiohttp

        # 稳定币列表（排除这些）
        stablecoins = {'USDCUSDT', 'FDUSDUSDT', 'USD1USDT', 'TUSDUSDT', 'BUSDUSDT', 'USDPUSDT'}

        try:
            # 创建代理连接器
            connector = None
            proxy = None

            if self.socks_proxy:
                connector = ProxyConnector.from_url(self.socks_proxy)
            elif self.http_proxy:
                connector = aiohttp.TCPConnector()
                proxy = self.http_proxy

            async with aiohttp.ClientSession(connector=connector) as session:
                # 获取现货24小时交易数据
                url = 'https://api.binance.com/api/v3/ticker/24hr'
                async with session.get(url, proxy=proxy) as response:
                    if response.status == 200:
                        data = await response.json()

                        # 筛选USDT交易对，排除稳定币
                        usdt_pairs = [
                            item for item in data
                            if item['symbol'].endswith('USDT')
                            and item['symbol'] not in stablecoins
                            and float(item['quoteVolume']) > 0
                        ]

                        # 按交易量排序
                        usdt_pairs.sort(key=lambda x: float(x['quoteVolume']), reverse=True)

                        # 返回前N个交易对
                        top_pairs = [item['symbol'] for item in usdt_pairs[:limit]]

                        print(f"✓ 成功获取前{len(top_pairs)}个交易对", flush=True)
                        return top_pairs
                    else:
                        print(f"⚠ API请求失败，状态码: {response.status}", flush=True)
                        return self.get_default_pairs()
        except Exception as e:
            print(f"⚠ 获取交易对失败: {e}，使用默认列表", flush=True)
            return self.get_default_pairs()

    def get_default_pairs(self) -> List[str]:
        """返回预定义的热门交易对（备用）"""
        return [
            'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'BNBUSDT',
            'DOGEUSDT', 'ADAUSDT', 'TRXUSDT', 'AVAXUSDT', 'LINKUSDT',
            'SUIUSDT', 'UNIUSDT', 'BCHUSDT', 'LTCUSDT', 'DOTUSDT',
            'MATICUSDT', 'ATOMUSDT', 'FILUSDT', 'ARBUSDT', 'OPUSDT'
        ]
    
    async def watch_spot_tickers_batch(self, symbols: List[str], batch_id: int):
        """监听一批现货ticker数据"""
        # 使用组合流
        streams = '/'.join([f"{symbol.lower()}@ticker" for symbol in symbols])
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"

        while True:
            try:
                # 使用aiohttp的WebSocket客户端，支持代理
                connector = None
                if self.socks_proxy:
                    # 使用SOCKS代理
                    connector = ProxyConnector.from_url(self.socks_proxy)
                elif self.http_proxy:
                    # 使用HTTP代理
                    connector = aiohttp.TCPConnector()

                async with aiohttp.ClientSession(connector=connector) as session:
                    proxy = self.http_proxy if self.http_proxy and not self.socks_proxy else None
                    async with session.ws_connect(url, proxy=proxy) as ws:
                        print(f"✓ 现货WebSocket批次{batch_id}已连接 ({len(symbols)}个币种)", flush=True)

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if 'data' in data:
                                    ticker = data['data']
                                    symbol = ticker['s']
                                    price = float(ticker['c'])
                                    async with self.price_lock:
                                        self.spot_prices[symbol] = price
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                print(f"现货WebSocket批次{batch_id}错误: {ws.exception()}", flush=True)
                                break
            except Exception as e:
                print(f"现货WebSocket批次{batch_id}异常: {e}，5秒后重连...", flush=True)
                await asyncio.sleep(5)

    async def watch_spot_tickers(self, symbols: List[str]):
        """监听现货ticker数据（支持大量币种，自动分批）"""
        print(f"正在连接现货WebSocket（共{len(symbols)}个币种）...", flush=True)

        # 每批100个币种
        batch_size = 100
        tasks = []

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            task = asyncio.create_task(self.watch_spot_tickers_batch(batch, i//batch_size + 1))
            tasks.append(task)
            # 稍微延迟，避免同时建立太多连接
            await asyncio.sleep(0.5)

        # 等待所有任务
        await asyncio.gather(*tasks)
    
    async def watch_futures_tickers_batch(self, symbols: List[str], batch_id: int):
        """监听一批合约ticker数据"""
        # 使用组合流
        streams = '/'.join([f"{symbol.lower()}@ticker" for symbol in symbols])
        url = f"wss://fstream.binance.com/stream?streams={streams}"

        while True:
            try:
                # 使用aiohttp的WebSocket客户端，支持代理
                connector = None
                if self.socks_proxy:
                    # 使用SOCKS代理
                    connector = ProxyConnector.from_url(self.socks_proxy)
                elif self.http_proxy:
                    # 使用HTTP代理
                    connector = aiohttp.TCPConnector()

                async with aiohttp.ClientSession(connector=connector) as session:
                    proxy = self.http_proxy if self.http_proxy and not self.socks_proxy else None
                    async with session.ws_connect(url, proxy=proxy) as ws:
                        print(f"✓ 合约WebSocket批次{batch_id}已连接 ({len(symbols)}个币种)", flush=True)

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if 'data' in data:
                                    ticker = data['data']
                                    symbol = ticker['s']
                                    price = float(ticker['c'])
                                    async with self.price_lock:
                                        self.futures_prices[symbol] = price
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                print(f"合约WebSocket批次{batch_id}错误: {ws.exception()}", flush=True)
                                break
            except Exception as e:
                print(f"合约WebSocket批次{batch_id}异常: {e}，5秒后重连...", flush=True)
                await asyncio.sleep(5)

    async def watch_futures_tickers(self, symbols: List[str]):
        """监听合约ticker数据（支持大量币种，自动分批）"""
        print(f"正在连接合约WebSocket（共{len(symbols)}个币种）...", flush=True)

        # 每批100个币种
        batch_size = 100
        tasks = []

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            task = asyncio.create_task(self.watch_futures_tickers_batch(batch, i//batch_size + 1))
            tasks.append(task)
            # 稍微延迟，避免同时建立太多连接
            await asyncio.sleep(0.5)

        # 等待所有任务
        await asyncio.gather(*tasks)
    
    async def get_spread_data(self, symbol: str) -> Dict:
        """获取单个交易对的价差数据"""
        async with self.price_lock:
            spot_price = self.spot_prices.get(symbol, 0.0)
            futures_price = self.futures_prices.get(symbol, 0.0)
        
        if spot_price > 0 and futures_price > 0:
            spread = futures_price - spot_price
            spread_percent = (spread / spot_price) * 100
            
            return {
                'symbol': symbol,
                'spot_price': spot_price,
                'futures_price': futures_price,
                'spread': spread,
                'spread_percent': spread_percent,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        return None
    
    async def display_spreads(self, symbols: List[str], interval: int = 1):
        """显示价差数据"""
        await asyncio.sleep(3)  # 等待WebSocket连接并接收初始数据
        
        while True:
            try:
                # 获取所有交易对的价差数据
                tasks = [self.get_spread_data(symbol) for symbol in symbols]
                results = await asyncio.gather(*tasks)
                
                # 过滤有效数据并按价差百分比绝对值从大到小排序
                valid_results = [r for r in results if r is not None]
                valid_results.sort(key=lambda x: abs(x['spread_percent']), reverse=True)

                # 只显示前20个
                top_results = valid_results[:20]

                # 清屏并显示结果
                print("\033[2J\033[H")  # 清屏
                print(f"🔴 实时更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (WebSocket)")
                print("=" * 120)
                print(f"{'排名':<6}{'交易对':<15}{'现货价格':<18}{'合约价格':<18}{'价差':<18}{'价差%':<12}")
                print("=" * 120)

                for idx, data in enumerate(top_results, 1):
                    spread_color = "\033[92m" if data['spread_percent'] > 0 else "\033[91m"
                    reset_color = "\033[0m"
                    
                    print(f"{idx:<6}{data['symbol']:<15}"
                          f"{data['spot_price']:<18.8f}"
                          f"{data['futures_price']:<18.8f}"
                          f"{spread_color}{data['spread']:<18.8f}"
                          f"{data['spread_percent']:>+10.4f}%{reset_color}")
                
                print("=" * 120)
                async with self.price_lock:
                    spot_count = len(self.spot_prices)
                    futures_count = len(self.futures_prices)
                print(f"📊 数据源: 现货({spot_count}个) | 合约({futures_count}个) | 按Ctrl+C退出")
                
                await asyncio.sleep(interval)
                
            except KeyboardInterrupt:
                print("\n\n监控已停止")
                break
            except Exception as e:
                print(f"\n显示出错: {e}")
                await asyncio.sleep(interval)

    async def monitor_spreads(self, update_interval: int = 1, top_n: int = 200):
        """监控价差（websockets库版本）"""
        import sys

        # 获取前N个交易对
        print(f"正在获取前{top_n}个交易对...", flush=True)
        symbols = await self.get_top_pairs(limit=top_n)

        print(f"\n监控的交易对数量: {len(symbols)}", flush=True)
        print(f"前10个: {', '.join(symbols[:10])}", flush=True)
        print(f"显示更新间隔: {update_interval}秒", flush=True)
        print("正在连接WebSocket...", flush=True)
        print("=" * 120, flush=True)
        sys.stdout.flush()

        # 创建三个并发任务
        tasks = [
            asyncio.create_task(self.watch_spot_tickers(symbols)),
            asyncio.create_task(self.watch_futures_tickers(symbols)),
            asyncio.create_task(self.display_spreads(symbols, update_interval))
        ]

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            print("\n\n正在关闭...")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def main():
    """主函数"""
    monitor = BinanceSpreadMonitor()
    # 监控前200个交易对
    await monitor.monitor_spreads(update_interval=1, top_n=200)


if __name__ == '__main__':
    import sys
    print("=" * 120, flush=True)
    print("Binance 现货与合约价差监控器 (WebSocket版本)", flush=True)
    print("监控前200个交易量最大的币种 - 完全免费", flush=True)
    print("=" * 120, flush=True)
    sys.stdout.flush()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序已退出")

