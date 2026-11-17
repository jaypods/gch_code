"""Direct local runtime: independent market, strategy and execution loops.

No webpage/chart, watchdog, release report or historical audit is on the
execution path. Only the user's local start action enables financial writes.
Each thread owns its SQLite connection and transport; charts use a fourth,
public-only connection outside this service.
"""
from contextlib import closing
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from threading import Event,Lock,Thread
import time

from .account import InvalidState
from .binance_transport import BinanceTransport,WireReply,TransportError
from .combined_inputs import CombinedInputs,CombinedPreparation
from .credentials import load_local_credentials
from .live_source_inputs import LiveSourceInputs
from .market_metadata import parse_exchange_info
from .simple_broker import SimpleBroker
from .simple_cash import SimpleCash
from .simple_engine import SimpleEngine
from .simple_dependency import DependencyEngine
from .simple_exchange import SimpleExchange
from .simple_executor import SimpleExecutor
from .simple_feed import SimpleFeed,SimpleMappings
from .simple_history import SimpleHistory
from .simple_inputs import SimpleInputData,SYMBOLS
from .simple_market import SimpleMarket
from .simple_resume import SimpleResume
from .simple_store import SimpleStore,PERIODS

MINUTE=60000
FIVE=300000


class DirectControl:
    def __init__(self):
        self.lock=Lock();self.stop=Event();self.enabled=False;self.entries_allowed=False
        self.needs=();self.funding_window=None;self.messages={}
        self._clock=None;self._mono=None

    def start(self):
        with self.lock:
            self.enabled=True;self.entries_allowed=True
        return dict(accepted=True)

    def pause(self):
        with self.lock:
            self.entries_allowed=False
        return dict(accepted=True)

    def switches(self):
        with self.lock:return self.enabled,self.entries_allowed

    def set_clock(self, now_ms):
        with self.lock:self._clock=now_ms;self._mono=time.monotonic()

    def now_ms(self):
        with self.lock:
            return int(time.time()*1000) if self._clock is None else self._clock+int((time.monotonic()-self._mono)*1000)

    def request_data(self, needs, funding_window=None):
        with self.lock:self.needs=tuple(needs);self.funding_window=funding_window

    def data_request(self):
        with self.lock:return self.needs,self.funding_window

    def message(self, name, text=None):
        with self.lock:
            if text is None:self.messages.pop(name,None)
            else:self.messages[name]=text

    def display(self, *, strategy_mode):
        with self.lock:
            mode=('paused' if self.enabled and not self.entries_allowed else strategy_mode if self.enabled else 'stopped')
            message='；'.join(self.messages[k] for k in ('account','execution','strategy','market') if k in self.messages)
            if not message:message='已有持仓继续按原策略处理' if mode=='paused' else '交易后自动汇总成交与历史 K 线' if mode=='running' else '点击“开始策略”后按原策略运行'
            return dict(mode=mode,message=message,enabled=self.enabled,entries_allowed=self.entries_allowed,
                        engine_version='dependency-live-1')


def public_transport():
    return BinanceTransport(clock_max_age_ms=120000)


def _sync(transport):
    if transport.clock_age_ms is None or transport.clock_age_ms>=30000:
        transport.sync_clock()


def _message(exc, label):
    # Do not persist HTTP exception strings, response bodies or credentials.
    if isinstance(exc,TransportError):
        code='' if exc.exchange_code is None else '（'+str(exc.exchange_code)+'）'
        return label+'暂未收到接口响应'+code
    if isinstance(exc,InvalidState):
        text=str(exc)
        if len(text)<160 and 'http' not in text.lower():return label+'：'+text
    return label+'暂未完成（'+type(exc).__name__+'）'


class DirectRuntime:
    def __init__(self, path, *, legacy_market=None, control=None, public_factory=public_transport,
                 private_factory=None):
        self.path=Path(path);self.legacy_market=legacy_market
        self.control=control or DirectControl();self.public_factory=public_factory
        self.private_factory=private_factory or (lambda:BinanceTransport(load_local_credentials(),clock_max_age_ms=120000))
        self.threads=[]
        with closing(SimpleStore(self.path)) as store:
            DependencyEngine(store)  # Decode/migrate in memory; writes remain user-controlled.
            # Opening/restarting the application never re-enables old writes.
            store.set_value('runtime_display',self.control.display(strategy_mode='stopped'))

    def start_background(self):
        if self.threads:return
        for name,fn in (('c801-market',self._market),('c801-strategy',self._strategy),('c801-execution',self._execution)):
            thread=Thread(target=fn,name=name,daemon=True);self.threads.append(thread);thread.start()

    def close(self):
        self.control.stop.set()
        for thread in self.threads:thread.join(timeout=4)

    def _market(self):
        transport=None;feed=None
        with closing(SimpleStore(self.path)) as store:
            sources=None;catalog_at=0;schedule_at=0;last_fill=0
            try:
                transport=self.public_factory();feed=SimpleFeed(transport,store,legacy_path=self.legacy_market)
                data=SimpleInputData(store,transport)
                while not self.control.stop.is_set():
                    try:
                        _sync(transport);now=transport.now_ms();self.control.set_clock(now)
                        if sources is None or now-catalog_at>=120000:
                            universe=feed.refresh_catalog();now=transport.now_ms()
                            if sources is None:sources=LiveSourceInputs(feed.market,universe,now_ms=now,mappings=feed.mappings)
                            else:sources.refresh(universe,now_ms=now,allow_expansion=True)
                            feed.subscribe(sources,now_ms=now);catalog_at=now
                        feed.poll(budget_ms=35,maximum_events=2000)
                        now=transport.now_ms()
                        if now-schedule_at>=45000:
                            for symbol in SYMBOLS:
                                try:data.observe_schedule(symbol)
                                except Exception as exc:self.control.message('market',_message(exc,'主流币资金费日历'))
                            schedule_at=transport.now_ms()
                        needs,window=self.control.data_request()
                        now=transport.now_ms()
                        if window and window[0]<window[1]<=now and not data.funding_ready(start_ms=window[0],end_ms=window[1],now_ms=now):
                            try:data.fetch_funding_one(start_ms=window[0],end_ms=window[1])
                            except Exception as exc:self.control.message('market',_message(exc,'主流币模型资金费'))
                        bounded=[]
                        for need in needs:
                            end=min(need.end_ms,now//PERIODS[need.interval]*PERIODS[need.interval])
                            if need.start_ms<end:bounded.append(replace(need,end_ms=end))
                        if bounded and now-last_fill>=150:
                            result=feed.fill_one(bounded);last_fill=transport.now_ms()
                            self.control.message('market',None if result['complete'] else '正在补充 '+result['symbol']+' 的策略行情')
                        else:self.control.message('market',None)
                    except Exception as exc:
                        self.control.message('market',_message(exc,'行情'))
                        self.control.stop.wait(.5)
                    self.control.stop.wait(.03)
            except Exception as exc:
                self.control.message('market',_message(exc,'行情连接'))
            finally:
                if feed is not None:feed.close()
                if transport is not None:transport.close()

    def _execution(self):
        with closing(SimpleStore(self.path)) as store:
            broker=SimpleBroker(store);cash=SimpleCash(store,broker)
            transport=None;last_account=0;last_income=0
            try:
                while not self.control.stop.is_set():
                    try:
                        if transport is None:
                            transport=self.private_factory()
                            exchange=SimpleExchange(transport,store,broker);history=SimpleHistory(exchange)
                            executor=SimpleExecutor(exchange,cash,history)
                        _sync(transport);now=transport.now_ms();enabled,entries=self.control.switches()
                        transport.mutations_enabled=enabled
                        if not last_account or now-last_account>=15000 or enabled and store.value('initial_capital') is None:
                            snapshot=exchange.account().snapshot;last_account=transport.now_ms()
                            if enabled:cash.initialize(snapshot,now_ms=last_account)
                            self.control.message('account',None)
                        initial=store.value('initial_capital')
                        if initial and now-last_income>=60000:
                            start=store.value('income_cursor',initial['created_ms']+1);end=now-2000
                            if start<=end:
                                history.schedule('INCOME',start_ms=start,end_ms=end)
                                store.set_value('income_cursor',end+1)
                            last_income=now
                        if initial:
                            result=executor.step(enabled=enabled,entries_allowed=entries)
                            self.control.message('execution',None if not result['errors'] else '正在处理订单或成交返回：'+','.join(e['phase'] for e in result['errors']))
                        else:self.control.message('execution',None)
                    except Exception as exc:
                        self.control.message('account',_message(exc,'账户连接'))
                        if isinstance(exc,TransportError) and exc.http_status in (401,403,451):
                            transport.close();transport=None;last_account=0
                        self.control.stop.wait(2)
                    self.control.stop.wait(.2)
            finally:
                if transport is not None:transport.close()

    def _strategy(self):
        with closing(SimpleStore(self.path)) as store:
            engine=DependencyEngine(store);market=SimpleMarket(store);data=SimpleInputData(store,self.control)
            sources=None;catalog_at=None;mode='stopped';last_display=0
            while not self.control.stop.is_set():
                try:
                    now=self.control.now_ms();enabled,entries=self.control.switches()
                    raw=store.value('market_catalog')
                    if raw is None:
                        self.control.message('strategy','等待当前合约目录');mode='data_wait'
                        self.control.stop.wait(.2);continue
                    if raw['received_ms']!=catalog_at:
                        universe=parse_exchange_info(WireReply(raw['body'],raw['sent_ms'],raw['received_ms']))
                        if sources is None:
                            sources=LiveSourceInputs(market,universe,now_ms=now,mappings=SimpleMappings(store))
                            if engine.roster:sources.retain_stock_roster(engine.roster)
                        else:sources.refresh(universe,now_ms=now,allow_expansion=True)
                        catalog_at=raw['received_ms']
                    if not enabled:
                        mode='stopped';self.control.message('strategy',None)
                        self.control.stop.wait(.2);continue
                    if engine.state is None:
                        initial=store.value('initial_capital')
                        if initial is None:
                            mode='starting';self.control.message('strategy','正在读取账户可用资金')
                            self.control.stop.wait(.2);continue
                        engine.initialize(now_ms=now,initial=initial['amounts'],symbols=sources.stock_symbols)
                    result=engine.step(sources,data,now_ms=now,clock=self.control.now_ms)
                    self.control.request_data(result['needs'],result['funding_window'])
                    waits=result['waiting'];mode='running'
                    message='按品种与必要关联分别运行'
                    if waits:message+='；等待相关数据：'+'、'.join(list(waits)[:3])+(' 等 '+str(len(waits))+' 项' if len(waits)>3 else '')
                    self.control.message('strategy',message)
                    store.set_value('dependency_display',dict(waiting=waits,updated_ms=now,revision=result['revision']))
                except Exception as exc:
                    self.control.request_data(tuple(dict.fromkeys(engine.requests)),engine.funding_window)
                    mode='data_wait';self.control.message('strategy',_message(exc,'策略输入'))
                    self.control.stop.wait(.2)
                finally:
                    now=self.control.now_ms()
                    if now-last_display>=500:
                        store.set_value('runtime_display',self.control.display(strategy_mode=mode));last_display=now
                    self.control.stop.wait(.1)
