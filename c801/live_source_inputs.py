"""Actual-arrival stock/alt source inputs for the shared CombinedController.

The current exchange universe is never a historical point-in-time universe.
Backfilled history may warm today's features but cannot create past decisions.
Roster expansion needs explicit observer migration by the owner scheduler;
silently retaining an incomplete universe or dropping an owned symbol is refused.
"""
import numpy as np
from .account import InvalidState
from .market_metadata import MarketUniverse
from .market_warmup import CandleNeed
from .strategies.alt_sources import HourlyWindow,FlowBar,FlowWindow,ranked,HOUR,STEP
from .strategies.stock_sources import StockObservation,StockSourceObserver,LIQUIDITY_BARS


class LiveSourceInputs:
    def __init__(self,store,universe,*,now_ms,mappings):
        self.store,self.mappings=store,mappings
        self._fresh(universe,now_ms)
        self.crypto_names=tuple(sorted(universe.symbols('main',now_ms=now_ms)+universe.symbols('alt',now_ms=now_ms)))
        self.stock_symbols=universe.symbols('stock_us',now_ms=now_ms)
        if not {'BTCUSDT','ETHUSDT'}<=set(self.crypto_names) or not self.stock_symbols:
            raise InvalidState('完整组合的主流币或股票观察范围不可用')
        self.universe=universe;self.contracts={c.symbol:c for c in universe.contracts}

    @staticmethod
    def _fresh(universe,now):
        if not isinstance(universe,MarketUniverse) or type(now) is not int or not 0<=now-universe.received_ms<=300000:
            raise InvalidState('当前合约完整目录过期或尚未收到')

    def retain_stock_roster(self,symbols):
        """Restart keeps original source ranks, including nontrading old members."""
        symbols=tuple(symbols)
        if (not symbols or len(set(symbols))!=len(symbols)
                or any(s not in self.contracts or self.contracts[s].category!='stock_us' for s in symbols)):
            raise InvalidState('恢复股票范围缺少原合约身份或发生分类变化')
        self.stock_symbols=symbols+tuple(sorted(set(self.stock_symbols)-set(symbols)))

    def refresh(self,universe,*,now_ms,allow_expansion=False):
        self._fresh(universe,now_ms)
        if type(allow_expansion) is not bool:raise InvalidState('合约扩展标记无效')
        stocks=set(universe.symbols('stock_us',now_ms=now_ms))
        cryptos=set(universe.symbols('main',now_ms=now_ms)+universe.symbols('alt',now_ms=now_ms))
        available={c.symbol:c for c in universe.contracts}
        if not allow_expansion and (stocks-set(self.stock_symbols) or cryptos-set(self.crypto_names)):
            raise InvalidState('出现新上市品种，必须显式扩展策略观察器与预热范围')
        if any(s not in available for s in (*self.stock_symbols,*self.crypto_names)):
            raise InvalidState('旧观察品种从目录消失，先核对持仓与到期状态')
        if any(available[s].category!=self.contracts[s].category or available[s].listed_ms!=self.contracts[s].listed_ms
               for s in (*self.stock_symbols,*self.crypto_names)):
            raise InvalidState('旧合约身份或上市时间改变，需要核对复用代码')
        self.stock_symbols+=tuple(sorted(stocks-set(self.stock_symbols)))
        self.crypto_names=tuple(sorted(set(self.crypto_names)|cryptos))
        self.universe=universe;self.contracts=available

    def _stock_additions(self,previous):
        previous=tuple(previous)
        if (not previous or self.stock_symbols[:len(previous)]!=previous
                or len(set(previous))!=len(previous)):
            raise InvalidState('股票新范围必须保留原观察器排序')
        return self.stock_symbols[len(previous):]

    def stock_admission_needs(self,previous,*,through_ms):
        from .stock_admission import warmup_ends
        needs=[]
        for s in self._stock_additions(previous):
            ends=warmup_ends(through_ms,self.contracts[s].listed_ms)
            if ends:needs.append(CandleNeed('perpetual',s,'5m',ends.start-STEP,through_ms))
        return tuple(needs)

    def stock_admission(self,previous,*,through_ms,known_ms):
        from .stock_admission import warmup_ends
        self._current(known_ms);added=self._stock_additions(previous)
        if not added:return None
        observations={}
        for s in added:
            c=self.contracts[s];ends=warmup_ends(through_ms,c.listed_ms)
            if ends:
                rows,_=self.store.window('perpetual',s,'5m',start_ms=ends.start-STEP,end_ms=through_ms,now_ms=known_ms)
                observations[s]=tuple(StockObservation(o.bar,'present',c.known_ms) for o in rows)
            else:observations[s]=()
        return dict(previous_symbols=tuple(previous),target_symbols=self.stock_symbols,through_ms=through_ms,
            catalog_known_ms=self.universe.received_ms,catalog_digest=self.universe.source_digest,
            contracts=tuple(self.contracts[s] for s in added),observations=observations)

    def _current(self,now):self._fresh(self.universe,now)

    def stock_needs(self,start_ms):
        if type(start_ms) is not int or start_ms%STEP:raise InvalidState('股票预热起点错误')
        end=start_ms-STEP;begin=end-(LIQUIDITY_BARS+1)*STEP;needs=[]
        for symbol in self.stock_symbols:
            c=self.contracts[symbol];first=max(begin,c.listed_ms//STEP*STEP)
            last=min(end,c.delivery_ms//STEP*STEP)
            if first<last:needs.append(CandleNeed('perpetual',symbol,'5m',first,last))
        return tuple(needs)

    def stock_frame(self,end_ms,*,known_ms):
        self._current(known_ms)
        rows,_=self.store.frame('perpetual',self.stock_symbols,'5m',end_ms=end_ms,now_ms=known_ms)
        observations={}
        for symbol in self.stock_symbols:
            c=self.contracts[symbol];o=rows.get(symbol)
            if end_ms<=c.listed_ms:
                if o is not None:raise InvalidState('上市前出现行情，与已知身份矛盾')
                observations[symbol]=StockObservation(None,'not_listed',c.known_ms)
            elif end_ms-STEP>=c.delivery_ms:
                observations[symbol]=StockObservation(None,'quarantined',c.known_ms)
            elif o is None:
                raise InvalidState('股票行情存在未解释缺口：'+symbol)
            else:observations[symbol]=StockObservation(o.bar,'present',c.known_ms)
        return observations

    def prepare_stock(self,*,start_ms,now_ms):
        self._current(now_ms)
        if type(start_ms) is not int or start_ms%STEP or not start_ms-STEP<=now_ms<start_ms:
            raise InvalidState('股票预热必须在首个组合决策前完成')
        source_start=start_ms-STEP;first=source_start-LIQUIDITY_BARS*STEP
        observer=StockSourceObserver(symbols=self.stock_symbols,start_ms=source_start,warmup_start_ms=first)
        for end in range(first,source_start+STEP,STEP):
            observer.advance(end,self.stock_frame(end,known_ms=now_ms),universe_known_ms=self.universe.received_ms,
                universe_complete=True,known_ms=now_ms)
        # Warmup never recreates old mothers. Original source momentum starts
        # with the first admitted observation after feature-only warmup.
        if observer.births:raise InvalidState('股票预热意外产生了历史入场来源')
        return observer

    def stock_quality(self,*,now_ms):
        self._current(now_ms)
        return {s:self.contracts[s].tradable(now_ms=now_ms) for s in self.stock_symbols}

    def stock_minutes(self,end_ms,*,known_ms):
        self._current(known_ms)
        rows,_=self.store.frame('perpetual',self.stock_symbols,'1m',end_ms=end_ms,now_ms=known_ms);result={}
        for symbol in self.stock_symbols:
            c=self.contracts[symbol]
            if end_ms<=c.listed_ms or end_ms-60000>=c.delivery_ms:continue
            if symbol not in rows:raise InvalidState('股票已收盘分钟未完整收到：'+symbol)
            result[symbol]=rows[symbol].bar
        return result

    def alt_hourly_needs(self,end_ms):
        if type(end_ms) is not int or end_ms%HOUR:raise InvalidState('山寨小时预热时钟错误')
        needs=[]
        for symbol in self.crypto_names:
            c=self.contracts[symbol];first=max(end_ms-193*HOUR,c.listed_ms//HOUR*HOUR)
            last=min(end_ms,c.delivery_ms//HOUR*HOUR)
            if first<last:needs.append(CandleNeed('perpetual',symbol,'1h',first,last))
        return tuple(needs)

    def hourly(self,end_ms,*,known_ms):
        result=self._observed_hourly(end_ms,known_ms=known_ms)
        result.validate(end_ms+STEP,False)
        return result

    def outage_hourly(self,end_ms,*,known_ms):
        """Actual-time feature repair only; not an old timely decision packet.

        The recovery observer independently validates this late window and
        records missed candidates. Ordinary HourlyWindow.validate/advance
        still rejects observations received after its original decision time.
        """
        if type(known_ms) is not int or type(end_ms) is not int or known_ms<end_ms+STEP+60000:
            raise InvalidState('中断小时输入必须记录错过窗口后的实际接收时间')
        return self._observed_hourly(end_ms,known_ms=known_ms)

    def _observed_hourly(self,end_ms,*,known_ms):
        self._current(known_ms)
        if type(end_ms) is not int or end_ms%HOUR or not end_ms<=known_ms:raise InvalidState('山寨小时尚未收盘')
        # Status changes are not backdated across old prices. A now-ineligible
        # asset stays in the retained roster but is excluded from this complete
        # current decision universe. BTC/ETH are mandatory context inputs.
        names=tuple(s for s in self.crypto_names if self.contracts[s].tradable(now_ms=known_ms))
        if not {'BTCUSDT','ETHUSDT'}<=set(names):raise InvalidState('BTC/ETH 上下文合约不可交易')
        shape=(193,len(names));close,high,low,quote=(np.full(shape,np.nan) for _ in range(4))
        received=np.zeros(shape,dtype=np.int64);absence=np.ones(shape,dtype=np.int8);last_known=self.universe.received_ms
        for j,symbol in enumerate(names):
            c=self.contracts[symbol];begin=end_ms-193*HOUR
            rows,missing=self.store.window('perpetual',symbol,'1h',start_ms=begin,end_ms=end_ms,now_ms=known_ms,require_complete=False)
            for t in missing:
                if t+HOUR>c.listed_ms:raise InvalidState('山寨小时窗口存在未解释缺口：'+symbol)
            for o in rows:
                b=o.bar;i=(b.start_ms-begin)//HOUR
                if b.end_ms<=c.listed_ms:raise InvalidState('山寨上市前出现行情')
                close[i,j],high[i,j],low[i,j],quote[i,j]=map(float,(b.close,b.high,b.low,b.quote_volume))
                received[i,j]=b.received_ms;absence[i,j]=0;last_known=max(last_known,b.received_ms)
        return HourlyWindow(end_ms,last_known,names,close,high,low,quote,received,absence,True,names)

    def alt_feature_needs(self,end_ms,*,known_ms):
        w=self.hourly(end_ms,known_ms=known_ms);pairs=self.mappings.current(now_ms=known_ms)
        _,rank,_,_=ranked(w,192,names=w.source_names);needs=[]
        for j in rank:
            symbol=w.names[j];needs.append(CandleNeed('perpetual',symbol,'5m',end_ms-13*STEP,end_ms))
            pair=pairs.get(symbol)
            if pair is not None and pair.known_ms<=end_ms-HOUR:
                needs.append(CandleNeed('spot',pair.spot_symbol,'5m',end_ms-HOUR,end_ms))
        return tuple(needs)

    def alt_packet(self,end_ms,*,known_ms):
        w=self.hourly(end_ms,known_ms=known_ms);pairs=self.mappings.current(now_ms=known_ms)
        _,rank,_,_=ranked(w,192,names=w.source_names);fives={};flows={}
        for j in rank:
            symbol=w.names[j]
            rows,_=self.store.window('perpetual',symbol,'5m',start_ms=end_ms-13*STEP,end_ms=end_ms,now_ms=known_ms)
            fives[symbol]=tuple(o.bar for o in rows);pair=pairs.get(symbol)
            if pair is not None and pair.known_ms<=end_ms-HOUR:
                spot,_=self.store.window('spot',pair.spot_symbol,'5m',start_ms=end_ms-HOUR,end_ms=end_ms,now_ms=known_ms)
                flows[symbol]=FlowWindow(symbol,pair.spot_symbol,pair.known_ms,
                    tuple(FlowBar(o.bar,o.buy_quote) for o in spot),tuple(FlowBar(o.bar,o.buy_quote) for o in rows[-12:]))
        return dict(alt_hourly=w,alt_five_minute=fives,alt_flow=flows)
