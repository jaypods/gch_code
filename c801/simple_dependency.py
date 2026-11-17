"""Dependency-scoped live scheduling. No network or exchange writes.

Original book predicates, ranking and cash reservations are reused. Independent
observations and position maintenance may advance without a complete universe;
new cross-sectional admissions still require their entire original universe.
Only CURRENT decisions reach the durable outbox. Late source observations use
the existing no-historical-entry continuation functions.
"""
from copy import deepcopy
from dataclasses import replace
import heapq
import time

from .account import InvalidState
from .combined_controller import CombinedController
from .main_controller import MainController
from .main_shadow import MainShadowModel
from .main_warmup import main_needs, prepare_main
from .market import CausalMarket
from .market_warmup import CandleNeed
from .outage_recovery import recover_stock_frame, recover_alt_frame
from .simple_orders import normalize_action
from .simple_resume import advance_missed_shadow
from .state_codec import encode, decode, validate_time
from .strategies.main import MainQuote, MainReceipt, SYMS
from .strategies.alt import AltBook, AltReceipt, order_key
from .strategies.alt_sources import AltSourceObserver, HOUR, STEP
from .strategies.stock import StockBook, StockReceipt
from .strategies.stock_sources import StockSourceObserver, StockObservation, LIQUIDITY_BARS

MINUTE = 60000
FORMAT = 'dependency-live-1'


def split_stock(observer):
    """Keep the original features, source identities and rank ordinals."""
    result = {}
    for symbol in observer.symbols:
        one = object.__new__(StockSourceObserver)
        one.__dict__.update({k: deepcopy(v) for k, v in observer.__dict__.items()
                             if k not in ('roster_admissions', 'features', 'births', 'lifecycle',
                                          'active', 'previous', 'history', 'validity_history')})
        one.symbols = (symbol,)
        one.features = {symbol: deepcopy(observer.features[symbol])}
        one.previous = {symbol: observer.previous[symbol]}
        one.active = {symbol: observer.active[symbol]} if symbol in observer.active else {}
        one.births = [s for s in observer.births if s.symbol == symbol]
        ids = {s.source_id for s in one.births}
        one.lifecycle = [deepcopy(r) for r in observer.lifecycle if r.get('source_id') in ids]
        one.history = {t: {symbol: row[symbol]} for t, row in observer.history.items()}
        one.validity_history = {t: {symbol: row[symbol]} for t, row in observer.validity_history.items()}
        result[symbol] = one
    return result


def migrate(controller):
    if not isinstance(controller, CombinedController) or controller.halt_reason or controller.stock is None:
        raise InvalidState('独立调度需要完整的原策略状态')
    c = deepcopy(controller)
    stock = object.__new__(StockBook)
    stock.__dict__.update({k: v for k, v in c.stock.book.__dict__.items() if k != 'outage_transitions'})
    return dict(format=FORMAT, start_ms=c.start_ms, clock_ms=c.last_clock,
                main=c.main, model=c.shadow_model, perpetual=c.perpetual, spot=c.spot,
                alt=c.alt, alt_observer=c.alt_observer, stock=stock,
                stocks=split_stock(c.stock.observer), roster=tuple(c.stock.observer.symbols),
                meta=dict(main_done=c.main.funded.last_clock, main_prepared=True,
                          alt_done=c.alt.last_clock, alt_source_frame=-1, alt_sources=(),
                          stock_selected=stock.last_clock // STEP * STEP,
                          stock_seq=len(c.stock.observer.births), stock_cursor=0,
                          managed={oid:stock.last_clock for oid in stock.orders},
                          main_protected={symbol:c.main.funded.last_clock for symbol in SYMS},
                          alt_managed={o['symbol']:c.alt.last_clock for o in c.alt.orders.values()}, gaps=[]),
                prior_gaps=deepcopy(getattr(c, 'outage_transitions', [])))


def new_state(*, start_ms, initial, symbols):
    main = MainController(start_ms=start_ms, initial=float(initial['M784']))
    stock = StockBook(start_ms=start_ms, initial=float(initial['S800']))
    observers = {s: StockSourceObserver(symbols=(s,), start_ms=start_ms-STEP,
                 warmup_start_ms=start_ms-STEP-LIQUIDITY_BARS*STEP) for s in symbols}
    return dict(format=FORMAT, start_ms=start_ms, clock_ms=start_ms-MINUTE,
                main=main, model=MainShadowModel(main.shadow), perpetual=CausalMarket(), spot=CausalMarket(),
                alt=AltBook(start_ms=start_ms, initial=float(initial['A787'])),
                alt_observer=AltSourceObserver(start_ms=((start_ms-STEP+HOUR-1)//HOUR)*HOUR+STEP),
                stock=stock, stocks=observers, roster=tuple(symbols), prior_gaps=[],
                meta=dict(main_done=start_ms-STEP, main_prepared=False,
                          alt_done=start_ms-STEP, alt_source_frame=-1, alt_sources=(),
                          stock_selected=start_ms-STEP, stock_seq=0, stock_cursor=0,
                          managed={}, main_protected={}, alt_managed={}, gaps=[]))


class DependencyEngine:
    def __init__(self, store):
        self.store = store
        self.state = None
        self.revision = 0
        saved = store.state()
        if saved:
            value = decode(saved['payload'], profile='live')
            self.state = migrate(value) if isinstance(value, CombinedController) else value
            if not isinstance(self.state, dict) or self.state.get('format') != FORMAT:
                raise InvalidState('独立调度状态格式不符')
            if self.state['model'].book is not self.state['main'].shadow:
                raise InvalidState('机会模型账户绑定错误')
            self.revision = saved['revision']
        self.waiting = {}
        self.requests = []
        self.funding_window = None
        self.dirty = False

    @property
    def roster(self):
        return () if self.state is None else self.state['roster']

    def initialize(self, *, now_ms, initial, symbols):
        if self.state is not None:
            raise InvalidState('不能重新分配已运行账户本金')
        self.state = new_state(start_ms=(now_ms//STEP+1)*STEP, initial=initial, symbols=symbols)
        self.dirty = True

    def need(self, venue, symbol, interval, begin, end):
        if begin < end:
            period={'1m':MINUTE,'5m':STEP,'1h':HOUR,'1d':86400000}[interval]
            self.requests.append(CandleNeed(venue, symbol, interval, begin, min(end,begin+10000*period)))

    def bars(self, venue, symbol, interval, begin, end, *, cutoff=None):
        self.need(venue, symbol, interval, begin, end)
        rows, _ = self.market.window(venue, symbol, interval, start_ms=begin, end_ms=end,
                                    now_ms=self.now if cutoff is None else cutoff)
        return tuple(r.bar for r in rows)

    def bar(self, symbol, interval, end, *, cutoff=None, venue='perpetual'):
        period = {'1m': MINUTE, '5m': STEP}[interval]
        return self.bars(venue, symbol, interval, end-period, end, cutoff=cutoff)[0]

    def attempt(self, key, fields, operation):
        """A failed dependency cannot discard another successful operation."""
        saved = deepcopy({f: self.state[f] for f in (*fields, 'meta')})
        first = len(self.actions)
        try:
            changed = operation()
            self.waiting.pop(key, None)
            self.dirty |= changed is not False
            return True
        except Exception as exc:
            self.state.update(saved)
            # main and model are ALWAYS staged together to preserve identity.
            del self.actions[first:]
            message = str(exc) if isinstance(exc, InvalidState) else type(exc).__name__
            self.waiting[key] = message[:150]
            return False

    def emit(self, sleeve, rows):
        for row in rows:
            action = dict(row, known_ms=self.now, cycle_ms=self.frame)
            first = None
            if sleeve == 'S800' and action.get('kind') == 'EXIT_REQUEST' and action.get('reason') == 'deadline':
                first = self.state['stock'].orders[action['order_id']]['first_ms']
            self.actions.append(normalize_action(sleeve, action, now_ms=self.now, first_entry_ms=first))

    def gap(self, name, old, new, period):
        if new > old+period:
            self.state['meta']['gaps'].append(dict(component=name, previous_ms=old,
                current_ms=new, received_ms=self.now, missed=(new-old)//period-1))

    def receipts(self, sleeve, book, cutoff):
        cls = {'M784': MainReceipt, 'A787': AltReceipt, 'S800': StockReceipt}[sleeve]
        rows = [r for r in self.pending if r['sleeve'] == sleeve and r['receipt']['available_ms'] <= cutoff]
        for item in rows:
            book.receive(cls(**item['receipt']))
        delivered=False
        while book.queue and book.queue[0][0] <= cutoff:
            book._deliver(heapq.heappop(book.queue)[2], cutoff)
            delivered=True
        if sleeve == 'M784': book._settle_groups(cutoff)
        self.consumed.extend(r['id'] for r in rows)
        return bool(rows) or delivered

    def receive_step(self, sleeve, field):
        def run():
            book = self.state['main'].funded if sleeve == 'M784' else self.state[field]
            cutoff = self.now if sleeve == 'S800' else self.frame//STEP*STEP
            first = len(book.actions) if sleeve == 'S800' else 0
            changed = self.receipts(sleeve, book, cutoff)
            if sleeve == 'S800': self.emit(sleeve, book.actions[first:])
            return changed
        before = len(self.consumed)
        ok = self.attempt('回报:'+sleeve, ('main','model') if sleeve=='M784' else (field,), run)
        if not ok: del self.consumed[before:]
        return ok

    def main_step(self):
        st = self.state
        # Required pair/model data cannot be replaced by a single-symbol signal.
        def model():
            m = self.state['model']; end = m.last_minute+2*MINUTE
            if end > self.frame: return False
            self.funding_window = (end-MINUTE, end)
            bars = {s: self.bar(s, '1m', end) for s in SYMS}
            funding = self.data.funding(start_ms=end-MINUTE, end_ms=end, now_ms=self.now)
            if end < self.frame:
                advance_missed_shadow(m, end, bars, known_ms=self.now, funding=funding)
            else:
                # A missed five-minute decision creates no shadow entry orders.
                clock = (end-MINUTE)//STEP*STEP
                if m.book.last_clock < clock:
                    self.gap('M784:model', m.book.last_clock, clock, STEP)
                    while m.book.queue and m.book.queue[0][0]<=clock:
                        m.book._deliver(heapq.heappop(m.book.queue)[2],clock)
                    m.book._settle_groups(clock)
                    m.book.last_clock = clock
                m.observe_closed_minute(end-MINUTE, bars, received_ms=self.now, funding=funding)
            del m.equity[:-2]
        self.attempt('主流币:机会模型', ('main','model'), model)
        next_five = (self.frame//STEP+1)*STEP
        def warm():
            if self.state['meta'].get('prepared_for') == next_five: return False
            self.requests.extend(main_needs(next_five))
            main = prepare_main(self.market, start_ms=next_five, now_ms=self.now)
            self.state['perpetual'] = main.perpetual
            self.state['spot'] = main.spot
            for close in main.weekly.completed.values(): self.state['main'].weekly.add(close)
            self.state['meta'].update(main_prepared=True, prepared_for=next_five)
        # Normal live windows are prefetched before the next decision boundary.
        self.attempt('主流币:指标', ('main','model','perpetual','spot'), warm)
        if self.frame % STEP or self.frame < st['start_ms']: return
        for symbol in SYMS:
            def protect(symbol=symbol):
                meta=self.state['meta']
                if meta['main_protected'].get(symbol,-1)>=self.frame: return False
                b=self.bar(symbol,'5m',self.frame-STEP,cutoff=self.frame)
                q=MainQuote(symbol,b.start_ms,b.end_ms,b.received_ms,float(b.close),float(b.quote_volume))
                q.validate(self.frame)
                self.emit('M784',self.state['main'].funded._protection(self.frame,{symbol:q},symbols={symbol}))
                meta['main_protected'][symbol]=self.frame
            self.attempt('主流币:'+symbol,('main','model'),protect)
        def decide():
            s=self.state;main=s['main'];meta=s['meta']
            if meta['main_done']>=self.frame:return False
            if not meta['main_prepared']:raise InvalidState('主流币指标尚未准备完成')
            if self.frame>s['start_ms'] and s['model'].last_minute!=self.frame-MINUTE:
                raise InvalidState('双币机会模型尚未到当前分钟')
            # New inputs retain original first-arrival times and five-minute lag.
            for venue,market in (('perpetual',s['perpetual']),('spot',s['spot'])):
                for symbol in SYMS:
                    for b in self.bars(venue,symbol,'5m',self.frame-(292 if venue=='perpetual' else 3)*STEP,
                                       self.frame-STEP,cutoff=self.frame):market.add(b)
            for book in (main.funded,main.shadow):
                self.gap('M784:book',book.last_clock,self.frame,STEP)
                book.last_clock=self.frame-STEP
            result=main.advance(self.frame,s['perpetual'],s['spot'])
            s['model'].dispatch(result['shadow_actions'])
            self.emit('M784',result['funded_actions']);meta['main_done']=self.frame
        self.attempt('主流币:双币确认',('main','model','perpetual','spot'),decide)

    def stock_observation(self, symbol, end):
        c=self.sources.contracts[symbol]
        if end<=c.listed_ms:return StockObservation(None,'not_listed',c.known_ms)
        if end-STEP>=c.delivery_ms:return StockObservation(None,'quarantined',c.known_ms)
        return StockObservation(self.bar(symbol,'5m',end),'present',c.known_ms)

    def stock_features(self):
        st=self.state;end=self.frame//STEP*STEP
        # New catalog members warm independently; their absence blocks only the
        # global S800 ranking, never existing-position management or M/A.
        for symbol in self.sources.stock_symbols:
            if symbol not in st['stocks']:
                st['stocks'][symbol]=StockSourceObserver(symbols=(symbol,),start_ms=end+STEP,
                    warmup_start_ms=max(STEP,end-LIQUIDITY_BARS*STEP))
                st['roster']+= (symbol,);self.dirty=True
        roster=st['roster'];cursor=st['meta']['stock_cursor']%len(roster)
        budget=4096;deadline=time.monotonic()+.18
        for j in range(len(roster)):
            symbol=roster[(cursor+j)%len(roster)];observer=st['stocks'][symbol]
            c=self.sources.contracts[symbol]
            self.need('perpetual',symbol,'5m',max(observer.last_clock,c.listed_ms//STEP*STEP),
                      min(end,c.delivery_ms//STEP*STEP))
            batch_end=min(end,observer.last_clock+(512 if observer.last_clock<observer.start_ms else 16)*STEP)
            begin=max(observer.last_clock,c.listed_ms//STEP*STEP)
            last=min(batch_end,c.delivery_ms//STEP*STEP)
            rows={}
            if begin<last:
                try:
                    observed,_=self.market.window('perpetual',symbol,'5m',start_ms=begin,end_ms=last,
                                                   now_ms=self.now,require_complete=False)
                except Exception as exc:
                    self.waiting['股票:'+symbol]=str(exc)[:150] if isinstance(exc,InvalidState) else type(exc).__name__
                    continue
                rows={r.bar.end_ms:r.bar for r in observed}
            while observer.last_clock<batch_end and budget>0:
                frame=observer.last_clock+STEP
                try:
                    if frame<=c.listed_ms:o=StockObservation(None,'not_listed',c.known_ms)
                    elif frame-STEP>=c.delivery_ms:o=StockObservation(None,'quarantined',c.known_ms)
                    elif frame not in rows:raise InvalidState('股票行情缺失：'+symbol)
                    else:o=StockObservation(rows[frame],'present',c.known_ms)
                    if frame<observer.start_ms:
                        observer.advance(frame,{symbol:o},universe_known_ms=self.sources.universe.received_ms,
                                         universe_complete=True,known_ms=self.now)
                    elif frame<self.frame:
                        recover_stock_frame(observer,frame,{symbol:o},known_ms=self.now,
                            universe_known_ms=self.sources.universe.received_ms,universe_complete=True)
                    else:
                        count=len(observer.births)
                        observer.advance(frame,{symbol:o},universe_known_ms=self.sources.universe.received_ms,
                                         universe_complete=True,known_ms=self.now)
                        if len(observer.births)>count:
                            st['meta']['stock_seq']+=1
                            original=observer.births[-1]
                            new=replace(original,source_id=f"S800:{st['meta']['stock_seq']:06d}",ordinal=roster.index(symbol))
                            observer.births[-1]=new
                            if symbol in observer.active:observer.active[symbol]=new
                            observer.history[frame][symbol]=new.source_id
                    budget-=1;self.dirty=True;self.waiting.pop('股票:'+symbol,None)
                except InvalidState as exc:
                    self.waiting['股票:'+symbol]=str(exc)[:150];break
            if budget==0 or time.monotonic()>=deadline:
                st['meta']['stock_cursor']=(cursor+j+1)%len(roster);break
        else:st['meta']['stock_cursor']=0

    def stock_step(self):
        s=self.state;book=s['stock']
        if self.frame<s['start_ms']:return
        for oid in tuple(book.orders):
            def manage(oid=oid):
                s=self.state;b=s['stock'];p=b.orders[oid];meta=s['meta']
                if p['settled'] or meta['managed'].get(oid,-1)>=self.frame:return False
                first=len(b.actions);view=None;bars={}
                if self.now<p['expiry_ms'] and self.frame>=p['active_ms'] and not p['entry_done']:
                    observer=s['stocks'][p['symbol']]
                    view=observer.execution_view(self.frame,current_quality={p['symbol']:self.sources.contracts[p['symbol']].tradable(now_ms=self.now)},
                        quality_known_ms=self.sources.universe.received_ms,decision_ms=self.now)
                if p['remaining']>0 and p['protection'] is None:
                    bars[p['symbol']]=self.bar(p['symbol'],'1m',self.frame)
                b._manage_order(p,self.frame,self.now,bars,view)
                self.emit('S800',b.actions[first:]);meta['managed'][oid]=self.frame
            self.attempt('股票持仓:'+str(oid),('stock',),manage)
        def select():
            s=self.state;b=s['stock'];meta=s['meta']
            if self.frame%STEP or self.frame<=s['start_ms'] or meta['stock_selected']>=self.frame:return False
            if any(not p['settled'] for p in b.orders.values()):return False
            if any(o.last_clock!=self.frame for o in s['stocks'].values()):
                raise InvalidState('股票新开仓等待完整候选评分，已有持仓分别处理')
            sources=tuple(o.active[symbol] for symbol,o in s['stocks'].items() if symbol in o.active)
            bars={source.symbol:self.stock_observation(source.symbol,self.frame).bar for source in sources}
            for source in sources:
                source.validate(self.now);b.sources.setdefault(source.source_id,source)
            first=len(b.actions);b._select(self.frame,self.now,sources,bars)
            self.emit('S800',b.actions[first:]);meta['stock_selected']=self.frame
        self.attempt('股票:候选比较',('stock',),select)
        # This records receipt/maintenance time, not a claim that every source
        # advanced. Each source and each order has its own durable cursor.
        if self.state['stock'].last_clock<self.frame:
            self.gap('S800:book',self.state['stock'].last_clock,self.frame,MINUTE)
            self.state['stock'].last_clock=self.frame
            self.state['stock'].last_decision_ms=self.now;self.dirty=True

    def alt_source_step(self):
        observer=self.state['alt_observer'];frame=observer.last_clock+HOUR
        if frame>self.frame:return
        self.requests.extend(self.sources.alt_hourly_needs(frame-STEP))
        def observe():
            o=self.state['alt_observer']
            if frame<self.frame:
                recover_alt_frame(o,frame,self.sources.outage_hourly(frame-STEP,known_ms=self.now),known_ms=self.now)
                return
            self.requests.extend(self.sources.alt_feature_needs(frame-STEP,known_ms=self.now))
            packet=self.sources.alt_packet(frame-STEP,known_ms=self.now)
            sources=o.advance(frame,packet['alt_hourly'],
                five_minute=lambda symbol,end,clock:packet['alt_five_minute'].get(symbol),
                flow=lambda symbol,end,clock:packet['alt_flow'].get(symbol))
            self.state['meta'].update(alt_source_frame=frame,alt_sources=tuple(sources))
        self.attempt('山寨:排名',('alt_observer',),observe)

    def alt_step(self):
        if self.frame%STEP or self.frame<self.state['start_ms']:return
        # Per-symbol calendars and protective decisions do not depend on the
        # hourly universe, nor on another held symbol's missing candle.
        book=self.state['alt']
        active=tuple(dict.fromkeys(o['symbol'] for cid,o in book.orders.items()
            if not o['entry_done'] or cid in book.cycles and not book.cycles[cid]['closed']))
        for symbol in active:
            def manage(symbol=symbol):
                b=self.state['alt'];meta=self.state['meta']
                if meta['alt_managed'].get(symbol,-1)>=self.frame:return False
                events=self.data.calendars(self.sources.universe,(symbol,),now_ms=self.now)
                for event in events:
                    event.validate()
                    if event.known_ms>self.frame:raise InvalidState('山寨到期信息尚未到原策略使用时点')
                    b.calendar[symbol]=event
                market=CausalMarket()
                market.add(self.bar(symbol,'5m',self.frame-STEP,cutoff=self.frame))
                for p in b.cycles.values():
                    if p['symbol']!=symbol or p.get('is_add') or p['closed'] or p['conditional_exit_ms'] is not None:continue
                    if self.frame in ((p['entry_ms']+age*HOUR+STEP-1)//STEP*STEP for age in (48,72,96,120,144)):
                        market.add(self.bar(symbol,'5m',self.frame-STEP-24*HOUR,cutoff=self.frame))
                first=len(b.requests)
                for cid,o in b.orders.items():
                    if o['symbol']!=symbol:continue
                    event=b.calendar.get(symbol)
                    if event and self.frame>=event.settlement_ms and (not o['entry_done'] or cid in b.cycles and not b.cycles[cid]['closed']):
                        raise InvalidState('到期品种仍有未结算订单或仓位：'+symbol)
                    if event and self.frame>=event.no_new_ms and not o['entry_done'] and cid not in b.cancel_requested:
                        b._request('CANCEL_ENTRY',cid,self.frame,effective_ms=self.frame,reason='announced_no_new_position');b.cancel_requested.add(cid)
                    if event and cid in b.cycles and b.cycles[cid]['remaining']>0 and cid not in b.calendar_exit_requested:
                        b._request('EXIT_REQUEST',cid,self.frame,effective_ms=max(self.frame,event.exit_ms),reason='announced_pre_settlement_exit',priority=30)
                        b.calendar_exit_requested.add(cid)
                b._parent_protection(self.frame,market,symbols={symbol})
                b._trend(self.frame,market,symbols={symbol})
                b._observe_exits(self.frame,market,symbols={symbol})
                self.emit('A787',b.requests[first:]);meta['alt_managed'][symbol]=self.frame
            self.attempt('山寨持仓:'+symbol,('alt',),manage)
        def admit():
            s=self.state;b=s['alt'];meta=s['meta']
            if meta['alt_done']>=self.frame:return False
            if any(meta['alt_managed'].get(symbol,-1)!=self.frame for symbol in active):
                raise InvalidState('新增山寨仓位等待现有仓位的完整估值与持仓处理')
            hourly_due=self.frame%HOUR==STEP and meta['alt_source_frame']!=self.frame
            if hourly_due:raise InvalidState('新开仓等待本小时完整排名')
            parents=meta['alt_sources'] if meta['alt_source_frame']==self.frame else ()
            for source in parents:
                for event in self.data.calendars(self.sources.universe,(source['symbol'],),now_ms=self.now):
                    if event.known_ms>self.frame:raise InvalidState('新开仓到期信息晚于原策略时点')
                    b.calendar[source['symbol']]=event
            market=CausalMarket()
            for symbol in dict.fromkeys((*active,*(p['symbol'] for p in parents))):
                market.add(self.bar(symbol,'5m',self.frame-STEP,cutoff=self.frame))
            self.gap('A787:book',b.last_clock,self.frame,STEP);b.last_clock=self.frame-STEP
            b.cash_peak=max(b.cash_peak,b.known_cash)
            first=len(b.requests)
            children=b._prepare_children(self.frame,market)
            members=tuple(p['id'] for p in sorted(parents,key=order_key))
            for source in sorted(parents,key=order_key):
                b._validate_source(source,self.frame)
                if source['id'] in b.consumed_sources:raise InvalidState('山寨来源重复')
                b._admit(source,self.frame,market,members);b.consumed_sources.add(source['id'])
            for source in sorted(children,key=order_key):b._admit(source,self.frame,market,())
            self.emit('A787',b.requests[first:]);b.last_clock=self.frame;meta['alt_done']=self.frame
        self.attempt('山寨:资金与入场',('alt',),admit)

    def step(self, sources, data, *, now_ms, clock=None):
        if self.state is None:raise InvalidState('账户本金尚未初始化')
        self.now=now_ms;self.frame=now_ms//MINUTE*MINUTE
        self.sources=sources;self.market=sources.store;self.data=data
        self.requests=[];self.funding_window=None;self.actions=[];self.consumed=[]
        self.pending=self.store.pending_receipts(now_ms=now_ms)
        before=encode(self.state,profile='live');self.dirty=False
        try:
            sources._current(now_ms)
            ok_main=self.receive_step('M784','main')
            ok_alt=self.receive_step('A787','alt')
            ok_stock=self.receive_step('S800','stock')
            if ok_main:self.main_step()
            self.alt_source_step()
            if ok_alt:self.alt_step()
            self.stock_features()
            if ok_stock:self.stock_step()
            # Recheck real elapsed time before publishing. A slow feature
            # operation must never turn a previous-minute entry into a new one.
            finished=now_ms if clock is None else clock()
            if finished//MINUTE*MINUTE!=self.frame:
                raise InvalidState('计算跨过分钟边界，本轮不补发过期订单')
            if (self.dirty or self.actions or self.consumed) and self.frame>=self.state['start_ms']-MINUTE:
                self.state['clock_ms']=self.frame
                validate_time(self.state,clock_ms=self.frame,received_ms=now_ms)
                payload=encode(self.state,profile='live')
                self.revision=self.store.save_state(payload,clock_ms=self.frame,decision_ms=now_ms,
                    expected_revision=self.revision,actions=self.actions,consumed_receipts=self.consumed)
            return dict(actions=self.actions,waiting=dict(self.waiting),revision=self.revision,
                        needs=tuple(dict.fromkeys(self.requests)),funding_window=self.funding_window)
        except BaseException:
            self.state=decode(before,profile='live')
            raise
