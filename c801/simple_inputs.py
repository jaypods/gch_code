"""Current original-strategy inputs, without an audited replay service."""
from dataclasses import asdict
import json

from .account import InvalidState
from .combined_inputs import CombinedInputs
from .execution_market import timed
from .funding_inputs import parse_funding_history, SettledFunding
from .simple_orders import stamp
from .simple_store import _json
from .strategies.alt import AltCalendar

SYMBOLS=('BTCUSDT','ETHUSDT')


class SimpleInputData:
    def __init__(self, store, transport):
        if (getattr(transport,'_credentials',None) is not None or getattr(transport,'credentials',None) is not None
                or getattr(transport,'mutations_enabled',False)):
            raise InvalidState('策略公开行情输入不能使用交易密钥')
        self.store,self.db,self.transport=store,store.db,transport
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS simple_public_funding(
                symbol TEXT NOT NULL,event_ms INTEGER NOT NULL,kind TEXT NOT NULL,
                received_ms INTEGER NOT NULL,body TEXT NOT NULL,
                PRIMARY KEY(symbol,event_ms,kind));
            CREATE TABLE IF NOT EXISTS simple_funding_windows(
                symbol TEXT NOT NULL,start_ms INTEGER NOT NULL,end_ms INTEGER NOT NULL,
                received_ms INTEGER NOT NULL,PRIMARY KEY(symbol,start_ms,end_ms));
            CREATE TABLE IF NOT EXISTS simple_expected_funding(
                symbol TEXT NOT NULL,event_ms INTEGER NOT NULL,known_ms INTEGER NOT NULL,
                PRIMARY KEY(symbol,event_ms));
        ''')

    def observe_schedule(self, symbol):
        if symbol not in SYMBOLS:
            raise InvalidState('主流币模型资金费品种错误')
        reply=self.transport.request('QUERY_MARK_PRICE',dict(symbol=symbol))
        body,event,received=timed(reply,symbol,self.transport.now_ms())
        next_ms=stamp(body.get('nextFundingTime'))
        if next_ms<event:
            raise InvalidState('资金费下一结算时间早于行情时点')
        self.db.execute('INSERT OR IGNORE INTO simple_expected_funding VALUES(?,?,?)',(symbol,next_ms,received))
        return next_ms

    def funding_ready(self, *, start_ms, end_ms, now_ms):
        stamp(start_ms);stamp(end_ms);stamp(now_ms)
        if not start_ms<end_ms<=now_ms:
            raise InvalidState('资金费输入必须属于已结束区间')
        for symbol in SYMBOLS:
            row=self.db.execute('SELECT 1 FROM simple_funding_windows WHERE symbol=? AND start_ms<=? AND end_ms>=? AND received_ms<=? LIMIT 1',
                                (symbol,start_ms,end_ms,now_ms)).fetchone()
            if row is None:
                return False
            missing=self.db.execute('''SELECT 1 FROM simple_expected_funding x WHERE symbol=?
                AND event_ms>=? AND event_ms<? AND known_ms<=? AND NOT EXISTS
                (SELECT 1 FROM simple_public_funding f WHERE f.symbol=x.symbol AND f.event_ms=x.event_ms AND f.kind='Regular' AND f.received_ms<=?)''',
                (symbol,start_ms,end_ms,now_ms,now_ms)).fetchone()
            if missing:
                return False
        return True

    def fetch_funding_one(self, *, start_ms, end_ms):
        """One bounded REST page; an expected unpaid event remains pending."""
        now=self.transport.now_ms()
        stamp(start_ms);stamp(end_ms)
        if not start_ms<end_ms<=now or end_ms-start_ms>31*86400000:
            raise InvalidState('资金费请求需要有限的已结束区间')
        for symbol in SYMBOLS:
            done=self.db.execute('SELECT start_ms,end_ms FROM simple_funding_windows WHERE symbol=? AND start_ms<=? AND end_ms>=? AND received_ms<=? LIMIT 1',
                                 (symbol,start_ms,end_ms,now)).fetchone()
            if done:
                missing=self.db.execute('''SELECT 1 FROM simple_expected_funding x WHERE symbol=? AND event_ms>=? AND event_ms<?
                    AND NOT EXISTS (SELECT 1 FROM simple_public_funding f WHERE f.symbol=x.symbol AND f.event_ms=x.event_ms AND f.kind='Regular')''',
                    (symbol,start_ms,end_ms)).fetchone()
                if not missing:
                    continue
                self.db.execute('DELETE FROM simple_funding_windows WHERE symbol=? AND start_ms=? AND end_ms=?',(symbol,*done))
            reply=self.transport.request('QUERY_FUNDING_HISTORY',dict(symbol=symbol,startTime=start_ms,endTime=end_ms-1,limit=1000))
            events=parse_funding_history(reply,symbol=symbol,start_ms=start_ms,end_ms=end_ms-1)
            with self.store.transaction():
                for event in events:
                    old=self.db.execute('SELECT body FROM simple_public_funding WHERE symbol=? AND event_ms=? AND kind=?',event.key).fetchone()
                    if old:
                        prior=SettledFunding(**json.loads(old[0]))
                        if (prior.rate,prior.mark)!=(event.rate,event.mark):
                            raise InvalidState('交易所修订了已经接收的模型资金费，不能覆盖旧决策输入')
                    else:
                        self.db.execute('INSERT INTO simple_public_funding VALUES(?,?,?,?,?)',
                                        (event.symbol,event.time_ms,event.rate_type,event.received_ms,_json(asdict(event))))
                due={r[0] for r in self.db.execute('SELECT event_ms FROM simple_expected_funding WHERE symbol=? AND event_ms>=? AND event_ms<? AND known_ms<=?',
                                                 (symbol,start_ms,end_ms,reply.received_ms))}
                present={e.time_ms for e in events if e.rate_type=='Regular'}
                complete=len(events)<1000 and due<=present
                if complete:
                    self.db.execute('INSERT INTO simple_funding_windows VALUES(?,?,?,?)',(symbol,start_ms,end_ms,reply.received_ms))
            return dict(symbol=symbol,received=len(events),complete=complete)
        return None

    def funding(self, *, start_ms, end_ms, now_ms):
        if not self.funding_ready(start_ms=start_ms,end_ms=end_ms,now_ms=now_ms):
            raise InvalidState('这一分钟的模型资金费尚未实际收到')
        return tuple(SettledFunding(**json.loads(body)).shadow_event() for (body,) in self.db.execute(
            'SELECT body FROM simple_public_funding WHERE event_ms>=? AND event_ms<? AND received_ms<=? ORDER BY event_ms,symbol,kind',
            (start_ms,end_ms,now_ms)))

    def calendars(self, universe, symbols, *, now_ms):
        if not 0<=now_ms-universe.received_ms<=300000:
            raise InvalidState('需要当前合约到期目录')
        saved=self.store.value('alt_calendars',{})
        contracts={c.symbol:c for c in universe.contracts}
        result=[]
        for symbol in symbols:
            c=contracts.get(symbol);old=saved.get(symbol)
            if c is None:
                if old is None:raise InvalidState('原山寨观察品种缺少合约到期信息')
                result.append(AltCalendar(**old['calendar']));continue
            if c.category!='alt':raise InvalidState('山寨到期输入包含其他品种')
            identity=[c.contract_type,c.underlying_type,c.listed_ms,c.delivery_ms]
            if old is not None:
                if old['identity']!=identity:
                    raise InvalidState('合约修改了已生效的到期日期')
                result.append(AltCalendar(**old['calendar']));continue
            known=universe.received_ms
            if c.listed_ms<=known<c.delivery_ms<=known+30*86400000:
                cutoff=max(known,c.delivery_ms-30*60000)
                event=AltCalendar(symbol,known,cutoff,cutoff,c.delivery_ms);event.validate()
                saved[symbol]=dict(identity=identity,calendar=asdict(event));result.append(event)
            elif not c.tradable(now_ms=now_ms):
                raise InvalidState('该合约当前不可交易且尚未收到到期指令')
        self.store.set_value('alt_calendars',saved)
        return tuple(result)

    def packet(self, inputs, controller, *, frame_ms, now_ms):
        if not isinstance(inputs,CombinedInputs):
            raise InvalidState('输入必须属于原三策略组合')
        funding=None if frame_ms==controller.start_ms else self.funding(start_ms=frame_ms-60000,end_ms=frame_ms,now_ms=now_ms)
        names=tuple(s for s in inputs.sources.crypto_names if s not in SYMBOLS)
        calendars=self.calendars(inputs.sources.universe,names,now_ms=now_ms) if frame_ms%300000==0 else ()
        return inputs.packet(controller,frame_ms=frame_ms,received_ms=now_ms,shadow_funding=funding,alt_calendars=calendars)
