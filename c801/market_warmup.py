"""Bounded, resumable public warmup requests, one REST call per step.

The owner calls step only when account reconciliation/protection work permits.
Already persisted windows are reused after restart. Missing intervals are not
reclassified as zero volume, delisting, or non-trading hours. Those require
separate listing/calendar evidence. No parallel requests or hidden retries.
"""
from collections import deque
from dataclasses import dataclass
from .account import InvalidState,PositionKey
from .binance_transport import CANDLE_PERIODS,TransportError

# A REST response immediately after a boundary can still contain the prior
# in-progress candle. Stream x=true observations do not need this REST delay.
REST_CLOSE_DELAY_MS=5000


@dataclass(frozen=True)
class CandleNeed:
    venue:str
    symbol:str
    interval:str
    start_ms:int
    end_ms:int

    def __post_init__(self):
        PositionKey(self.symbol,'BOTH')
        p=CANDLE_PERIODS.get(self.interval)
        if (self.venue not in ('perpetual','spot') or p is None
                or any(type(t) is not int for t in (self.start_ms,self.end_ms))
                or not 0<=self.start_ms<self.end_ms or self.start_ms%p or self.end_ms%p
                or (self.end_ms-self.start_ms)//p>100000):raise InvalidState('预热行情需求无效')


class MarketWarmup:
    def __init__(self,transport,store,needs,*,public_sources=None,page_size=499):
        needs=tuple(needs)
        if not needs or any(not isinstance(n,CandleNeed) for n in needs) or len(set(needs))!=len(needs):
            raise InvalidState('预热需求为空、类型错误或重复')
        self.transport,self.store=transport,store
        if type(page_size) is not int or not 1<=page_size<=1500:raise InvalidState('公开行情分页容量无效')
        self.page_size=page_size
        # A spot source must be explicitly supplied. Futures candles are never
        # silently substituted for the strategy's weekly spot-price reference.
        self.sources={'perpetual':lambda p:transport.request('QUERY_CANDLES',p)}
        if public_sources is not None:
            if not isinstance(public_sources,dict) or set(public_sources)-{'spot'} or any(not callable(v) for v in public_sources.values()):
                raise InvalidState('预热公开数据源配置错误')
            self.sources.update(public_sources)
        self.needs=needs;self.queue=deque(needs);self.completed=set();self.wait_until={};self.failures={}
        self.requests=0;self.halt_reason=None;self.last_received_ms=None

    @property
    def ready(self):return self.halt_reason is None and not self.store.faulted() and len(self.completed)==len(self.needs)

    def progress(self):
        return dict(ready=self.ready,windows=len(self.needs),complete=len(self.completed),
            pending=len(self.needs)-len(self.completed),requests=self.requests,
            last_received_ms=self.last_received_ms,halt_reason=self.halt_reason)

    def _missing(self,need,now):
        return self.store.missing(need.venue,need.symbol,need.interval,start_ms=need.start_ms,
            end_ms=need.end_ms,now_ms=now)

    def extend_single_window(self,need):
        """Extend one pending download without resetting its retry history.

        Only the end may grow. Existing bars retain their first arrival and
        the ordinary step still enforces page limits and the REST close delay.
        An unresolved earliest gap, transport failure or cooldown survives
        every extension; moving the horizon cannot buy new attempts.
        """
        if self.halt_reason is not None or self.store.faulted():
            raise InvalidState('已暂停的行情补数不能扩大范围继续重试')
        if len(self.needs)!=1 or not isinstance(need,CandleNeed):
            raise InvalidState('扩展补数只接受同一个行情窗口')
        old=self.needs[0]
        if ((need.venue,need.symbol,need.interval,need.start_ms)!=
                (old.venue,old.symbol,old.interval,old.start_ms) or need.end_ms<old.end_ms):
            raise InvalidState('扩展补数不能改变品种、起点或缩短范围')
        if need==old:return
        self.needs=(need,)
        self.queue=deque((need,))
        self.completed=set()
        self.wait_until={need:until for _,until in self.wait_until.items()}
        self.failures={(need,reason):attempts for (_,reason),attempts in self.failures.items()}

    def step(self):
        if self.halt_reason is not None:raise InvalidState('行情预热已暂停，不能自动重试')
        if self.store.faulted():self.halt_reason='MARKET_CONFLICT';raise InvalidState('行情数据冲突，预热暂停')
        now=self.transport.now_ms()
        if type(now) is not int or now<0:raise InvalidState('预热时钟无效')
        for _ in range(min(32,len(self.queue))):
            need=self.queue.popleft()
            if need.end_ms>now or self.wait_until.get(need,0)>now:
                self.queue.append(need);continue
            missing=self._missing(need,now)
            if not missing:self.completed.add(need);continue
            if need.venue not in self.sources:
                self.queue.append(need)
                return dict(state='SOURCE_REQUIRED',venue=need.venue)
            p=CANDLE_PERIODS[need.interval]
            missing=tuple(t for t in missing if t+p<=now-REST_CLOSE_DELAY_MS)
            if not missing:
                self.queue.append(need);continue
            start=missing[0];end=start+p
            capacity=min(self.page_size,1000 if need.venue=='spot' else 1500)
            # Bridge cached islands within one bounded page. Every returned
            # duplicate still passes the store's content/revision checks; a
            # missing bar remains a gap, and recent unclosed bars stay excluded.
            for t in missing[1:]:
                if t>=start+capacity*p:break
                end=t+p
            params=dict(symbol=need.symbol,interval=need.interval,startTime=start,endTime=end-1,limit=(end-start)//p)
            self.queue.append(need)
            try:
                self.requests+=1
                reply=self.sources[need.venue](params)
                result=self.store.put_rest(need.venue,reply,symbol=need.symbol,interval=need.interval,
                    start_ms=start,end_ms=end,limit=params['limit'])
                self.last_received_ms=reply.received_ms
                if result['missing']:
                    # Bound repetitions of an unresolved earliest gap. Partial
                    # progress cannot reset this count forever by adding only
                    # later bars while the same earliest bar never arrives.
                    key=(need,result['missing'][0]);attempts=self.failures.get(key,0)+1;self.failures[key]=attempts
                    if attempts>=3:
                        self.halt_reason='UNRESOLVED_MARKET_GAP'
                        return dict(state='HALTED',reason=self.halt_reason,venue=need.venue,
                            symbol=need.symbol,interval=need.interval,missing_start=result['missing'][0])
                    self.wait_until[need]=reply.received_ms+5000
                else:self.wait_until.pop(need,None)
                return dict(state='RECEIVED',venue=need.venue,symbol=need.symbol,interval=need.interval,
                    start_ms=start,end_ms=end,appended=result['appended'],missing=len(result['missing']))
            except TransportError as exc:
                self.wait_until[need]=now+max(1000,exc.retry_after_ms)
                if exc.reason in ('REQUEST_COOLDOWN','REQUEST_BUDGET_COOLDOWN','CLOCK_CALIBRATION_EXPIRED'):
                    return dict(state=exc.reason,retry_after_ms=max(1000,exc.retry_after_ms))
                key=(need,'transport');self.failures[key]=self.failures.get(key,0)+1
                if self.failures[key]>=3:self.halt_reason='PUBLIC_SOURCE_FAILED'
                return dict(state='HALTED' if self.halt_reason else 'READ_FAILED',reason=exc.reason,
                    retry_after_ms=max(1000,exc.retry_after_ms))
            except Exception:
                self.halt_reason='INVALID_OR_UNSAVED_MARKET';raise
        return dict(state='READY' if self.ready else 'WAITING')
