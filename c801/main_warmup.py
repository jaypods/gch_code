"""M784 live startup inputs; separate spot/perpetual histories and UTC weeks.

Uses public first-arrival records. It does not claim a historical startup was
known before today's download. Historical replay supplies its explicit arrival
assumption through the archive provider instead.
"""
from copy import deepcopy
from dataclasses import dataclass
from .account import InvalidState
from .combined_controller import CombinedController
from .market import CausalMarket
from .market_warmup import CandleNeed
from .strategies.main import CLOCK,SYMS
from .strategies.main_weekly import ORIGIN,WEEK,WeeklyClose,WeeklyReferences

DAY=86400000


def main_needs(start_ms):
    if type(start_ms) is not int or start_ms% CLOCK or start_ms<ORIGIN+3*CLOCK:
        raise InvalidState('实时主流币预热起点无效')
    # At decision S the current source candle opens S-2*5m; crossing also
    # compares its predecessor. Monday 00:10 can therefore require 30 old
    # weeks; most starts require 29. Do not request a future Monday close.
    prior_week=ORIGIN+((start_ms-3*CLOCK-ORIGIN)//WEEK)*WEEK
    current_week=ORIGIN+((start_ms-2*CLOCK-ORIGIN)//WEEK)*WEEK
    needs=[]
    for venue in ('perpetual','spot'):
        for symbol in SYMS:
            needs.append(CandleNeed(venue,symbol,'1d',prior_week-29*WEEK,current_week))
            count=291 if venue=='perpetual' else 2
            end=start_ms-CLOCK
            needs.append(CandleNeed(venue,symbol,'5m',end-count*CLOCK,end))
    return tuple(needs)


@dataclass
class MainWarmup:
    start_ms:int
    prepared_ms:int
    perpetual:CausalMarket
    spot:CausalMarket
    weekly:WeeklyReferences
    cache_anchor:dict

    def install(self,controller):
        if (not isinstance(controller,CombinedController) or controller.start_ms!=self.start_ms
                or controller.perpetual is not self.perpetual or controller.spot is not self.spot
                or controller.cycles or controller.last_clock!=self.start_ms-60000
                or controller.main.weekly.completed):raise InvalidState('主流币预热只能绑定到同一行情的未启动组合')
        controller.main.weekly=deepcopy(self.weekly)


def prepare_main(store,*,start_ms,now_ms):
    needs=main_needs(start_ms)
    if type(now_ms) is not int or not 0<=now_ms<start_ms:raise InvalidState('应在首次决策前完成预热，不能追溯启动')
    anchor=store.verify_windows(needs,now_ms=now_ms)
    if anchor['faulted']:raise InvalidState('行情缓存已暂停')
    frames={}
    # Validate all required windows before constructing a runnable input set.
    for need in needs:
        rows,_=store.window(need.venue,need.symbol,need.interval,start_ms=need.start_ms,end_ms=need.end_ms,now_ms=now_ms)
        frames[need]=rows
    perpetual,spot=CausalMarket(),CausalMarket();weekly=WeeklyReferences()
    for need,rows in frames.items():
        market=perpetual if need.venue=='perpetual' else spot
        if need.interval=='5m':
            for row in rows:market.add(row.bar)
        else:
            for i in range(0,len(rows),7):
                week=rows[i:i+7];end=week[-1].bar.end_ms
                if len(week)!=7 or end+CLOCK>now_ms or any(r.bar.quote_volume<=0 for r in week):
                    raise InvalidState('预热周线缺少完整真实日线或尚未经过发布时间')
                weekly.add(WeeklyClose(need.symbol,need.venue,week[0].bar.start_ms,float(week[-1].bar.close),
                    max(end+CLOCK,*(r.bar.received_ms for r in week))))
    return MainWarmup(start_ms,now_ms,perpetual,spot,weekly,dict(seq=anchor['events'],digest=anchor['digest']))
