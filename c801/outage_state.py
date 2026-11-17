"""Explicit gap cursors, distinct from executed strategy decisions.

The strategy methods are inherited unchanged. Existing serialized classes keep
their original fields. Only an explicitly composed outage state uses these
extensions; composition is detached and grants no broker or resume permission.
"""
from copy import deepcopy
from .account import InvalidState
from .combined_controller import CombinedController,MINUTE
from .strategies.stock import StockBook,FIVE
from .strategies.alt_sources import HOUR,STEP


class OutageCombinedController(CombinedController):
    """cycles counts actual decisions; outage_transitions accounts for gaps."""
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs);self.outage_transitions=[]


class OutageStockBook(StockBook):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs);self.outage_transitions=[]


GAP_FIELDS={'previous_clock_ms','previous_decision_ms','resume_ms','known_ms','missed_minutes'}


def validate_gap_cursor(obj):
    """Validate explicit missed slots without inventing a decision timestamp."""
    if any(type(getattr(obj,k)) is not int for k in ('start_ms','last_clock','last_decision_ms')):
        raise InvalidState('中断时钟必须为毫秒整数')
    log=obj.outage_transitions
    if not isinstance(log,list) or not log:raise InvalidState('恢复控制器缺少明确的中断记录')
    total=0;end=obj.start_ms-MINUTE;last_known=-1
    for event in log:
        if not isinstance(event,dict) or set(event)!=GAP_FIELDS or any(type(v) is not int for v in event.values()):
            raise InvalidState('中断时钟记录格式错误')
        before,decision,resume,known,missed=(event[k] for k in ('previous_clock_ms','previous_decision_ms','resume_ms','known_ms','missed_minutes'))
        if (before%MINUTE or resume%MINUTE or before<end or not 0<=decision<before+MINUTE
                or not resume<=known<resume+MINUTE or known<last_known
                or missed<1 or resume-MINUTE-before!=missed*MINUTE):
            raise InvalidState('中断时钟范围、真实决策时间或处理时间错误')
        total+=missed;end=resume-MINUTE;last_known=known
    if obj.last_clock<end:raise InvalidState('恢复时钟倒退到已登记的中断范围')
    if obj.last_clock==end:
        if obj.last_decision_ms!=log[-1]['previous_decision_ms']:raise InvalidState('中断游标伪造了新的策略决策时间')
    elif not obj.last_clock<=obj.last_decision_ms<obj.last_clock+MINUTE:
        raise InvalidState('恢复后的真实决策时钟与分钟边界不一致')
    return total


def _extend(obj,cls,event):
    result=object.__new__(cls);result.__dict__.update(deepcopy(obj.__dict__))
    result.outage_transitions=deepcopy(getattr(obj,'outage_transitions',[]))+[dict(event)]
    return result


def compose_outage_state(original,funded,sources,*,resume_ms,known_ms,perpetual_bars=(),spot_bars=(),weekly_closes=()):
    """Compose audited staged parts; never install, send, or infer a fill.

    The caller separately verifies source/receipt journals, market provenance,
    current ownership, pending broker plans, protection and funding continuity.
    This function cannot certify any of those external release conditions.
    """
    from .outage_receipts import books
    from .state_codec import encode,decode,validate_time
    from .outage_receipts import digest
    from .main_shadow import MainShadowModel
    from .strategies.stock_sources import StockSourceObserver
    from .strategies.alt_sources import AltSourceObserver
    from .market import Bar
    from .strategies.main_weekly import WeeklyClose,ORIGIN
    if (not isinstance(original,CombinedController) or not isinstance(funded,CombinedController)
            or original.stock is None or funded.stock is None
            or original.cycles<1 or original.halt_reason or funded.halt_reason
            or type(resume_ms) is not int or resume_ms%MINUTE or type(known_ms) is not int
            or not resume_ms<=known_ms<resume_ms+MINUTE or resume_ms<original.last_clock+2*MINUTE):
        raise InvalidState('中断合成需要原已运行检查点与当前可恢复分钟')
    if (funded.start_ms,funded.last_clock,funded.last_decision_ms,funded.cycles)!=(original.start_ms,original.last_clock,original.last_decision_ms,original.cycles):
        raise InvalidState('回报副本改变了原运行时钟、起点或真实决策次数')
    if not isinstance(sources,dict) or set(sources)!={'M784','A787','S800'}:
        raise InvalidState('中断合成缺少三个来源状态')
    if (not isinstance(sources['M784'],MainShadowModel) or not isinstance(sources['A787'],AltSourceObserver)
            or not isinstance(sources['S800'],StockSourceObserver)):
        raise InvalidState('中断合成的来源类型错误')
    cursor=resume_ms-MINUTE;five=cursor//FIVE*FIVE;hour=(cursor-STEP)//HOUR*HOUR+STEP
    expected={'M784':cursor,'A787':hour,'S800':five}
    for sleeve,source in sources.items():
        if (source.last_minute+MINUTE if sleeve=='M784' else source.last_clock)!=expected[sleeve]:raise InvalidState('中断来源尚未连续恢复到同一边界')
        validate_time(source,clock_ms=cursor,received_ms=known_ms)
    if (sources['M784'].book.mode!='shadow' or sources['M784'].book.initial!=1000.
            or set(sources['M784'].orders)!=set(original.shadow_model.orders)
            or sources['S800'].start_ms!=original.stock.observer.start_ms
            or sources['A787'].conditional_history!=original.alt_observer.conditional_history):
        raise InvalidState('中断合成改变了影子订单、股票范围或覆盖假设')
    from .stock_admission import validate_roster_extension
    validate_roster_extension(original.stock.observer,sources['S800'])
    if any(row.get('known_ms',row['decision_ms'])>known_ms for row in sources['A787'].states):
        raise InvalidState('山寨恢复状态使用了后续才收到的观察')
    old_books,new_books=books(original),books(funded)
    for sleeve,b in new_books.items():
        before=old_books[sleeve]
        if (b.initial,b.cost,b.last_clock)!=(before.initial,before.cost,before.last_clock) or set(b.orders)!=set(before.orders):
            raise InvalidState('中断回报副本重设了本金、时钟或订单范围')
        if any(row['delivered_ms']>known_ms for row in b.deliveries):raise InvalidState('中断合成提前使用了后续回报处理结果')
        if b.queue and b.queue[0][0]<=known_ms:raise InvalidState('中断合成仍有到期回报未入账')
    if funded.stock.book.last_decision_ms!=original.stock.book.last_decision_ms:
        raise InvalidState('股票回报副本伪造了新的决策')
    if sources['M784'].book.queue and sources['M784'].book.queue[0][0]<=known_ms:
        raise InvalidState('影子恢复仍有到期回报未入账')
    if any(r['delivered_ms']>known_ms for r in sources['M784'].book.deliveries):raise InvalidState('影子处理结果来自未来')
    event=dict(previous_clock_ms=original.last_clock,previous_decision_ms=original.last_decision_ms,
        resume_ms=resume_ms,known_ms=known_ms,missed_minutes=(cursor-original.last_clock)//MINUTE)
    result=_extend(original,OutageCombinedController,event)
    result.main.funded=deepcopy(new_books['M784']);result.alt=deepcopy(new_books['A787'])
    stock_event=dict(event,previous_clock_ms=old_books['S800'].last_clock,
        previous_decision_ms=old_books['S800'].last_decision_ms,
        missed_minutes=(cursor-old_books['S800'].last_clock)//MINUTE)
    if stock_event['previous_clock_ms']!=original.last_clock:raise InvalidState('原股票与组合时钟不一致，不能掩盖已有缺口')
    result.stock.book=_extend(new_books['S800'],OutageStockBook,stock_event)
    result.stock.observer=deepcopy(sources['S800']);result.alt_observer=deepcopy(sources['A787'])
    result.shadow_model=deepcopy(sources['M784']);result.main.shadow=result.shadow_model.book
    prior_shadow_clock=result.main.shadow.last_clock
    if prior_shadow_clock not in (five, five-FIVE) or prior_shadow_clock!=(cursor-MINUTE)//FIVE*FIVE:
        raise InvalidState('影子分钟模型与机会决策时钟不一致')
    for b in (result.main.funded,result.main.shadow,result.alt):b.last_clock=five
    result.stock.book.last_clock=cursor;result.last_clock=cursor
    # Original last_decision_ms and actual cycles are intentionally unchanged.
    observed_inputs=[]
    for venue,market,rows in (('perpetual',result.perpetual,perpetual_bars),('spot',result.spot,spot_bars)):
        for bar in rows:
            if not isinstance(bar,Bar) or bar.period_ms!=FIVE or bar.end_ms>resume_ms or bar.received_ms>known_ms:
                raise InvalidState('恢复行情不是实际已收的完整五分钟数据')
            market.add(bar);observed_inputs.append((venue,bar))
    weekly_closes=tuple(weekly_closes)
    for close in weekly_closes:
        if not isinstance(close,WeeklyClose) or close.received_ms>known_ms:raise InvalidState('恢复周线尚未实际收到')
        result.main.weekly.add(close)
    # Preserve M784's original frame cutoff (not the later recovery cutoff).
    # Bars downloaded after that cutoff cannot masquerade as previously known.
    next_main=(resume_ms+FIVE-1)//FIVE*FIVE;end=next_main-FIVE
    cutoff=min(known_ms,next_main)
    for symbol in ('BTCUSDT','ETHUSDT'):
        result.perpetual.window(symbol,FIVE,end,291,now_ms=cutoff,lag_ms=0)
        if end>ORIGIN+FIVE:
            for market_name,market in (('perpetual',result.perpetual),('spot',result.spot)):
                previous,current=market.window(symbol,FIVE,end,2,now_ms=cutoff,lag_ms=0)
                for bar in (previous,current):result.main.weekly.live30(symbol,market_name,bar,cutoff)
    result._compact_observations()
    # A codec round-trip checks graph sharing, explicit missed-slot arithmetic,
    # unchanged actual clocks and still-delayed receipt queues.
    validate_time(result,clock_ms=cursor,received_ms=known_ms)
    result=decode(encode(result,profile='live'),profile='live')
    proof=dict(kind='EXPLICIT_OUTAGE_CURSOR',event=event,stock_event=stock_event,
        original_state_hash=digest(encode(original,profile='live')),
        funded_state_hash=digest(encode(funded,profile='live')),
        source_hashes={s:digest(encode(p,profile='live')) for s,p in sources.items()},
        market_input_hash=digest(encode(dict(bars=observed_inputs,weekly_closes=tuple(weekly_closes)),profile='live')),
        resulting_state_hash=digest(encode(result,profile='live')),actual_decisions_preserved=original.cycles,
        missed_entry_orders=0,shadow_boundary_decision_skipped=prior_shadow_clock!=five,
        external_account_reconciliation_certified=False,installed=False)
    return result,proof
