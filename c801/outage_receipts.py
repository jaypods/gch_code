"""Deliver actual delayed receipts to the saved strategy, without audit replay."""
from copy import deepcopy
from dataclasses import replace
import hashlib,heapq
from .account import InvalidState,SLEEVES
from .combined_controller import CombinedController,MINUTE
from .state_codec import encode,decode,validate_time
from .strategies.main import MainReceipt
from .strategies.alt import AltReceipt
from .strategies.stock import StockReceipt


def digest(raw):return hashlib.sha256(raw).hexdigest()


def action_intent(sleeve,action):
    """Stable id across crash/replay; source action remains byte-for-byte frozen.

    This is not a risk approval. The owner-run dispatcher must separately pass
    the independent supervisor and ExecutionLedger.begin_send checks.
    """
    if sleeve not in SLEEVES or not isinstance(action,dict):raise InvalidState('待执行动作缺少策略归属')
    raw=encode(action,profile='live')
    return {'id':sleeve+':'+digest(raw),'sleeve':sleeve,'action':action}


def books(controller):
    if not isinstance(controller,CombinedController) or controller.stock is None:
        raise InvalidState('中断回报恢复需要原三个完整资金账户')
    return dict(M784=controller.main.funded,A787=controller.alt,S800=controller.stock.book)


def stage_funded_receipts(controller,rows,*,input_after,known_ms):
    """Pure detached result; callers must verify durable receipt provenance.

    Original arrivals and modeled availability survive. Only an input whose
    availability predates an already committed decision receives an explicit
    processing-delay annotation. Receipts are delivered at actual recovery time.
    Stock's receipt-driven deadline request is returned, never discarded/sent.
    """
    if (not isinstance(controller,CombinedController) or controller.cycles<1 or controller.halt_reason
            or type(input_after) is not int or input_after<0 or type(known_ms) is not int
            or known_ms<controller.last_clock+2*MINUTE):
        raise InvalidState('回报恢复需要已中断的已提交状态与实际处理时间')
    result=decode(encode(controller,profile='live'),profile='live');funded=books(result)
    prior_known=max([controller.last_decision_ms]+[d['delivered_ms'] for b in funded.values() for d in b.deliveries])
    if known_ms<prior_known:raise InvalidState('中断回报实际处理时间倒退')
    before={s:dict(cash=b.known_cash,clock=b.last_clock,orders=set(b.orders),deliveries=len(b.deliveries)) for s,b in funded.items()}
    stock_actions=len(result.stock.book.actions);cursor=input_after;missed=[];delayed=[];received=[];last_arrival=-1
    for row in rows:
        if (not isinstance(row,dict) or set(row)!={'seq','event_id','kind','received_ms','payload'}
                or type(row['seq']) is not int or row['seq']!=cursor+1
                or type(row['received_ms']) is not int or not last_arrival<=row['received_ms']<=known_ms):
            raise InvalidState('中断回报输入必须是实际已收的连续前缀')
        payload=row['payload'];validate_time(payload,clock_ms=known_ms,received_ms=known_ms)
        if row['kind']=='MARKET':
            if (not isinstance(payload,dict) or set(payload)!={'frame_ms','packet'}
                    or type(payload['frame_ms']) is not int or payload['frame_ms']%MINUTE
                    or not controller.last_clock<payload['frame_ms']<=known_ms-MINUTE
                    or not isinstance(payload['packet'],dict) or 'decision_ms' in payload['packet'] or 'now' in payload['packet']):
                raise InvalidState('只能登记已错过的行情输入，不能执行或隐藏当前决策')
            missed.append(dict(input_seq=row['seq'],frame_ms=payload['frame_ms'],received_ms=row['received_ms'],reason='MISSED_DECISION_WINDOW'))
        elif row['kind']=='RECEIPT':
            if not isinstance(payload,dict) or set(payload)!={'sleeve','receipt'} or payload['sleeve'] not in SLEEVES:
                raise InvalidState('中断回报归属格式错误')
            r=payload['receipt']
            if (not isinstance(r,(MainReceipt,AltReceipt,StockReceipt))
                    or r.received_ms!=row['received_ms'] or r.event_id!=row['event_id']):
                raise InvalidState('中断回报身份或原始接收时间不一致')
            if r.available_ms<=result.last_decision_ms:
                delayed.append(dict(input_seq=row['seq'],event_id=r.event_id,received_ms=r.received_ms,
                    original_available_ms=r.available_ms,processing_ms=known_ms,available_ms=known_ms))
                r=replace(r,available_ms=known_ms)
            result.receive(payload['sleeve'],r,received_ms=row['received_ms'])
            received.append(row['seq'])
        else:raise InvalidState('中断恢复不能跳过未知控制或账户输入')
        cursor=row['seq'];last_arrival=row['received_ms']
    for sleeve,book in funded.items():
        while book.queue and book.queue[0][0]<=known_ms:book._deliver(heapq.heappop(book.queue)[2],known_ms)
        if sleeve=='M784':book._settle_groups(known_ms)
        if book.last_clock!=before[sleeve]['clock'] or set(book.orders)!=before[sleeve]['orders']:
            raise InvalidState('回报恢复意外推进策略时钟或创建订单')
    actions=deepcopy(result.stock.book.actions[stock_actions:])
    if any(a.get('kind')!='EXIT_REQUEST' or a.get('reason')!='deadline' or a.get('decision_ms')!=known_ms for a in actions):
        raise InvalidState('回报恢复产生了非持仓保护用途的动作')
    intents=tuple(action_intent('S800',a) for a in actions)
    evidence=dict(kind='FUNDED_OUTAGE_RECEIPTS',known_ms=known_ms,previous_known_ms=prior_known,
        input_after=input_after,input_through=cursor,received_inputs=tuple(received),missed_market_inputs=tuple(missed),
        processing_delays=tuple(delayed),cash_before={s:b['cash'] for s,b in before.items()},
        cash_after={s:b.known_cash for s,b in funded.items()},
        delivered={s:len(b.deliveries)-before[s]['deliveries'] for s,b in funded.items()},
        protective_intents=intents,created_entry_orders=0,strategy_clock_advanced=False)
    # Revalidate the complete detached graph, including queue and shared shadow
    # identity; a failed delivery cannot corrupt the caller's original objects.
    validate_time(result,clock_ms=result.last_clock,received_ms=known_ms)
    decode(encode(result,profile='live'),profile='live')
    return result,evidence
