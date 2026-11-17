"""Bounded Binance reads for the direct strategy runtime; no order submission.

Position and trade endpoints checked against the official Binance REST trade
reference on 2026-11-15:
https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade
"""
from .account import InvalidState, PositionKey, number, positive
from .binance_read import parse_observation
from .binance_transport import WireReply
from .execution_market import parse_book_ticker
from .simple_orders import Quote, stamp


class SimpleExchange:
    def __init__(self, transport, store, broker):
        if broker.store is not store: raise InvalidState('账户读取与订单记录必须属于同一服务')
        self.transport=transport;self.store=store;self.broker=broker
        self._filters = None
        self._filters_ms = 0
        self._brackets = {}

    def _read(self, operation, params):
        reply=self.transport.request(operation,params)
        if not isinstance(reply,WireReply) or reply.http_status!=200 or not 0<=reply.sent_ms<=reply.received_ms:
            raise InvalidState('接口读取没有完整成功响应')
        return reply

    def account(self):
        operations=('QUERY_ACCOUNT_CONFIG','QUERY_ACCOUNT','QUERY_POSITIONS','QUERY_OPEN_ORDERS','QUERY_OPEN_PROTECTIONS')
        replies=[self._read(op,{}) for op in operations]
        observation=parse_observation(*(r.body for r in replies),exchange_ms=replies[0].sent_ms,
                                      received_ms=replies[-1].received_ms,completed=operations)
        s=observation.snapshot
        # A multi-request REST observation remains explicitly non-atomic. No
        # old global "complete/reconciled" flag is invented to pass a gate.
        display=dict(equity=str(s.equity),available=str(s.available),received_ms=s.received_ms,
                     sample_start_ms=s.exchange_ms,positions=[dict(symbol=p.key.symbol,side=p.key.side,
                     amount=str(p.amount),unrealized_pnl=str(p.unrealized_pnl)) for p in s.positions])
        self.store.set_value('account_display',display)
        return observation

    def quote(self, symbol):
        reply=self._read('QUERY_BOOK_TICKER',dict(symbol=symbol))
        q=parse_book_ticker(reply,symbol=symbol,delivered_ms=self.transport.now_ms())
        return Quote(q.symbol,q.event_ms,q.received_ms,q.bid,q.ask,q.request_ms)

    def symbol_config(self, symbol):
        PositionKey(symbol,'BOTH');reply=self._read('QUERY_SYMBOL_CONFIG',dict(symbol=symbol))
        rows=reply.body
        if type(rows) is not list or len(rows)!=1 or rows[0].get('symbol')!=symbol:
            raise InvalidState('账户合约配置没有唯一匹配')
        row=rows[0];lev=row.get('leverage')
        if type(lev) is not int or not 1<=lev<=125 or row.get('marginType') not in ('CROSSED','ISOLATED') or type(row.get('isAutoAddMargin')) is not bool:
            raise InvalidState('账户合约杠杆或保证金配置错误')
        return dict(symbol=symbol,leverage=lev,margin_type=row['marginType'],auto_add_margin=row['isAutoAddMargin'],
                    maximum_notional=str(positive(row['maxNotionalValue'])),query_ms=reply.sent_ms,received_ms=reply.received_ms)

    def execution_rules(self, key, *, entry=False):
        from .execution_market import parse_execution_filters, parse_leverage_brackets, parse_mark_price
        now = self.transport.now_ms()
        if self._filters is None or now-self._filters_ms > 240000:
            reply = self._read('QUERY_MARKET_RULES', {})
            self._filters = parse_execution_filters(reply)
            self._filters_ms = reply.received_ms
        if key.symbol not in self._filters:
            raise InvalidState('交易所当前目录没有该合约')
        prior = self._brackets.get(key.symbol)
        if prior is None or now-prior.query_ms > 3300000:
            prior = parse_leverage_brackets(self._read('QUERY_LEVERAGE_BRACKETS',dict(symbol=key.symbol)),symbol=key.symbol)
            self._brackets[key.symbol] = prior
        config = self.symbol_config(key.symbol)
        mark = parse_mark_price(self._read('QUERY_MARK_PRICE',dict(symbol=key.symbol)),symbol=key.symbol) if entry else None
        rules = self._filters[key.symbol].rules(key, brackets=prior, symbol_config=config,
                                               mark=mark, now_ms=self.transport.now_ms())
        return rules, config

    def income_page(self, *, start_ms, end_ms, limit=1000):
        stamp(start_ms);stamp(end_ms)
        if not start_ms <= end_ms < self.transport.now_ms() or end_ms-start_ms>7*86400000 or type(limit) is not int or not 1<=limit<=1000:
            raise InvalidState('实际资金流水读取窗口错误')
        reply = self._read('QUERY_INCOME',dict(startTime=start_ms,endTime=end_ms,limit=limit))
        if not isinstance(reply.body,list) or len(reply.body)>limit:
            raise InvalidState('资金流水分页响应错误')
        rows=[];seen=set()
        for raw in reply.body:
            required={'symbol','incomeType','income','asset','time','tranId'}
            if not isinstance(raw,dict) or not required<=raw.keys():
                raise InvalidState('资金流水字段不完整')
            at=stamp(raw['time']);tid=str(raw['tranId']);kind=raw['incomeType']
            if not start_ms<=at<=end_ms or not tid.isdigit() or (kind,tid) in seen or not isinstance(kind,str) or not kind:
                raise InvalidState('资金流水编号或时间错误')
            number(raw['income'])
            if kind=='FUNDING_FEE':PositionKey(raw['symbol'],'BOTH')
            if not isinstance(raw['asset'],str) or not raw['asset'].isalnum():raise InvalidState('资金流水资产错误')
            seen.add((kind,tid));rows.append({k:raw[k] for k in required})
        complete=len(rows)<limit
        middle=(start_ms+end_ms)//2
        remaining=[] if complete else ([dict(start_ms=start_ms,end_ms=end_ms,saturated_millisecond=True)] if start_ms==end_ms
            else [dict(start_ms=start_ms,end_ms=middle),dict(start_ms=middle+1,end_ms=end_ms)])
        return dict(rows=rows,received_ms=reply.received_ms,complete=complete,remaining=remaining)

    def order(self, request_id):
        """Query the saved identity; never create an order to test credentials."""
        r=self.broker.query_request(request_id,self.transport)
        if r['operation']=='SEND_PROTECTION' and r['child_id']:
            args=r['body']['arguments']
            reply=self._read('QUERY_ORDER',dict(symbol=args['symbol'],orderId=r['child_id']))
            self.broker.bind_protection_child(request_id,reply.body,received_ms=reply.received_ms,query_ms=reply.sent_ms)
        return r

    def trade_page(self, symbol, *, start_ms, end_ms, limit=1000):
        """One incremental page. A full page is not proof of complete history.

        Inclusive intervals split without overlap. A saturated single
        millisecond is explicitly unresolved; it is never marked as covered.
        """
        PositionKey(symbol,'BOTH');stamp(start_ms);stamp(end_ms)
        if not start_ms<=end_ms<self.transport.now_ms() or end_ms-start_ms>7*86400000 or type(limit) is not int or not 1<=limit<=1000:
            raise InvalidState('成交读取窗口错误')
        reply=self._read('QUERY_TRADES',dict(symbol=symbol,startTime=start_ms,endTime=end_ms,limit=limit))
        if type(reply.body) is not list or len(reply.body)>limit: raise InvalidState('成交分页响应错误')
        ids=set();rows=[]
        for raw in reply.body:
            required={'symbol','id','orderId','positionSide','side','qty','price','commission','commissionAsset','realizedPnl','time'}
            if not isinstance(raw,dict) or not required<=raw.keys() or raw['symbol']!=symbol:
                raise InvalidState('成交字段或请求合约不匹配')
            PositionKey(symbol,raw['positionSide']);at=stamp(raw['time']);tid=str(raw['id']);oid=str(raw['orderId'])
            if not tid.isdigit() or not oid.isdigit() or int(oid)<1 or tid in ids or not start_ms<=at<=end_ms or raw['side'] not in ('BUY','SELL'):
                raise InvalidState('成交身份重复、时点或方向错误')
            positive(raw['qty']);positive(raw['price']);number(raw['commission']);number(raw['realizedPnl'])
            ids.add(tid);rows.append({k:raw[k] for k in required})
        complete=len(rows)<limit;pending=[]
        if not complete:
            if start_ms==end_ms:pending=[dict(start_ms=start_ms,end_ms=end_ms,saturated_millisecond=True)]
            else:
                middle=(start_ms+end_ms)//2
                pending=[dict(start_ms=start_ms,end_ms=middle),dict(start_ms=middle+1,end_ms=end_ms)]
        return dict(symbol=symbol,start_ms=start_ms,end_ms=end_ms,rows=sorted(rows,key=lambda r:(r['time'],int(r['id']))),
                    received_ms=reply.received_ms,complete=complete,remaining=pending)

    def import_known_trades(self, page):
        """Bind only exact confirmed order IDs; return unassigned trades intact.

        Unassigned may be manual trades or a not-yet-queried algo child. This
        reader never permanently labels a late strategy fill as a manual one.
        """
        delivered_ms=self.transport.now_ms()
        if page['received_ms']>delivered_ms: raise InvalidState('成交页面尚未真正收到')
        imported=0;unassigned=[]
        for row in page['rows']:
            matches=self.store.db.execute("SELECT id FROM simple_requests WHERE operation IN ('SEND_ORDER','ALGO_CHILD') AND exchange_id=? AND exchange_status IS NOT NULL AND json_extract(body,'$.arguments.symbol')=?",(str(row['orderId']),row['symbol'])).fetchall()
            if len(matches)>1: raise InvalidState('成交订单存在重复归属')
            if not matches:unassigned.append(row);continue
            imported+=int(self.broker.record_trade(matches[0][0],row,received_ms=delivered_ms,observed_ms=page['received_ms']))
        return dict(imported=imported,unassigned=unassigned,complete=page['complete'])
