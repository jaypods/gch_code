"""Explicit append-only stock roster transitions at actual observation time.

Old decisions, source ranks and financial books are never replayed. Historical
bars only warm the new symbol's indicators and last missed direction. Original
arrival times are retained in the journaled input; no historical mother is born.
The original observer class/encoding stays unchanged until a real admission.
"""
import hashlib
from copy import deepcopy
from .account import InvalidState,PositionKey
from .market_metadata import ContractMetadata
from .strategies.stock import FIVE
from .strategies.stock_sources import StockSourceObserver,StockObservation,FeatureState,LIQUIDITY_BARS


class AdmittedStockSourceObserver(StockSourceObserver):
    """Only explicitly admitted observers carry roster_admissions."""
    pass


def warmup_ends(through_ms,listed_ms):
    if (type(through_ms) is not int or through_ms<FIVE or through_ms%FIVE
            or type(listed_ms) is not int or listed_ms<0):
        raise InvalidState('新增合约预热边界无效')
    first=max(through_ms-LIQUIDITY_BARS*FIVE,listed_ms//FIVE*FIVE+FIVE,FIVE)
    return range(first,through_ms+FIVE,FIVE)


def validate_admissions(observer):
    log=observer.roster_admissions
    if not isinstance(log,list) or not log:raise InvalidState('扩展股票观察器缺少接纳记录')
    prior=None;known=0;clock=0
    keys={'before_symbols','added_symbols','through_ms','known_ms','catalog_known_ms','catalog_digest',
          'input_sha256','warmup_bars','missed_direction','created_sources','created_orders'}
    for row in log:
        if not isinstance(row,dict) or set(row)!=keys:raise InvalidState('股票接纳记录字段错误')
        before,added=row['before_symbols'],row['added_symbols']
        if (type(before) is not tuple or type(added) is not tuple or not before or not added
                or len(set(before+added))!=len(before+added)
                or prior is not None and before!=prior):
            raise InvalidState('股票接纳记录不是保留原排序的追加')
        for s in before+added:PositionKey(s,'LONG')
        if (any(type(row[k]) is not int for k in ('through_ms','known_ms','catalog_known_ms','created_sources','created_orders'))
                or row['through_ms']%FIVE or row['through_ms']<clock or row['through_ms']>observer.last_clock
                or not max(row['through_ms'],known,row['catalog_known_ms'])<=row['known_ms']<=observer.last_known_ms
                or row['catalog_known_ms']<0 or row['created_sources']!=0 or row['created_orders']!=0):
            raise InvalidState('股票接纳时序或历史下单记录无效')
        for key in ('catalog_digest','input_sha256'):
            value=row[key]
            if type(value) is not str or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
                raise InvalidState('股票接纳证据摘要无效')
        if (set(row['warmup_bars'])!=set(added) or set(row['missed_direction'])!=set(added)
                or any(type(v) is not int or not 0<=v<=LIQUIDITY_BARS+1 for v in row['warmup_bars'].values())
                or any(type(v) is not int or v not in (-1,0,1) for v in row['missed_direction'].values())):
            raise InvalidState('股票接纳指标预热记录无效')
        prior=before+added;known=row['known_ms'];clock=row['through_ms']
    if (observer.symbols!=prior or set(observer.features)!=set(prior) or set(observer.previous)!=set(prior)):
        raise InvalidState('股票观察范围与接纳记录不符')


def admit_stock_roster(observer,payload,*,known_ms):
    """Validate and compute all new features before an in-memory atomic swap."""
    if (not isinstance(observer,StockSourceObserver) or type(payload) is not dict
            or set(payload)!={'previous_symbols','target_symbols','through_ms','catalog_known_ms',
                              'catalog_digest','contracts','observations'}):
        raise InvalidState('股票接纳输入字段错误')
    before=payload['previous_symbols'];target=payload['target_symbols'];through=payload['through_ms']
    if (type(before) is not tuple or type(target) is not tuple or before!=observer.symbols
            or target[:len(before)]!=before or len(target)<=len(before) or len(set(target))!=len(target)
            or type(through) is not int or through!=observer.last_clock or through%FIVE
            or type(known_ms) is not int or known_ms<max(through,observer.last_known_ms)
            or type(payload['catalog_known_ms']) is not int or not 0<=payload['catalog_known_ms']<=known_ms):
        raise InvalidState('股票接纳不能重排、重置旧状态或使用未来资料')
    digest=payload['catalog_digest']
    if type(digest) is not str or len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest):
        raise InvalidState('股票接纳缺少完整目录摘要')
    added=target[len(before):];contracts=payload['contracts'];observations=payload['observations']
    if (type(contracts) is not tuple or len(contracts)!=len(added)
            or any(not isinstance(c,ContractMetadata) for c in contracts)
            or tuple(c.symbol for c in contracts)!=added or type(observations) is not dict or set(observations)!=set(added)):
        raise InvalidState('新增股票身份或预热范围不完整')
    features={};directions={};counts={}
    for c in contracts:
        PositionKey(c.symbol,'LONG')
        if (c.category!='stock_us' or c.contract_type!='TRADIFI_PERPETUAL' or c.underlying_type!='EQUITY'
                or any(type(v) is not int for v in (c.known_ms,c.listed_ms,c.delivery_ms))
                or c.known_ms!=payload['catalog_known_ms'] or not 0<=c.listed_ms< c.delivery_ms
                or not c.listed_ms<=known_ms or c.delivery_ms<=known_ms):
            raise InvalidState('新增股票身份、上市状态或可知时间无效')
        rows=observations[c.symbol];ends=warmup_ends(through,c.listed_ms)
        if type(rows) is not tuple or len(rows)!=len(ends):raise InvalidState('新增股票指标预热存在缺口')
        state=FeatureState();direction=0
        for end,o in zip(ends,rows):
            if not isinstance(o,StockObservation) or o.status!='present' or o.metadata_known_ms!=c.known_ms:
                raise InvalidState('新增股票历史不能伪装成未上市或隔离')
            o.validate(c.symbol,end,False,known_ms=known_ms)
            row,change=state.preview(o);state.commit(change)
            direction=(1 if row['momentum']>0 else -1) if row['eligible'] else 0
        features[c.symbol]=state;directions[c.symbol]=direction;counts[c.symbol]=len(rows)
    from .state_codec import encode
    evidence=dict(before_symbols=before,added_symbols=added,through_ms=through,known_ms=known_ms,
        catalog_known_ms=payload['catalog_known_ms'],catalog_digest=digest,
        input_sha256=hashlib.sha256(encode(payload,profile='live')).hexdigest(),
        warmup_bars=counts,missed_direction=directions,created_sources=0,created_orders=0)
    candidate=object.__new__(AdmittedStockSourceObserver)
    candidate.__dict__.update(observer.__dict__)
    candidate.symbols=target;candidate.features=dict(observer.features,**features)
    candidate.previous=dict(observer.previous,**directions)
    candidate.roster_admissions=list(getattr(observer,'roster_admissions',()))+[evidence]
    candidate.last_known_ms=known_ms
    validate_admissions(candidate)
    observer.__class__=AdmittedStockSourceObserver;observer.__dict__=candidate.__dict__
    return deepcopy(evidence)


def validate_roster_extension(original,current):
    """Composition may add audited members but must preserve the original prefix."""
    before=tuple(original.symbols);after=tuple(current.symbols)
    old=tuple(getattr(original,'roster_admissions',()))
    log=tuple(getattr(current,'roster_admissions',()))
    if after==before and log==old:return
    if not isinstance(current,AdmittedStockSourceObserver):raise InvalidState('股票范围改变缺少显式接纳记录')
    validate_admissions(current)
    if (after[:len(before)]!=before or len(after)<=len(before) or log[:len(old)]!=old
            or len(log)<=len(old) or log[len(old)]['before_symbols']!=before):
        raise InvalidState('股票范围迁移改变了已知历史或原排序')


def validity_at(observer,end,symbol):
    rows=observer.validity_history[end]
    if symbol in rows:return rows[symbol]
    # Do not rewrite old snapshots to add the new symbol retrospectively.
    if any(symbol in row['added_symbols'] and end<=row['through_ms']
           for row in getattr(observer,'roster_admissions',())):return False
    raise InvalidState('原股票资格快照存在缺项')
