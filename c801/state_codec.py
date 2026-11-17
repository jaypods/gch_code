"""Versioned, allowlisted state graph; never pickle or dynamically import names.

Object references preserve a venue/controller sharing the same strategy book.
Only explicitly registered inert data/state classes can be restored. Credentials,
connections, callbacks and arbitrary objects cannot enter a checkpoint.
"""
from collections import deque
from dataclasses import fields,is_dataclass
from decimal import Decimal
import base64,json,math,re
import numpy as np
from .account import InvalidState,PositionKey,Position,WorkingOrder,AccountSnapshot,Ownership,ContractRules
from .audit import FORBIDDEN,canonical
from .market import Bar,CausalMarket
from .strategies.main import MainQuote,MainSource,MainReceipt,MainBook
from .strategies.main_weekly import WeeklyClose,WeeklyReferences
from .strategies.alt import AltCalendar,AltReceipt,AltBook
from .strategies.alt_risk import AltRisk
from .strategies.alt_sources import HourlyWindow,FlowBar,FlowWindow,AltSourceObserver
from .strategies.stock import StockSource,StockReceipt,StockCancellationView,StockBook
from .strategies.stock_sources import StockObservation,FeatureState,StockSourceObserver
from .market_metadata import ContractMetadata
from .stock_admission import AdmittedStockSourceObserver,validate_admissions
from .main_controller import MainController
from .stock_controller import StockController
from .combined_controller import CombinedController
from .outage_state import OutageCombinedController,OutageStockBook,validate_gap_cursor
from .main_paper import MainPaperVenue
from .main_shadow import MainShadowModel

SCHEMA=1
MAX_BYTES=128*1024*1024
MAX_NODES=1_000_000

DATA=(PositionKey,Position,WorkingOrder,AccountSnapshot,Ownership,ContractRules,Bar,MainQuote,MainSource,MainReceipt,
      WeeklyClose,AltCalendar,AltReceipt,HourlyWindow,FlowBar,FlowWindow,StockSource,StockReceipt,StockCancellationView,
      StockObservation,FeatureState,ContractMetadata)
MUTABLE={
 CausalMarket:'_bars',
 MainBook:'admissions consumed_sources cool cost deliveries event_payloads group_cash group_done group_filled groups initial known_cash known_peak last_clock mode orders positions queue requests reservations retained_cash_peak risk_counts risk_index risk_updates serial start_ms terminal_ids',
 MainController:'funded shadow weekly source_audit',
 WeeklyReferences:'completed',
 AltBook:'add_attempted admissions calendar calendar_exit_requested cancel_requested cash_peak child_exit_requests consumed_sources cost cycles deliveries initial known_cash known_peak last_clock observations orders protection_requests queue receipts requests risk serial',
 AltRisk:'batches cash_peak commit cost decisions events initial populations',
 AltSourceObserver:'conditional_history decisions events last_clock last_leader last_mother mother_count rank_checks source_evidence states',
 StockBook:'actions cancelled_sources consumed cost decisions deliveries gates initial known_cash last_clock last_decision_ms orders performance queue receipts recent_outcomes serial sources start_ms',
 StockSourceObserver:'active births conditional_history features frames history known_history last_clock last_known_ms lifecycle missing_observations previous start_ms symbols validity_history warmup_frames',
 StockController:'observer book halt_reason source_audit',
 CombinedController:'main shadow_model alt alt_observer stock stock_start_ms stock_initial stock_cost perpetual spot start_ms last_clock last_decision_ms halt_reason cycles',
 MainPaperVenue:'actual_cash attempts book equity funding_records last_minute orders positions requests serial trades',
 MainShadowModel:'actual_cash attempts book equity funding_records last_minute orders positions requests serial trades',
}
MUTABLE[OutageCombinedController]=MUTABLE[CombinedController]+' outage_transitions'
MUTABLE[OutageStockBook]=MUTABLE[StockBook]+' outage_transitions'
MUTABLE[AdmittedStockSourceObserver]=MUTABLE[StockSourceObserver]+' roster_admissions'
REGISTRY={c.__module__+'.'+c.__name__:c for c in (*DATA,*MUTABLE)}
FIELDS={c:tuple(f.name for f in fields(c)) if is_dataclass(c) else tuple(MUTABLE[c].split()) for c in (*DATA,*MUTABLE)}
PAPER_TYPES={MainPaperVenue}


def sensitive_key(key):
    return isinstance(key,str) and re.sub('[^a-z]','',key.lower()) in FORBIDDEN


def set_key(value):
    # Stable across processes/hash seeds, without changing dictionary order
    # (which can affect strategy tie-breaking and arithmetic accumulation).
    if value is None or type(value) in (str,int,bool):return canonical([type(value).__name__,value])
    if type(value) is float:return canonical(['float',value.hex()])
    if isinstance(value,Decimal):return canonical(['decimal',str(value)])
    if type(value) is tuple:return canonical(['tuple',[set_key(x) for x in value]])
    if type(value) is frozenset:return canonical(['frozenset',sorted(set_key(x) for x in value)])
    if type(value) is PositionKey:return canonical(['PositionKey',value.symbol,value.side])
    raise InvalidState('检查点集合元素类型不支持稳定排序')


def _validate_state(obj):
    # Domain-specific books require finite cash and coherent pending receipts.
    if isinstance(obj,(MainReceipt,AltReceipt,StockReceipt)):obj.validate()
    if isinstance(obj,StockSource):obj.validate(obj.available_ms)
    if isinstance(obj,MainSource):obj.validate(obj.decision_ms)
    if isinstance(obj,MainQuote):obj.validate(obj.close_ms+300000)
    if isinstance(obj,(MainBook,AltBook,StockBook)):
        if not math.isfinite(obj.initial) or obj.initial<=0 or not math.isfinite(obj.known_cash) or obj.cost not in (1,2):
            raise InvalidState('恢复的策略资金状态无效')
        if type(obj.last_clock) is not int or type(obj.serial) is not int or obj.serial<0:raise InvalidState('恢复的策略时钟或回报序号无效')
        known=obj.event_payloads if isinstance(obj,MainBook) else obj.receipts
        seen=set()
        for i,item in enumerate(obj.queue):
            if not isinstance(item,tuple) or len(item)!=3:raise InvalidState('恢复的回报队列格式错误')
            available,serial,payload=item
            cutoff=getattr(obj,'last_decision_ms',obj.last_clock)
            if isinstance(obj,OutageStockBook):cutoff=max(cutoff,obj.last_clock)
            if type(available) is not int or available<=cutoff or type(serial) is not int or not 0<serial<=obj.serial or serial in seen:
                raise InvalidState('恢复的回报队列时序或唯一性错误')
            seen.add(serial)
            if i and obj.queue[(i-1)//2][:2]>item[:2]:raise InvalidState('恢复的回报队列排序错误')
            event=payload.get('event_id')
            if event not in known or known[event]!=payload:raise InvalidState('恢复的回报队列与去重记录不符')
    if isinstance(obj,FeatureState):
        if type(obj.count) is not int or obj.count<0 or len(obj.quotes)>5760 or len(obj.closes)>25 or obj.closes.maxlen!=25:
            raise InvalidState('恢复的股票指标窗口无效')
        if obj.count and not math.isfinite(obj.atr):raise InvalidState('恢复的股票 ATR 无效')
    if isinstance(obj,StockController) and (not isinstance(obj.book,StockBook) or not isinstance(obj.observer,StockSourceObserver)):
        raise InvalidState('恢复的股票控制器依赖错误')
    if isinstance(obj,CombinedController):
        if not isinstance(obj.main,MainController) or not isinstance(obj.alt,AltBook) or not isinstance(obj.alt_observer,AltSourceObserver) or not isinstance(obj.perpetual,CausalMarket) or not isinstance(obj.spot,CausalMarket):
            raise InvalidState('恢复的组合控制器依赖错误')
        if (type(obj.stock_start_ms) is not int or obj.stock_start_ms<obj.start_ms or obj.stock_start_ms%300000
                or not math.isfinite(obj.stock_initial) or obj.stock_initial<=0 or obj.stock_cost not in (1,2)):
            raise InvalidState('恢复的股票覆盖起点或独立本金无效')
        if obj.stock_start_ms>obj.start_ms and not obj.alt_observer.conditional_history:
            raise InvalidState('股票延后启动必须保留历史覆盖假设')
        if obj.stock is None:
            if obj.stock_start_ms<=obj.start_ms or obj.last_clock>=obj.stock_start_ms or not obj.alt_observer.conditional_history:
                raise InvalidState('恢复的股票等待覆盖状态无效')
        elif (not isinstance(obj.stock,StockController) or obj.stock.book.start_ms!=obj.stock_start_ms
                or obj.stock.book.initial!=obj.stock_initial or obj.stock.book.cost!=obj.stock_cost):
            raise InvalidState('恢复的股票账户与原始配置不符')
        missed=validate_gap_cursor(obj) if isinstance(obj,OutageCombinedController) else 0
        if obj.cycles<0 or obj.last_clock!=obj.start_ms-60000+(obj.cycles+missed)*60000:raise InvalidState('恢复的组合控制器时钟错误')
        if not isinstance(obj.shadow_model,MainShadowModel) or obj.shadow_model.book is not obj.main.shadow:raise InvalidState('恢复的组合影子模型与机会账户不一致')
        if isinstance(obj,OutageCombinedController):
            if not isinstance(obj.stock.book,OutageStockBook):raise InvalidState('股票账户缺少配套中断记录')
            keys=('previous_clock_ms','resume_ms','known_ms','missed_minutes')
            if ([tuple(e[k] for k in keys) for e in obj.outage_transitions]
                    !=[tuple(e[k] for k in keys) for e in obj.stock.book.outage_transitions]):
                raise InvalidState('组合与股票中断范围不一致')
            five=obj.last_clock//300000*300000
            if (obj.stock.book.last_clock!=obj.last_clock or obj.stock.observer.last_clock!=five
                    or any(b.last_clock!=five for b in (obj.main.funded,obj.main.shadow,obj.alt))
                    or obj.shadow_model.last_minute!=obj.last_clock-60000
                    or obj.alt_observer.last_clock!=(obj.last_clock-300000)//3600000*3600000+300000):
                raise InvalidState('恢复后的三策略、来源和影子分钟时钟不一致')
    if isinstance(obj,OutageStockBook):validate_gap_cursor(obj)
    if isinstance(obj,(StockBook,CombinedController)) and (type(obj.last_decision_ms) is not int
            or not isinstance(obj,(OutageCombinedController,OutageStockBook)) and not obj.last_clock<=obj.last_decision_ms<obj.last_clock+60000):
        raise InvalidState('恢复的实际决策时钟与分钟边界不一致')
    if isinstance(obj,StockSourceObserver):
        if set(obj.history)!=set(obj.known_history) or set(obj.history)!=set(obj.validity_history) or type(obj.last_known_ms) is not int:
            raise InvalidState('恢复的来源可知时间记录不完整')
        if any(type(k) is not int or type(v) is not int or not k<=v<=obj.last_known_ms for k,v in obj.known_history.items()):raise InvalidState('恢复的来源可知时间无效')
    if isinstance(obj,AdmittedStockSourceObserver):validate_admissions(obj)
    if isinstance(obj,MainController) and (not isinstance(obj.funded,MainBook) or obj.funded.mode!='M784' or not isinstance(obj.shadow,MainBook) or obj.shadow.mode!='shadow'):
        raise InvalidState('恢复的主流币资金账户与影子账户混淆')
    if isinstance(obj,MainPaperVenue):
        expected=MainBook
        if not isinstance(obj.book,expected):raise InvalidState('恢复的模拟成交器与策略账户不符')
        if isinstance(obj,MainShadowModel) and (obj.book.mode!='shadow' or obj.book.initial!=1000.):raise InvalidState('恢复的影子模型不能接入真实资金账户')


def validate_time(root,*,clock_ms,received_ms):
    """Do not persist future arrivals as evidence already received now.

    A strategy's delayed availability may legitimately be in the future, but
    actual exchange events/receipts and completed strategy clocks cannot be.
    """
    seen=set()
    def walk(x):
        if id(x) in seen:return
        seen.add(id(x))
        if isinstance(x,(MainBook,AltBook,StockBook,AltSourceObserver,StockSourceObserver,CombinedController)):
            if x.last_clock>clock_ms:raise InvalidState('检查点包含晚于当前周期的策略状态')
        if isinstance(x,(StockBook,CombinedController)) and x.last_decision_ms>received_ms:raise InvalidState('检查点包含未来作出的决策')
        if isinstance(x,(OutageStockBook,OutageCombinedController)) and any(e['known_ms']>received_ms for e in x.outage_transitions):
            raise InvalidState('检查点包含尚未完成的中断恢复')
        if isinstance(x,StockSourceObserver) and x.last_known_ms>received_ms:raise InvalidState('检查点包含未来收到的来源状态')
        if isinstance(x,(MainBook,AltBook,StockBook)):
            events=x.event_payloads if isinstance(x,MainBook) else x.receipts
            if any(p['received_ms']>received_ms for p in events.values()):raise InvalidState('检查点包含尚未实际收到的成交回报')
        if isinstance(x,(Bar,MainQuote,MainReceipt,AltReceipt,StockReceipt,AccountSnapshot,WeeklyClose)) and x.received_ms>received_ms:
            raise InvalidState('检查点包含未来接收的行情或回报')
        if isinstance(x,AltCalendar) and x.known_ms>received_ms:raise InvalidState('检查点包含尚未公布的合约日历')
        if isinstance(x,ContractMetadata) and x.known_ms>received_ms:raise InvalidState('检查点包含未来接收的合约身份')
        if type(x) is dict:
            for k,v in x.items():walk(k);walk(v)
        elif type(x) in (list,tuple,set,frozenset,deque):
            for v in x:walk(v)
        elif type(x) in FIELDS:
            for key in FIELDS[type(x)]:walk(getattr(x,key))
    walk(root)


def encode(root,*,profile='paper'):
    if profile not in ('paper','live'):raise InvalidState('检查点运行环境错误')
    nodes=[];memo={};active=set()
    def visit(value,depth=0):
        if depth>150:raise InvalidState('检查点嵌套超过限制')
        if value is None or type(value) in (str,int,bool):return value
        if type(value) is float:
            return value if math.isfinite(value) else {'$float':'nan' if math.isnan(value) else '+inf' if value>0 else '-inf'}
        if isinstance(value,np.generic):return visit(value.item(),depth+1)
        if isinstance(value,Decimal):
            if not value.is_finite():raise InvalidState('检查点金额不能为非有限值')
            return {'$decimal':str(value)}
        identity=id(value)
        if identity in memo:
            if identity in active:raise InvalidState('检查点不接受循环引用')
            return {'$ref':memo[identity]}
        if len(nodes)>=MAX_NODES:raise InvalidState('检查点对象数量超过限制')
        index=len(nodes);memo[identity]=index;nodes.append(None);active.add(identity)
        v=lambda x:visit(x,depth+1)
        if type(value) is dict:
            if any(sensitive_key(k) for k in value):raise InvalidState('检查点拒绝敏感字段')
            node={'type':'dict','items':[[v(k),v(x)] for k,x in value.items()]}
        elif type(value) in (list,tuple,set,frozenset,deque):
            sequence=sorted(value,key=set_key) if type(value) in (set,frozenset) else value
            node={'type':type(value).__name__,'items':[v(x) for x in sequence]}
            if type(value) is deque:node['maxlen']=value.maxlen
        elif isinstance(value,np.ndarray):
            if value.dtype.kind not in 'biuf' or value.dtype.itemsize>8 or value.nbytes>MAX_BYTES:raise InvalidState('检查点数组类型或大小不支持')
            node={'type':'ndarray','dtype':value.dtype.str,'shape':list(value.shape),'data':base64.b64encode(value.tobytes(order='C')).decode('ascii'),'writeable':bool(value.flags.writeable)}
        elif type(value) in FIELDS:
            if profile=='live' and type(value) in PAPER_TYPES:raise InvalidState('实盘检查点不能混入模拟成交器')
            if profile=='live' and isinstance(value,CombinedController) and value.stock_start_ms!=value.start_ms:
                raise InvalidState('历史覆盖延后启动不能进入实盘检查点')
            if set(value.__dict__)!=set(FIELDS[type(value)]):raise InvalidState('检查点对象字段与冻结格式不符')
            _validate_state(value)
            node={'type':type(value).__module__+'.'+type(value).__name__,'fields':{k:v(getattr(value,k)) for k in FIELDS[type(value)]}}
        else:raise InvalidState('检查点不支持此对象类型')
        nodes[index]=node;active.remove(identity);return {'$ref':index}
    graph={'schema':SCHEMA,'profile':profile,'root':visit(root),'nodes':nodes}
    raw=canonical(graph).encode('utf8')
    if len(raw)>MAX_BYTES:raise InvalidState('检查点超过大小限制')
    return raw


def decode(raw,*,profile='paper'):
    if not isinstance(raw,bytes) or len(raw)>MAX_BYTES:raise InvalidState('检查点数据类型或大小错误')
    def no_duplicates(pairs):
        d={}
        for k,v in pairs:
            if k in d:raise InvalidState('检查点 JSON 字段重复')
            d[k]=v
        return d
    try:g=json.loads(raw.decode('utf8'),object_pairs_hook=no_duplicates,parse_constant=lambda x:(_ for _ in ()).throw(InvalidState('检查点非法数值')))
    except (UnicodeError,ValueError,TypeError):raise InvalidState('检查点 JSON 无效') from None
    if not isinstance(g,dict) or set(g)!={'schema','profile','root','nodes'} or g['schema']!=SCHEMA or g['profile']!=profile or profile not in ('paper','live'):
        raise InvalidState('检查点版本或运行环境不匹配')
    nodes=g['nodes']
    if not isinstance(nodes,list) or len(nodes)>MAX_NODES:raise InvalidState('检查点节点列表无效')
    memo={};active=set();restored=[]
    def restore(value,depth=0):
        if depth>150:raise InvalidState('检查点嵌套超过限制')
        if value is None or type(value) in (str,int,bool,float):return value
        if not isinstance(value,dict) or len(value)!=1:raise InvalidState('检查点引用格式错误')
        if '$decimal' in value:
            try:d=Decimal(value['$decimal'])
            except Exception:raise InvalidState('检查点金额无效') from None
            if not d.is_finite():raise InvalidState('检查点金额无效')
            return d
        if '$float' in value:
            if value['$float'] not in ('nan','+inf','-inf'):raise InvalidState('检查点特殊数值错误')
            return float(value['$float'])
        i=value.get('$ref')
        if type(i) is not int or not 0<=i<len(nodes):raise InvalidState('检查点引用越界')
        if i in active:raise InvalidState('检查点不能包含循环引用')
        if i in memo:return memo[i]
        n=nodes[i]
        if not isinstance(n,dict) or not isinstance(n.get('type'),str):raise InvalidState('检查点节点格式错误')
        active.add(i);kind=n['type'];v=lambda x:restore(x,depth+1)
        if kind in ('dict','list','tuple','set','frozenset','deque'):
            expected={'type','items'}|({'maxlen'} if kind=='deque' else set())
            if set(n)!=expected or not isinstance(n['items'],list):raise InvalidState('检查点容器字段错误')
            if kind=='dict':
                result={}
                for pair in n['items']:
                    if not isinstance(pair,list) or len(pair)!=2:raise InvalidState('检查点字典项错误')
                    key,item=v(pair[0]),v(pair[1])
                    if sensitive_key(key):raise InvalidState('检查点拒绝敏感字段')
                    try:
                        if key in result:raise InvalidState('检查点字典键重复')
                        result[key]=item
                    except TypeError:raise InvalidState('检查点字典键类型错误') from None
            else:
                items=[v(x) for x in n['items']]
                if kind=='deque':
                    size=n['maxlen']
                    if size is not None and (type(size) is not int or size<0 or len(items)>size):raise InvalidState('检查点队列长度错误')
                    result=deque(items,maxlen=size)
                else:
                    try:result={'list':list,'tuple':tuple,'set':set,'frozenset':frozenset}[kind](items)
                    except TypeError:raise InvalidState('检查点集合项错误') from None
                    if kind in ('set','frozenset') and len(result)!=len(items):raise InvalidState('检查点集合存在重复值')
        elif kind=='ndarray':
            if set(n)!={'type','dtype','shape','data','writeable'} or type(n['writeable']) is not bool or not isinstance(n['shape'],list) or len(n['shape'])>8:
                raise InvalidState('检查点数组字段错误')
            try:dt=np.dtype(n['dtype'])
            except Exception:raise InvalidState('检查点数组类型错误') from None
            if dt.kind not in 'biuf' or dt.itemsize>8 or any(type(x) is not int or x<0 for x in n['shape']):raise InvalidState('检查点数组类型或维度错误')
            count=math.prod(n['shape']);size=count*dt.itemsize
            if size>MAX_BYTES:raise InvalidState('检查点数组超过大小限制')
            try:data=base64.b64decode(n['data'],validate=True)
            except Exception:raise InvalidState('检查点数组编码错误') from None
            if len(data)!=size:raise InvalidState('检查点数组长度不符')
            result=np.frombuffer(data,dtype=dt).copy().reshape(n['shape']);result.flags.writeable=n['writeable']
        else:
            cls=REGISTRY.get(kind)
            if cls is None or profile=='live' and cls in PAPER_TYPES:raise InvalidState('检查点对象类型不在允许名单')
            if set(n)!={'type','fields'} or not isinstance(n['fields'],dict) or set(n['fields'])!=set(FIELDS[cls]):raise InvalidState('检查点对象字段错误')
            attrs={k:v(x) for k,x in n['fields'].items()}
            result=object.__new__(cls)
            for key,item in attrs.items():object.__setattr__(result,key,item)
            restored.append(result)
        memo[i]=result;active.remove(i);return result
    try:
        root=restore(g['root'])
        if len(memo)!=len(nodes):raise InvalidState('检查点有未引用的隐藏节点')
        for obj in restored:
            if profile=='live' and isinstance(obj,CombinedController) and obj.stock_start_ms!=obj.start_ms:
                raise InvalidState('历史覆盖延后启动不能进入实盘检查点')
            # Dataclass constructors with I/O do not exist in the allowlist.
            # Frozen validators run only after all graph dependencies restore.
            if hasattr(obj,'__post_init__'):obj.__post_init__()
            _validate_state(obj)
        return root
    except InvalidState:raise
    except Exception:raise InvalidState('检查点领域状态不合法') from None
