def independent_unit(ref, side, cost):
    p=D(str(ref)); side=D(side); fee=D('.0005')*cost; slip=D('.0004')*cost
    entry=p*(1+side*D('.015'))*(1+side*slip)
    stop=p*(1-side*D('.03'))*(1-side*slip)
    return side*(entry-stop)+fee*(entry+stop)

"""Independent cash, observed state, minute exits, waiting orders and complete source census."""
from collections import Counter
from decimal import Decimal as D
import numpy as np
START=1704067200000;END=1790812800000;M=60000;C=300000;SYMS=('BTCUSDT','ETHUSDT')
PRE={'nonpositive_known_equity','minimum_model_notional','known_quote_capacity','known_margin'}
def audit(r,raw,raw5,fund,marks,feat,warm,end=END,weekly=()):
    weekly_map={(x['symbol'],x['decision_ms']):x for x in weekly};cost=r['summary']['cost']
    ts=raw['BTCUSDT']['open_time'];n=len(ts);grid=raw5['BTCUSDT']['open_time'];n5=len(grid);terminal=end-C;tr=r['trades'];orders=r['orders'];fee=.0005*r['summary']['cost'];slip=.0004*r['summary']['cost'];assert np.array_equal(ts,np.arange(START,end,M))and np.array_equal(grid,np.arange(START,end,C));applied={}
    def near(a,b,label):assert abs(a-b)<=1e-6+1e-10*abs(b),(label,a,b)
    def delivery(t):return START+((int(t)-START+C-1)//C)*C
    cash=np.zeros(n);unreal=np.zeros(n);kd=np.zeros(n5);ku=np.zeros(n5);km=np.zeros(n5);expected_cash=[];expected_receipts=[];scan=0;fund_by={i:[]for i in range(len(tr))}
    def ce(k,kind,i,v):cash[k]+=v;expected_cash.append((k,kind,i,float(v)))
    def re(t,kind,delta,i=None,o=None):
        if delivery(t)<end:expected_receipts.append((int(t),delivery(t),kind,i,o,float(delta)))
    for a in tr:
        i=a['trade_id'];s=a['symbol'];side=a['side'];q=a['qty'];ei=a['entry_idx'];xi=a['exit_idx'];b=raw[s];assert a['entry_ms']==int(ts[ei])and a['entry_ms']>=a['planned_ms']==a['decision_ms']+C and a['entry_ms']<=terminal
        assert a['source_available_ms']==a['source_ms']+C==a['decision_ms'];assert a['entry_lower_ms']==a['entry_ms']and a['entry_upper_ms']==a['entry_ms']+M
        near(a['raw_entry'],float(b['open'][ei]),'rawentry');near(a['entry_fill'],a['raw_entry']*(1+side*slip),'entryfill');near(a['entry_fee'],q*a['entry_fill']*fee,'entryfee');near(a['exit_fill'],a['raw_exit']*(1-side*slip),'exitfill');near(a['exit_fee'],q*a['exit_fill']*fee,'exitfee')
        assert a['entry_receipt_ms']==a['entry_upper_ms']+C and a['exit_receipt_ms']==a['exit_ms']+C;assert a['exit_lower_ms']==int(ts[xi])and a['exit_upper_ms']==int(ts[xi])+M;assert a['exit_ms']==a['exit_upper_ms'];assert a['exit_ms']-a['entry_ms']<=144*3600000+C
        stop=a['initial_stop'];target=a['initial_target'];found=None;commands=[z for z in r['requests']if z['trade']==i]
        for k in range(ei,xi+1):
            scan+=1
            printed=float(b['quote_volume'][k])>0;op=float(b['open'][k]);lo=float(b['low'][k]);hi=float(b['high'][k]);t=int(ts[k]);kind='profit_lock'if stop!=a['initial_stop']else'stop'
            if printed and k>ei and side*(op-stop)<=0:found=(k,kind,'open',op)
            elif printed and k>ei and side*(op-target)>=0:found=(k,'target','open',target)
            elif printed and k>ei and t>=a['planned_ms']+144*3600000:found=(k,'time','open',op)
            if found:break
            if t<terminal:
                for req in commands:
                    if req['effective_ms']<=t and req['id']not in applied:stop=req['new_stop'];applied[req['id']]=t
            if not printed:continue
            kind='profit_lock'if stop!=a['initial_stop']else'stop'
            if t>=terminal:found=(k,'window_end','open',op)
            elif k>ei and side*(op-stop)<=0:found=(k,kind,'open',op)
            elif(lo<=stop if side>0 else hi>=stop):found=(k,kind,'intrabar',stop)
            elif(hi>=target if side>0 else lo<=target):found=(k,'target','intrabar',target)
            if found:break
        if a['exit_type']=='maintenance_proxy':assert found is None
        else:assert found is not None and found[:3]==(xi,a['exit_type'],a['exit_phase']),(i,found,a['exit_type']);near(a['raw_exit'],found[3],'rawexit')
        assert a['unprinted_exposure_bars']==int(np.sum(b['quote_volume'][ei+1:xi+1]<=0));ex=q*side*(a['exit_fill']-a['entry_fill'])-a['exit_fee'];ce(ei,'entry_fee',i,-a['entry_fee']);ce(xi,'exit',i,ex);re(a['entry_receipt_ms'],'entry',-a['entry_fee'],i,a['id']);re(a['exit_receipt_ms'],'exit',ex,i)
        unreal[ei:xi]+=q*side*(b['close'][ei:xi]-a['entry_fill']);ka=min(n5,(delivery(a['entry_receipt_ms'])-START)//C);kb=min(n5,(delivery(a['exit_receipt_ms'])-START)//C);ix=np.arange(ka,kb);prices=np.where(ix>=2,raw5[s]['close'][np.maximum(ix-2,0)],warm[s]['price']);ku[ka:kb]+=q*side*(prices-a['entry_fill']);km[ka:kb]+=q*a['raw_entry']/a['lev']
        for ft,rate in fund[s]:
            ft=int(ft);rate=float(rate)
            if not START<ft<end or ft<a['entry_ms']or ft>a['exit_upper_ms']:continue
            k=(ft-START)//M
            if k>xi:continue
            j=int(np.searchsorted(marks[s]['open_time'],ft-2*C,side='right')-1);assert j>=0;mark=float(marks[s]['close'][j]);av=int(marks[s]['open_time'][j])+2*C;assert av<=ft;amb=ft<a['entry_ms']+M or k==xi;signed=-side*q*mark*rate;flow=min(0.,signed)if amb else signed
            f=dict(event_ms=ft,rate=rate,mark=mark,reference_open_ms=int(marks[s]['open_time'][j]),reference_available_ms=av,ambiguous=amb,nominal_flow=signed,flow=flow,upper_flow=max(0.,signed)if amb else signed,booking_idx=k,available_ms=ft+C);fund_by[i].append(f);ce(k,'funding',i,flow);re(ft+C,'funding',flow,i)
        got=[x for x in r['funding']if x['trade']==i];assert len(got)==len(fund_by[i])
        for f,g in zip(got,fund_by[i]):
            for k,v in g.items():
                if isinstance(v,float):near(f[k],v,'fund '+k)
                else:assert f[k]==v,(i,k)
        fsum=sum(x['flow']for x in fund_by[i]);near(a['funding'],fsum,'tradefund');net=q*side*(a['exit_fill']-a['entry_fill'])-a['entry_fee']-a['exit_fee']+fsum;near(a['net_pnl'],net,'net');near(a['raw_pnl'],q*side*(a['raw_exit']-a['raw_entry']),'rawpnl');near(a['slippage'],a['raw_pnl']-q*side*(a['exit_fill']-a['entry_fill']),'slippage')
    for q in r['requests']:assert q['applied_ms']==applied.get(q['id']),(q,applied.get(q['id']))
    for o in orders:
        if o['status']=='rejected'and o['execution_ms']is not None:re(o['receipt_available_ms'],'reject',0.,o=o['id'])
    def rk(x):return(x[0],x[1],x[2],-1 if x[3]is None else x[3],-1 if x[4]is None else x[4])
    got=[(e['available_ms'],e['delivered_ms'],e['kind'],e['trade'],e['order'],e['delta'])for e in r['receipts']];assert len(got)==len(expected_receipts)
    for a,b in zip(sorted(got,key=rk),sorted(expected_receipts,key=rk)):assert a[:5]==b[:5],(a,b);near(a[5],b[5],'receipt')
    for e in r['receipts']:assert e['delivered_ms']==delivery(e['available_ms']);kd[(e['delivered_ms']-START)//C]+=e['delta']
    actualcash=[(e['bar'],e['kind'],e['trade'],e['delta'])for e in r['cash_events']];assert len(actualcash)==len(expected_cash)
    for a,b in zip(sorted(actualcash),sorted(expected_cash)):assert a[:3]==b[:3];near(a[3],b[3],'cash')
    for e in r['cash_events']:assert e['booking_ms']==int(ts[e['bar']])+M
    curve=1000+np.cumsum(cash)+unreal;assert np.array_equal(r['curve'][:,0],ts+M);err=float(np.max(np.abs(r['curve'][:,1]-curve)));assert err<1e-6,err
    kc=1000+np.cumsum(kd);ke=kc+ku;peak=np.maximum.accumulate(np.maximum(ke,1000.))
    for o in orders:
        t=o['decision_ms'];k=(t-START)//C;j=(o['source_ms']-START)//900000;s=o['symbol'];side=o['side'];assert t==o['source_ms']+C
        if o['family']=='ordinary':
            assert o['source_ms']==START+j*900000 and abs(feat[s]['ret4'][j])>=.002 and feat[s]['vol96'][j]<=.0045 and side==(1 if feat[s]['ret4'][j]>0 else-1);ref=float(feat[s]['reference'][j]);limit=float(feat[s]['prior_high'if side>0 else'prior_low'][j]);share=1.
        else:
            wp=weekly_map[(s,t)];assert side==1 and o['weekly_event']==wp['id'] and o['source_ms']==wp['source_close_ms'];assert o['reference_kind']in ['perpetual','spot'];ref=wp['reference'];limit=wp['limits'][o['reference_kind']];share=.5
        assert o['share']==share
        near(o['reference'],ref,'ref');near(o['known_cash'],kc[k],'knowncash');near(o['known_equity'],ke[k],'knowneq');near(o['known_margin'],km[k],'knownmargin');near(o['known_peak'],peak[k],'knownpeak');near(o['known_dd'],1-ke[k]/peak[k],'knowndd')
        prior=[a['id']for a in orders[:o['id']]if a['group']!=o['group'] and a['reject_reason']not in PRE and(a['receipt_available_ms']is None or delivery(a['receipt_available_ms'])>t)];assert sorted(prior)==sorted(o['reserved_before']);friction=sum(orders[i]['reserved_friction']for i in prior);rm=sum(orders[i]['reserved_margin']for i in prior);avail=ke[k]-friction;dd=1-ke[k]/peak[k];assert o['risk_scope']==o['symbol'];lev=4;assert o["risk_index"]==EXPECTED_RISK[o["id"]]["index"] and o["risk_event_count"]==EXPECTED_RISK[o["id"]]["count"];retained_peak,retained=EXPECTED_RETENTION[o['id']];near(o['retained_cash_peak'],retained_peak,'receivedcashpeak');near(o['profit_retention'],retained,'retainedprofit');sizing_base=max(0.,avail-retained);near(o['sizing_base'],sizing_base,'retentionbase');unconstrained=float(D(str(sizing_base))*D('.030')/independent_unit(o['reference'],o['side'],cost));quote=float(raw5[s]['quote_volume'][k-2])if k>=2 else warm[s]['quote'];expected=float(min(D(str(sizing_base))*D('.030')/independent_unit(o['reference'],o['side'],cost),D(str(quote))*D('.01')/D(str(o['reference']))));assert lev==o['lev'];near(o['qty'],expected*share,'decimal-fixedqty');near(o['unconstrained_qty'],unconstrained*share,'unconstrainedqty');near(o['capacity_qty'],.01*quote/o['reference']*share,'capacityqty');assert o['quantity_clipped']==(unconstrained*o['reference']>.01*quote);q=o['qty']/share;assert q<=unconstrained+1e-10 and q*o['reference']<=.01*quote+1e-8;near(o['available_after_reserve'],avail,'afterreserve')
        near(o['initial_stop'],o['reference']*(1-side*.03),'stop');near(o['initial_target'],o['reference']*(1+side*.08),'target');near(o['entry_cap_distance'],o['reference']*.015,'gapcap');assert o['planned_ms']==t+C and o['expiry_ms']==o['source_ms']+14400000;near(o['fixed_limit'],limit,'limit')
        quote=float(raw5[s]['quote_volume'][k-2])if k>=2 else warm[s]['quote'];near(o['capacity_quote'],quote,'capacity');fc=q*o['reference']*(slip+(1+slip)*fee);required=q*o['reference']/lev;near(o['reserved_friction'],fc*share,'reservedfriction');near(o['reserved_margin'],required*share,'reservedmargin')
        reason='nonpositive_known_equity'if avail<=0 else'minimum_model_notional'if q*o['reference']<(40 if o['family']=='weekly'else 20) else'known_quote_capacity'if q*o['reference']>.01*quote else'known_margin'if km[k]+rm+required>avail-fc else None
        if reason:assert o['reject_reason']==reason and o['execution_ms']is None
        else:assert o['reject_reason']not in PRE
        if o['status']=='filled':assert o['qty']==tr[o['trade']]['qty']
    # Independently reconstruct all ordinary and weekly opportunities and same-clock priority.
    grouped={}
    for o in orders:grouped.setdefault((o['family'],o['symbol'],o['source_ms']),[]).append(o)
    for key,gg in grouped.items():
        assert len(gg)==(2 if key[0]=='weekly'else 1)
        if key[0]=='weekly':
            assert {x['reference_kind']for x in gg}=={'perpetual','spot'} and gg[0]['qty']==gg[1]['qty']
            for name in ['decision_ms','planned_ms','expiry_ms','reference','initial_stop','initial_target','reserved_before','known_cash','known_equity','known_peak','lev']:assert gg[0][name]==gg[1][name]
    source_events=[]
    for j in range(len(feat['BTCUSDT']['ret4'])):
        source=START+j*900000;t=source+C
        if t>terminal:continue
        for s in SYMS:
            sig=float(feat[s]['ret4'][j]);vol=float(feat[s]['vol96'][j])
            if np.isfinite(sig)and abs(sig)>=.002 and np.isfinite(vol)and vol<=.0045:source_events.append((t,0,s,source,'ordinary'))
    for w in weekly:
        if START<=w['decision_ms']<=terminal:source_events.append((w['decision_ms'],1,w['symbol'],w['source_close_ms'],'weekly'))
    source_events.sort();census=Counter();wanted=set()
    for t,priority,s,source,family in source_events:
        key=(family,s,source);census['sources']+=1;census[family+'_sources']+=1
        earlier=lambda o:(o['decision_ms'],0 if o['family']=='ordinary'else 1,o['symbol'])<(t,priority,s)
        pending=[o for o in orders if o['symbol']==s and earlier(o) and o['reject_reason']not in PRE and(o['receipt_available_ms']is None or delivery(o['receipt_available_ms'])>t)]
        known=[a for a in tr if a['symbol']==s and delivery(a['entry_receipt_ms'])<=t<delivery(a['exit_receipt_ms'])]
        latest=max([START-6*3600000]+[a['exit_ms']for a in tr if a['symbol']==s and delivery(a['exit_receipt_ms'])<=t])
        why='reserved'if pending else'known_position'if known else'cooldown'if t<latest+6*3600000 else'submitted';census[why]+=1;census[family+'_'+why]+=1
        if why=='submitted':wanted.add(key);assert key in grouped
        else:assert key not in grouped,(key,why)
    assert wanted==set(grouped);assert census['sources']==r['counts']['signals']and census['reserved']+census['known_position']==r['counts']['occupied']and census['cooldown']==r['counts']['cooldown']

    # Every minute attempt, including waiting, expiries, refusals and true exchange-side margin.
    attempts={(x['order'],x['time_ms']):x for x in r['attempts']};assert len(attempts)==len(r['attempts']);expected=set();before=np.r_[1000.,1000+np.cumsum(cash)[:-1]]
    for o in orders:
        if o['reject_reason']in PRE:continue
        resolved=False
        for t in range(o['planned_ms'],terminal+1,C):
            key=(o['id'],t);expected.add(key);x=attempts[key]
            if t>=o['expiry_ms']:want='limit_expired';assert x['raw_open']is None and x['slipped_open']is None
            else:
                k=(t-START)//M;s=o['symbol'];b=raw[s];px=float(b['open'][k]);fill=px*(1+o['side']*slip);near(x['raw_open'],px,'attemptopen');near(x['slipped_open'],fill,'attemptfill')
                ch=EXPECTED_REFRESH.get(o['id']);active_limit=ch['new_limit'] if ch and ch['eligible'] and t>ch['effect_ms'] else o['fixed_limit']
                want='unprinted_execution'if float(b['quote_volume'][k])<=0 else'gap_outside_precommitted_bracket'if o['side']*(px-o['initial_stop'])<=0 or o['side']*(o['initial_target']-px)<=0 else'gap_exceeds_cap'if abs(px-o['reference'])>o['entry_cap_distance'] and not ((px<o['reference'] and fill<=active_limit and o['initial_stop']<fill<o['initial_target']) if o['side']==1 else (px>o['reference'] and fill>=active_limit and o['initial_target']<fill<o['initial_stop']))else'wait_limit'if o['side']*(fill-active_limit)>0 else None
                if want is None and o['family']=='weekly' and not(o['initial_stop']<fill<o['initial_target']):want='slipped_fill_outside_bracket'
                if want is None:
                    cashnow=before[k];active=[]
                    for a in tr:
                        if a['entry_idx']<k and a['exit_idx']==k and a['exit_phase']=='open'and a['exit_type']!='window_end':cashnow+=a['qty']*a['side']*(a['exit_fill']-a['entry_fill'])-a['exit_fee']
                        if a['entry_idx']==k and a['id']<o['id']:cashnow-=a['entry_fee'];active.append(a)
                        elif a['entry_idx']<k<=a['exit_idx']and not(a['exit_idx']==k and a['exit_phase']=='open'and a['exit_type']!='window_end'):active.append(a)
                    eq=cashnow+sum(a['qty']*a['side']*(float(raw[a['symbol']]['open'][k])-a['entry_fill'])for a in active);margin=sum(a['qty']*a['raw_entry']/a['lev']for a in active);near(o['execution_equity'],eq,'executioneq');near(o['execution_margin'],margin,'executionmargin');want='actual_margin'if eq<=0 or margin+o['qty']*px/o['lev']>eq-o['qty']*fill*fee else'filled'
            assert x['action']==want,(o['id'],t,want,x['action'])
            if want!='wait_limit':
                resolved=True;assert o['execution_ms']==t and o['receipt_available_ms']==t+C+(0 if want=='limit_expired'else M)
                if want=='filled':assert o['status']=='filled'and tr[o['trade']]['entry_ms']==t
                else:assert o['status']=='rejected'and o['reject_reason']==want
                break
        if not resolved:assert o['status']=='unexecuted_window_end'
    assert expected==set(attempts)
    for k in sorted({a['exit_idx']for a in tr if a['exit_type']=='maintenance_proxy'}):
        liquid=[a for a in tr if a['exit_idx']==k and a['exit_type']=='maintenance_proxy'];money=1000+float(np.sum(cash[:k+1]))-sum(a['qty']*a['side']*(a['exit_fill']-a['entry_fill'])-a['exit_fee']for a in liquid);active=[a for a in tr if a['entry_idx']<=k and(a['exit_idx']>k or a in liquid)];equity=money+sum(a['qty']*a['side']*(float(raw[a['symbol']]['close'][k])-a['entry_fill'])for a in active);gross=sum(a['qty']*float(raw[a['symbol']]['close'][k])for a in active);assert equity<=.005*gross
    values=np.r_[1000.,curve];final=float(values[-1]);dd=float(100*np.max(1-values/np.maximum.accumulate(values)));near(final,1000+sum(a['net_pnl']for a in tr),'finalcash');near(final,r['summary']['final_usdt'],'final');near(dd,r['summary']['max_drawdown_pct'],'DD');days=(end-START)/86400000;near(100*((final/1000)**(365.25/days)-1)if final>0 else-100.,r['summary']['cagr_pct'],'CAGR')
    return dict(passed=True,trades=len(tr),orders=len(orders),receipts=len(r['receipts']),funding_events=len(r['funding']),equity_points=n,known_strategy_points=n5,held_native_minutes_scanned=scan,max_equity_error_usdt=err,minute_attempts_checked=len(expected),all_source_occurrences_checked=census['sources'],admission_census=dict(census),fixed_quantity_and_delayed_cash_checked=True,all_exit_paths_checked=True,source_gap_free_certified=False,real_execution_certified=False)
