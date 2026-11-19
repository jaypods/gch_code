def risk_unit(ref, side, fee, slip):
    entry = ref * (1 + side * .015) * (1 + side * slip)
    stop_fill = ref * (1 - side * .03) * (1 - side * slip)
    return side * (entry - stop_fill) + fee * (entry + stop_fill)

def profit_level(b,t,start,entry,stop,delivered,side):
 clock=300000;j=(t-start)//clock-2
 if j<0 or j>=len(b['open_time']):return None
 begin=int(b['open_time'][j]);price=float(b['close'][j]);quote=float(b['quote_volume'][j])
 if begin!=t-2*clock or begin<delivered or not np.isfinite(price+quote)or price<=0 or quote<=0:return None
 e=Decimal(str(entry));r=abs(e-Decimal(str(stop)))
 if r<=0 or side*(Decimal(str(price))-e)<2*r:return None
 new=float(e+side*r)
 if not np.isfinite(new)or new<=0:raise ValueError('Unsupported nonpositive profit stop')
 return dict(source_open_ms=begin,source_close_ms=begin+clock,source_available_ms=begin+2*clock,source_close=price,source_quote=quote,risk_distance=float(r),trigger_price=float(e+side*2*r),new_stop=new)
"""Native minute exchange path with unchanged five-minute observed strategy clock."""
import heapq
from decimal import Decimal
import numpy as np
START=1704067200000;END=1790812800000;MINUTE=60000;CLOCK=300000
SYMS=('BTCUSDT','ETHUSDT');HOLD=144*3600000;COOL=6*3600000

def run(raw,raw5,fund,marks,feat,warm,cost,end=END,weekly=()):
    ts=raw['BTCUSDT']['open_time'];n=len(ts);terminal=end-CLOCK;fee=.0005*cost;slip=.0004*cost
    raw=dict(raw);raw5=dict(raw5);warm=dict(warm);weekly_by_time={}
    for p in weekly:
        if START<=p['decision_ms']<=terminal:weekly_by_time.setdefault(p['decision_ms'],[]).append(p)
    for sym in SYMS:
        for kind in ['perpetual','spot']:
            key=sym+'::weekly_'+kind;raw[key]=raw[sym];raw5[key]=raw5[sym];warm[key]=warm[sym]
    for vv in weekly_by_time.values():vv.sort(key=lambda x:x['symbol'])
    actual_cash=known_cash=1000.;known_peak=1000.;retained_cash_peak=1000.;risk_index={s:2 for s in SYMS};risk_counts={s:0 for s in SYMS};actual={};known={};cool={s:START for s in SYMS};reservations={}
    refresh_by_time={}
    for ev in REFRESH_SOURCES:
        if START<=ev["decision_ms"]<end:refresh_by_time.setdefault(ev["decision_ms"],[]).append(ev)
    group_members={};terminal_ids=set();group_cash={};group_filled=set();group_done=set();risk_updates=[]
    orders=[];trades=[];attempts=[];funding=[];cash_events=[];receipts=[];queue=[];serial=0;curve=[];requests=[];requested_stop={};pending_stop={};profit_requested=set();entry_delivered={}
    counts=dict(signals=0,occupied=0,cooldown=0,liquidations=0,unprinted_bars=0,unprinted_maintenance_bars=0,opposite_while_known_open=0)
    events=sorted((int(t),s,float(r))for s in SYMS for t,r in fund[s]if START<t<end);fi=0
    def enqueue(available,kind,delta=0.,trade=None,order=None):
        nonlocal serial
        serial+=1;heapq.heappush(queue,(available,serial,dict(kind=kind,delta=delta,trade=trade,order=order)))
    def cash(delta,k,kind,trade):
        nonlocal actual_cash
        actual_cash+=delta;cash_events.append(dict(bar=k,booking_ms=int(ts[k])+MINUTE,kind=kind,trade=trade,delta=delta))
    def known_state(k5):
        eq=known_cash;margin=0.
        for s,i in known.items():
            a=trades[i];p=float(raw5[s]['close'][k5-2])if k5>=2 else float(warm[s]['price']);eq+=a['side']*a['qty']*(p-a['entry_fill']);margin+=a['qty']*a['raw_entry']/a['lev']
        return eq,margin
    def actual_state(k,field):
        eq=actual_cash;gross=margin=0.
        for s,i in actual.items():
            a=trades[i];p=float(raw[s][field][k]);eq+=a['side']*a['qty']*(p-a['entry_fill']);gross+=a['qty']*p;margin+=a['qty']*a['raw_entry']/a['lev']
        return eq,gross,margin
    def plan(s,side,source,ref,k,preview=False):
        t=int(ts[k]);k5=(t-START)//CLOCK;eq,margin=known_state(k5);reserved=list(reservations.values());friction=sum(orders[i]['reserved_friction']for i in reserved);rm=sum(orders[i]['reserved_margin']for i in reserved)
        avail=eq-friction;dd=1-eq/max(known_peak,1e-12);lev=4;profit_retention=.5*max(0.,retained_cash_peak-1000.);sizing_base=max(0.,avail-profit_retention);q=sizing_base*.030/risk_unit(ref,side,fee,slip);stop=ref*(1-side*.03);target=ref*(1+side*.08)
        quote=float(raw5[s]['quote_volume'][k5-2])if k5>=2 else float(warm[s]['quote']);unconstrained=q;capacity_qty=.01*quote/ref;q=float(np.nextafter(capacity_qty,0.))if q*ref>.01*quote else q;fc=q*ref*(slip+(1+slip)*fee);required=q*ref/lev
        reason='nonpositive_known_equity'if avail<=0 else'minimum_model_notional'if q*ref<20 else'known_quote_capacity'if q*ref>.01*quote else'known_margin'if margin+rm+required>avail-fc else None;j=(source-START)//900000
        o=dict(id=len(orders),symbol=s,side=side,source_ms=source,source_available_ms=source+CLOCK,decision_ms=t,known_price_ms=t-CLOCK,reference=ref,initial_stop=stop,initial_target=target,entry_cap_distance=.015*ref,qty=q,lev=lev,known_cash=known_cash,known_equity=eq,known_margin=margin,known_peak=known_peak,known_dd=dd,risk_index=risk_index[s],risk_event_count=risk_counts[s],risk_scope=s,group_margin_budget=0.30,retained_cash_peak=retained_cash_peak,profit_retention=profit_retention,sizing_base=sizing_base,reserved_before=reserved,available_after_reserve=avail,reserved_margin=required,reserved_friction=fc,capacity_quote=quote,origin='ordinary',request_id=None,planned_ms=t+CLOCK,status='rejected'if reason else'pending',reject_reason=reason,receipt_available_ms=None,execution_ms=None,trade=None,fixed_limit=float(feat[s]['prior_high'if side>0 else'prior_low'][j]),expiry_ms=source+14400000)
        o.update(stop_risk_fraction=.030,unit_stop_risk=risk_unit(ref,side,fee,slip),sizing_rule='fixed_stop_risk',unconstrained_qty=unconstrained,capacity_qty=capacity_qty,quantity_clipped=q<unconstrained)
        o.update(position_key=s,family='ordinary',group='ordinary_'+str(o['id']),share=1.,weekly_event=None)
        if preview:return o
        orders.append(o);group_members[o["group"]]=[o["id"]]
        if reason is not None:terminal_ids.add(o["id"])
        if reason is None:reservations[s]=o['id']
    def plan_weekly(p,k):
        s=p['symbol'];o=plan(s,1,p['source_close_ms'],p['reference'],k,preview=True)
        assert o['decision_ms']==p['decision_ms'] and o['planned_ms']==p['planned_ms'] and o['expiry_ms']==p['expiry_ms']
        # One decision reserves the entire pair. No first-leg friction can resize its sibling.
        if o['reject_reason']!='nonpositive_known_equity' and o['qty']*o['reference']<40:o.update(status='rejected',reject_reason='minimum_model_notional')
        group='weekly_'+str(p['id']);q=o['qty'];fc=o['reserved_friction'];rm=o['reserved_margin']
        for kind in ['perpetual','spot']:
            key=s+'::weekly_'+kind;leg=dict(o,id=len(orders),position_key=key,family='weekly',group=group,share=.5,weekly_event=p['id'],reference_kind=kind,fixed_limit=p['limits'][kind],qty=q*.5,unconstrained_qty=o['unconstrained_qty']*.5,capacity_qty=o['capacity_qty']*.5,reserved_friction=fc*.5,reserved_margin=rm*.5)
            orders.append(leg);group_members.setdefault(group,[]).append(leg["id"])
            if leg["reject_reason"]is not None:terminal_ids.add(leg["id"])
            if leg['reject_reason']is None:reservations[key]=leg['id']

    def close(i,k,px,kind,phase):
        a=trades[i];t=int(ts[k]);fill=px*(1-a['side']*slip);fx=a['qty']*fill*fee;delta=a['qty']*a['side']*(fill-a['entry_fill'])-fx;cash(delta,k,'exit',i);stamp=t+MINUTE;available=stamp+CLOCK
        a.update(exit_idx=k,exit_ms=stamp,exit_lower_ms=t,exit_upper_ms=t+MINUTE,exit_phase=phase,raw_exit=px,exit_fill=fill,exit_fee=fx,exit_type=kind,status='closed',exit_receipt_ms=available,reversal_request_id=None);actual.pop(a['position_key']);enqueue(available,'exit',delta,i)
    for k,t0 in enumerate(ts):
        t=int(t0)
        if(t-START)%CLOCK==0:
            while queue and queue[0][0]<=t:
                available,seq,e=heapq.heappop(queue);known_cash+=e['delta']
                if e['kind']=='entry':s=trades[e['trade']]['position_key'];known[s]=e['trade'];reservations.pop(s,None);requested_stop[e['trade']]=trades[e['trade']]['initial_stop'];entry_delivered[e['trade']]=t
                elif e['kind']=='reject':reservations.pop(orders[e['order']]['position_key'],None)
                elif e['kind']=='exit':
                    a=trades[e['trade']];s=a['symbol'];key=a['position_key'];assert known.get(key)==e['trade'];known.pop(key);cool[s]=max(cool[s],a['exit_ms']+COOL)
                receipts.append(dict(available_ms=available,delivered_ms=t,serial=seq,**e))
                if e['trade']is not None:
                    a=trades[e['trade']];g=a['group'];assert g not in group_done
                    group_cash[g]=group_cash.get(g,Decimal(0))+Decimal(str(e['delta']))
                    if e['kind']=='entry':group_filled.add(g)
                    if e['kind']=='exit':terminal_ids.add(a['id'])
                elif e['kind']=='reject':terminal_ids.add(e['order'])
            ready=sorted(g for g,ids in group_members.items()if g not in group_done and all(i in terminal_ids for i in ids))
            closed=[g for g in ready if g in group_filled]
            for coin in SYMS:
                own=[g for g in closed if orders[group_members[g][0]]['symbol']==coin]
                if not own:continue
                assert all(orders[i]['symbol']==coin for g in own for i in group_members[g])
                net=sum((group_cash[g]for g in own),Decimal(0));prior_index=risk_index[coin]
                risk_index[coin]=max(0,min(2,prior_index+(1 if net>0 else -1 if net<0 else 0)));risk_counts[coin]+=1
                risk_updates.append(dict(time_ms=t,symbol=coin,groups=own,net_decimal=str(net),net=float(net),prior_index=prior_index,index=risk_index[coin],leverage=(4,10,20)[risk_index[coin]]))
            group_done.update(ready)
            retained_cash_peak=max(retained_cash_peak,known_cash)
            eq,_=known_state((t-START)//CLOCK);known_peak=max(known_peak,eq)
            if t+CLOCK<terminal:
                for key,i in sorted(known.items(),key=lambda item:item[1]):
                    if i in profit_requested:continue
                    a=trades[i];level=profit_level(raw5[a['symbol']],t,START,a['entry_fill'],a['initial_stop'],entry_delivered[i],a['side'])
                    if level is None:continue
                    old=requested_stop[i];new=level['new_stop'];assert a['side']*(new-old)>0
                    req=dict(id=len(requests),trade=i,order=a['id'],symbol=a['symbol'],side=a['side'],decision_ms=t,effective_ms=t+CLOCK,old_stop=old,new_stop=new,qty=a['qty'],entry_receipt_ms=a['entry_receipt_ms'],entry_delivered_ms=entry_delivered[i],observation=level,applied_ms=None)
                    requests.append(req);pending_stop.setdefault(i,[]).append(req);requested_stop[i]=new;profit_requested.add(i)
            for ev in refresh_by_time.get(t,[]):
                for oi in list(reservations.values()):
                    o=orders[oi]
                    if o.get('refresh') is not None or t<=o['decision_ms'] or t+CLOCK>=o['expiry_ms'] or ev['symbol']!=o['symbol'] or ev['side']!=o['side']:continue
                    lim=ev['fixed_limit'];side=o['side'];valid=side*(lim-o['fixed_limit'])>0 and side*(lim-o['initial_stop'])>0 and side*(o['initial_target']-lim)>0
                    o['refresh']=dict(source=ev,request_ms=t,effect_ms=t+CLOCK,acknowledgment_ms=t+2*CLOCK,new_limit=lim,eligible=bool(valid),reason='refresh' if valid else 'first_source_not_usable')
            if(t-START)%900000==CLOCK and t<=terminal:
                j=(t-START-CLOCK)//900000;source=t-CLOCK
                for s in SYMS:
                    sig=float(feat[s]['ret4'][j]);vol=float(feat[s]['vol96'][j])
                    if not np.isfinite(sig)or abs(sig)<.002 or not np.isfinite(vol)or vol>.0045:continue
                    counts['signals']+=1;side=1 if sig>0 else-1
                    if any(orders[i]['symbol']==s for i in reservations.values()):counts['occupied']+=1;continue
                    if any(trades[i]['symbol']==s for i in known.values()):
                        if any(trades[i]['symbol']==s and trades[i]['side']!=side for i in known.values()):counts['opposite_while_known_open']+=1
                        counts['occupied']+=1;continue
                    if t<cool[s]:counts['cooldown']+=1;continue
                    plan(s,side,source,float(feat[s]['reference'][j]),k)
            # Existing ordinary sources have priority at the same decision clock.
            for p in weekly_by_time.get(t,[]):
                s=p['symbol'];counts['signals']+=1
                if any(orders[i]['symbol']==s for i in reservations.values()):counts['occupied']+=1;continue
                if any(trades[i]['symbol']==s for i in known.values()):counts['occupied']+=1;continue
                if t<cool[s]:counts['cooldown']+=1;continue
                plan_weekly(p,k)
        observed=set(actual.values())
        for s,i in list(actual.items()):
            a=trades[i];op=float(raw[s]['open'][k]);q=float(raw[s]['quote_volume'][k]);side=a['side']
            # Existing opening protection/deadline has priority over a new command.
            if q>0:
                if side*(op-a['active_stop'])<=0:close(i,k,op,'profit_lock'if a['active_stop']!=a['initial_stop']else'stop','open');continue
                elif side*(op-a['initial_target'])>=0:close(i,k,a['initial_target'],'target','open');continue
                elif t>=a['planned_ms']+HOLD:close(i,k,op,'time','open');continue
            if t<terminal:
                for req in pending_stop.get(i,[]):
                    if req['applied_ms']is None and req['effective_ms']<=t:
                        a['active_stop']=req['new_stop'];req['applied_ms']=t
            if q<=0:a['unprinted_exposure_bars']+=1;counts['unprinted_bars']+=1;continue
            if t<terminal and side*(op-a['active_stop'])<=0:close(i,k,op,'profit_lock','open')
        for s,oi in list(reservations.items()):
            o=orders[oi]
            if o['status']!='pending'or o['planned_ms']>t or t>terminal or(t-START)%CLOCK!=0:continue
            if t>=o['expiry_ms']:
                o.update(status='rejected',reject_reason='limit_expired',execution_ms=t,execution_quote=None,execution_equity=None,execution_margin=None,receipt_available_ms=t+CLOCK);attempts.append(dict(order=oi,time_ms=t,raw_open=None,slipped_open=None,action='limit_expired'));enqueue(t+CLOCK,'reject',order=oi);continue
            change=o.get('refresh');active_limit=change['new_limit'] if change and change['eligible'] and t>change['effect_ms'] else o['fixed_limit']
            px=float(raw[s]['open'][k]);eq,_,margin=actual_state(k,'open');fill=px*(1+o['side']*slip);fx=o['qty']*fill*fee
            reason='unprinted_execution'if float(raw[s]['quote_volume'][k])<=0 else'gap_outside_precommitted_bracket'if o['side']*(px-o['initial_stop'])<=0 or o['side']*(o['initial_target']-px)<=0 else'gap_exceeds_cap'if abs(px-o['reference'])>o['entry_cap_distance'] and not (o['side']*(px-o['reference'])<0 and o['side']*(fill-active_limit)<=0 and o['side']*(fill-o['initial_stop'])>0 and o['side']*(o['initial_target']-fill)>0)else None
            if reason is None and o['side']*(fill-active_limit)>0:attempts.append(dict(order=oi,time_ms=t,raw_open=px,slipped_open=fill,action='wait_limit'));continue
            if reason is None and o['family']=='weekly' and not(o['initial_stop']<fill<o['initial_target']):reason='slipped_fill_outside_bracket'
            if reason is None and(eq<=0 or margin+o['qty']*px/o['lev']>eq-fx):reason='actual_margin'
            attempts.append(dict(order=oi,time_ms=t,raw_open=px,slipped_open=fill,action=reason or'filled'));o.update(execution_ms=t,execution_quote=px,execution_equity=eq,execution_margin=margin,receipt_available_ms=t+MINUTE+CLOCK)
            if reason:o.update(status='rejected',reject_reason=reason);enqueue(t+MINUTE+CLOCK,'reject',order=oi);continue
            assert s not in actual;i=len(trades);a=dict(o,trade_id=i,entry_idx=k,entry_ms=t,entry_lower_ms=t,entry_upper_ms=t+MINUTE,entry_receipt_ms=t+MINUTE+CLOCK,raw_entry=px,entry_fill=fill,entry_fee=fx,funding=0.,exit_idx=None,exit_ms=None,exit_fill=None,exit_fee=0.,exit_type=None,status='open',unprinted_exposure_bars=0)
            a['active_stop']=a['initial_stop'];trades.append(a);actual[s]=i;observed.add(i);o.update(status='filled',trade=i);cash(-fx,k,'entry_fee',i);enqueue(t+MINUTE+CLOCK,'entry',-fx,i,oi)
        for s,i in list(actual.items()):
            a=trades[i]
            if float(raw[s]['quote_volume'][k])<=0:continue
            if t>=terminal:close(i,k,float(raw[s]['open'][k]),'window_end','open');continue
            lo,hi=float(raw[s]['low'][k]),float(raw[s]['high'][k])
            if(lo<=a['active_stop']if a['side']>0 else hi>=a['active_stop']):close(i,k,a['active_stop'],'profit_lock'if a['active_stop']!=a['initial_stop']else'stop','intrabar')
            elif(hi>=a['initial_target']if a['side']>0 else lo<=a['initial_target']):close(i,k,a['initial_target'],'target','intrabar')
        while fi<len(events)and events[fi][0]<t+MINUTE:
            ft,s,rate=events[fi];fi+=1;eligible=[i for i in observed if trades[i]['symbol']==s and trades[i]['entry_ms']<=ft and(trades[i]['exit_ms']is None or ft<=trades[i]['exit_upper_ms'])]
            if not eligible:continue
            j=int(np.searchsorted(marks[s]['open_time'],ft-2*CLOCK,side='right')-1);assert j>=0 and int(marks[s]['open_time'][j])+2*CLOCK<=ft;mark=float(marks[s]['close'][j])
            for i in eligible:
                a=trades[i];ambiguous=ft<a['entry_ms']+MINUTE or a['exit_idx']==k;signed=-a['side']*a['qty']*mark*rate;flow=min(0.,signed)if ambiguous else signed;a['funding']+=flow;cash(flow,k,'funding',i);enqueue(ft+CLOCK,'funding',flow,i)
                funding.append(dict(trade=i,event_ms=ft,rate=rate,mark=mark,reference_open_ms=int(marks[s]['open_time'][j]),reference_available_ms=int(marks[s]['open_time'][j])+2*CLOCK,ambiguous=ambiguous,nominal_flow=signed,flow=flow,upper_flow=max(0.,signed)if ambiguous else signed,booking_idx=k,available_ms=ft+CLOCK))
        eq,gross,_=actual_state(k,'close')
        if actual and eq<=.005*gross:
            if any(float(raw[s]['quote_volume'][k])<=0 for s in actual):counts['unprinted_maintenance_bars']+=1
            else:
                for s,i in list(actual.items()):close(i,k,float(raw[s]['close'][k]),'maintenance_proxy','close');counts['liquidations']+=1
        eq,_,_=actual_state(k,'close');curve.append((t+MINUTE,eq))
    for s,oi in reservations.items():
        if orders[oi]['status']=='pending':orders[oi].update(status='unexecuted_window_end',reject_reason='outside_window')
    for a in trades:
        assert a['status']=='closed','Unprinted terminal exposure remains unresolved'
        a['net_pnl']=a['qty']*a['side']*(a['exit_fill']-a['entry_fill'])-a['entry_fee']-a['exit_fee']+a['funding'];a['raw_pnl']=a['qty']*a['side']*(a['raw_exit']-a['raw_entry']);a['slippage']=a['raw_pnl']-a['qty']*a['side']*(a['exit_fill']-a['entry_fill']);a['hold_hours']=(a['exit_ms']-a['entry_ms'])/3600000
    cv=np.asarray(curve);values=np.r_[1000.,cv[:,1]];final=float(values[-1]);days=(end-START)/86400000
    return dict(risk_updates=risk_updates,trades=trades,orders=orders,attempts=attempts,requests=requests,funding=funding,cash_events=cash_events,receipts=receipts,curve=cv,counts=counts,summary=dict(final_usdt=final,cumulative_pct=100*(final/1000-1),cagr_pct=100*((final/1000)**(365.25/days)-1)if final>0 else-100.,max_drawdown_pct=float(100*np.max(1-values/np.maximum.accumulate(values))),trades=len(trades),frequency_per_2days=len(trades)*2/days,mean_hold_hours=float(np.mean([a['hold_hours']for a in trades]))if trades else 0.,max_hold_hours=max([a['hold_hours']for a in trades],default=0.),liquidations=counts['liquidations'],no_print_orders=sum(o['reject_reason']=='unprinted_execution'for o in orders),no_print_exposure=sum(a['unprinted_exposure_bars']for a in trades),cost=cost,days=days,start_ms=START,end_ms=end,variant='weekly_shared_capital',opening_attempts=len(attempts),wait_attempts=sum(x['action']=='wait_limit'for x in attempts),expired_orders=sum(o['reject_reason']=='limit_expired'for o in orders),mean_wait_minutes=float(np.mean([(a['entry_ms']-a['planned_ms'])/MINUTE for a in trades]))if trades else 0.,terminal_cutoff_ms=terminal,execution_interval_ms=MINUTE,strategy_clock_ms=CLOCK))
