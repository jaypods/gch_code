"""A787 admission sizing on receipt-known state. No execution or source search.

The caller supplies the complete simultaneous-source population before context
rejections, and a snapshot rebuilt from received fills. This object owns event
anchors and permanent commitments; cancellation does not refund a commitment.
Initial notional caps scale with the actual initial sleeve allocation once.
"""
from copy import deepcopy
import math
from ..account import InvalidState
from ..market import Bar

STEP=300000


def freeze_price(source,cost):
    fee,slip=.001*cost,.002*cost
    reference,stop=float(source['reference_price']),float(source['stop']);distance=reference-stop
    if not all(math.isfinite(x) and x>0 for x in (reference,stop)) or distance<=0:raise InvalidState('山寨来源风险距离无效')
    raw=reference+.5*distance;fill=raw*(1+slip);exit_fill=stop*(1-slip)
    loss=fill-exit_fill+fee*(fill+exit_fill)
    leverage=max((x for x in (2,3,5,10) if loss/fill+.02<=1/x),default=0)
    return dict(signal_reference=reference,initial_R=distance,raw_price_cap=raw,fill_price_cap=fill,stop_fill_assumption=exit_fill,
                worst_stop_loss_per_unit=loss,leverage=leverage,margin_buffer_fraction=.02,entry_fee_per_unit=fill*fee,
                planned_signal_end_ms=source['signal_end_ms'])


def adaptive(cycles,equity,peak):
    # Preserve the original same-time ordering of (exit upper bound, net PnL).
    closed=sorted((int(p.get('exit_interval_end_ms',p.get('exit_ms',0))),p['realized']-p['fees']+p['funding']) for p in cycles.values() if p.get('closed'))
    streak=0
    for _,net in reversed(closed):
        if net>=0:break
        streak+=1
    dd=max(0.,1-equity/max(peak,equity,1e-9))
    factor=.5 if dd>=.5 else .65 if dd>=.4 else .8 if dd>=.3 else .9 if dd>=.2 else 1.
    if streak>=8:factor=min(factor,.7)
    elif streak>=5:factor=min(factor,.85)
    return dict(drawdown=dd,loss_streak=streak,recent_mean=None,closed_count=len(closed),multiplier=factor,reason='mild')


class AltRisk:
    def __init__(self,initial,*,cost=1):
        if not math.isfinite(initial) or initial<=0 or cost not in (1,2):raise InvalidState('山寨账户初始资金或成本错误')
        self.initial=float(initial);self.cash_peak=float(initial);self.cost=cost;self.events={};self.commit={};self.batches={};self.populations={};self.decisions={}

    def _event(self,source,snapshot,risk,now):
        event_id=source['leadership_event_hour'];equity=snapshot['equity']
        if event_id not in self.events:self.events[event_id]=dict(anchor_equity=equity,anchor_ms=now,budget=2.3*risk*equity)
        anchor=self.events[event_id];closed=[];opened=[]
        for identity,p in snapshot['cycles'].items():
            if p['leadership_event_hour']!=event_id:continue
            net=p['realized']-p['fees']+p['funding']
            if p['closed']:
                loss=max(0.,-net)
                if loss:closed.append(dict(id=identity,loss=loss,known_ms=p['exit_interval_end_ms']))
            else:
                exit_fill=p['stop']*(1-p['slip']*2)
                projected=net+p['remaining']*(exit_fill-p['entry_fill']-exit_fill*p['fee_estimate']*2)
                opened.append(dict(id=identity,reserved_loss=max(0.,-projected)))
        spent=sum(x['loss'] for x in closed);reserved=sum(x['reserved_loss'] for x in opened)
        return dict(event_anchor_equity=anchor['anchor_equity'],event_anchor_ms=anchor['anchor_ms'],event_budget=anchor['budget'],
                    event_closed_loss=spent,event_open_reserved_loss=reserved,event_remaining_before_new=max(0.,anchor['budget']-spent-reserved),
                    event_closed_losses=closed,event_open_reserves=opened)

    def consider(self,source,snapshot,quote:Bar,*,now_ms,cash_peak,simultaneous_ids=(),lifecycle=None):
        if now_ms%STEP or source['decision_ms']!=now_ms or source['signal_end_ms']!=now_ms-STEP or source['intended_entry_ms']!=now_ms+STEP:
            raise InvalidState('山寨信号决策时序错误')
        if source.get('input_available_ms',now_ms)>now_ms or snapshot['time_ms']!=now_ms:raise InvalidState('山寨信号或账户观察尚未收到')
        if quote.symbol!=source['symbol'] or quote.period_ms!=STEP or quote.end_ms!=now_ms-STEP or quote.received_ms>now_ms:
            raise InvalidState('山寨仓位必须使用延迟后的已收盘成交量')
        if not math.isfinite(cash_peak) or cash_peak<self.cash_peak:raise InvalidState('山寨已收款现金峰值无效或倒退')
        if len(set(simultaneous_ids))!=len(simultaneous_ids):raise InvalidState('同批来源重复')
        if simultaneous_ids and source['id'] not in simultaneous_ids and not source.get('is_add'):raise InvalidState('来源不在事先完整确定的批次内')
        if source['id'] in self.decisions:raise InvalidState('山寨来源不可重复消耗事件额度')
        for p in snapshot['cycles'].values():
            if p.get('entry_received_ms',0)>now_ms or p.get('exit_received_ms',0)>now_ms or p.get('closed') and p.get('exit_interval_end_ms',0)>now_ms:
                raise InvalidState('山寨账户包含未来成交或回报')
        prior=deepcopy(self.__dict__)
        try:
            members=tuple(simultaneous_ids) or (source['id'],)
            if not source.get('is_add'):
                old=self.populations.setdefault(now_ms,members)
                if old!=members:raise InvalidState('同批来源名单在处理过程中发生变化')
            out=self._consider(source,snapshot,float(quote.quote_volume),now_ms,float(cash_peak),members,lifecycle)
            self.cash_peak=float(cash_peak)
            self.decisions[source['id']]=deepcopy(out)
            if out['reason'] is None:
                identity=source['leadership_event_hour']
                self.commit[identity]=self.commit.get(identity,0.)+out['quantity']*out['plan']['worst_stop_loss_per_unit']
            return out
        except BaseException:
            self.__dict__.clear();self.__dict__.update(prior);raise

    def _consider(self,s,snap,quote,now,peak,members,calendar):
        out={'reason':None};is_child=bool(s.get('is_add'))
        if calendar and now>=calendar['modeled_schedule_known_ms'] and now>=calendar['no_new_position_ms']:
            return dict(reason='announced_no_new_position_before_reservation')
        plan=freeze_price(s,self.cost);out['plan']=plan;pos=snap['positions']
        if len(pos)>=6:out['reason']='slot_limit' if is_child else 'slot_or_symbol_occupied';return out
        if any(p['symbol']==s['symbol'] and (not is_child or p['id']!=s['parent_id']) for p in pos):
            out['reason']='other_known_same_symbol_lot' if is_child else 'slot_or_symbol_occupied';return out
        if not plan['leverage']:out['reason']='stop_exceeds_2x_with_2pct_buffer';return out
        eq=snap['equity']
        if not math.isfinite(eq) or eq<=0:out['reason']='nonpositive_equity';return out
        fee,slip=.001*self.cost,.002*self.cost
        adapt=adaptive(snap['cycles'],eq,snap['peak']);mult=adapt['multiplier'];risk=(.2 if s['momentum_rank']<=2 else .2*.666)*mult
        marks=snap['marks'];price=plan['fill_price_cap'];lev=plan['leverage'];loss=plan['worst_stop_loss_per_unit'];fu=plan['entry_fee_per_unit']
        margin=sum(p['remaining']*max(marks[p['symbol']],p.get('fill_price_cap',0))/p['leverage'] for p in pos)
        gross=sum(p['remaining']*max(marks[p['symbol']],p.get('fill_price_cap',0)) for p in pos)
        def stoprisk(p):return p['remaining']*max(0,marks[p['symbol']]-p['stop']*(1-slip)+fee*p['stop']*(1-slip))
        opened=sum(stoprisk(p) for p in pos);same=sum(stoprisk(p) for p in pos if p['leadership_event_hour']==s['leadership_event_hour'])
        event=self._event(s,snap,risk,now);left=max(0.,event['event_budget']-self.commit.get(s['leadership_event_hour'],0.))
        caps=dict(stop_budget=risk*eq/(loss+risk*fu),single_margin=.25*eq/(price/lev+.25*fu),
                  total_margin=max(0.,(.75*eq-margin)/(price/lev+.75*fu)),gross=max(0.,(7.5*eq-gross)/(price+7.5*fu)),
                  open_stop_risk=max(0.,(.10*eq-opened)/(loss+.10*fu)),same_event_open_risk=max(0.,(risk*eq-same)/(loss+risk*fu)),
                  previous_quote_participation=quote*.01/price,event_cumulative_loss=event['event_remaining_before_new']/loss,planned_event_commitment=left/loss)
        if is_child:
            same_margin=sum(p['remaining']*max(marks[p['symbol']],p.get('fill_price_cap',0))/p['leverage'] for p in pos if p['symbol']==s['symbol'])
            caps['single_margin']=max(0.,(.25*eq-same_margin)/(price/lev+.25*fu))
            caps['parent_quantity']=s['original_parent_quantity']
        elif len(members)>=2:
            if now not in self.batches:self.batches[now]={'members':members,'budget':max(0.,.10*eq-opened)/len(members)}
            if self.batches[now]['members']!=members:raise InvalidState('同批风险分母在处理过程中发生变化')
            caps['simultaneous_risk_share']=self.batches[now]['budget']/(loss+.10*fu)
        if s['leadership_event_hour']<0:caps['initial_capital_notional']=self.initial/price
        base=min(caps.values());caps['effective_quantity_overlay']=base*mult
        reserve=.5*max(0.,peak-self.initial);ratio=max(0.,eq-reserve)/eq
        caps['retained_cash_quantity']=min(caps.values())*ratio
        q=min(caps.values())
        if not all(math.isfinite(x) and x>=0 for x in caps.values()):raise InvalidState('山寨数量上限包含无效数值')
        out.update(size_limits=caps,binding_limit=min(caps,key=caps.get),quantity=q,adaptive=adapt,event=event,effective_risk_fraction=risk,
                   quantity_overlay=dict(base_quantity=base,known_multiplier=mult,after_adaptive_quantity=base*mult,known_cash_peak=peak,
                       retained_profit=reserve,known_equity=eq,retention_ratio=ratio,final_ceiling=caps['retained_cash_quantity']))
        if q*price<20:out['reason']='event_commitment_exhausted' if left<=1e-9 else 'below_research_minimum'
        return out
