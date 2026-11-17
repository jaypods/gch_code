"""Run the unchanged three strategy books against one current saved state.

No release checks, audit replay, watchdog, website wait, or final-backtest gate.
The engine produces durable intents only. The user-operated broker handles
exchange effects separately; a failed strategy minute publishes no intents.
"""
from decimal import Decimal as D

from .account import InvalidState
from .combined_controller import CombinedController, MINUTE
from .simple_orders import normalize_action, stamp
from .state_codec import encode, decode, validate_time
from .strategies.main import MainReceipt
from .strategies.alt import AltReceipt
from .strategies.stock import StockReceipt


def initial_allocation(snapshot):
    """Allocate currently spendable funds once, without adopting manual lots."""
    if snapshot.multi_asset_mode:
        raise InvalidState('此组合使用 USDT 单币种保证金')
    total = snapshot.free_capital()
    if total <= 0:
        raise InvalidState('账户当前没有可用保证金')
    share = total / D(3)
    return dict(M784=share, A787=share, S800=total - 2 * share)


class SimpleEngine:
    def __init__(self, store):
        self.store = store
        self.controller = None
        self.revision = 0
        self.payload = None
        saved = store.state()
        if saved is not None:
            self._load(saved)

    def _load(self, saved):
        controller = decode(saved['payload'], profile='live')
        if not isinstance(controller, CombinedController) or controller.halt_reason or controller.last_clock != saved['clock_ms']:
            raise InvalidState('最近保存的策略状态不完整')
        self.controller = controller
        self.revision = saved['revision']; self.payload = saved['payload']

    def initialize(self, controller, *, now_ms):
        stamp(now_ms)
        if self.store.state() is not None or self.controller is not None:
            raise InvalidState('已保存的实际策略状态不能重新分配本金')
        if not isinstance(controller, CombinedController) or controller.cycles or controller.halt_reason:
            raise InvalidState('首次启动需要尚未执行交易的原组合策略')
        if not controller.start_ms - MINUTE <= now_ms < controller.start_ms:
            raise InvalidState('首次启动应在目标分钟之前保存预热状态')
        validate_time(controller, clock_ms=controller.last_clock, received_ms=now_ms)
        payload = encode(controller, profile='live')
        revision = self.store.save_state(payload, clock_ms=controller.last_clock, decision_ms=now_ms, expected_revision=0)
        self.controller = controller; self.payload = payload; self.revision = revision
        return revision

    def advance(self, packet, *, now_ms):
        """Consume available actual receipts and commit this minute once.

        The original books require consecutive observations. If the next clock
        has been missed, this method never places an old signal into the current
        minute. A separate input/resume path must supply the current state.
        """
        stamp(now_ms)
        if self.controller is None:
            raise InvalidState('原策略尚未初始化')
        frame = self.controller.last_clock + MINUTE
        if not frame <= now_ms < frame + MINUTE:
            raise InvalidState('当前时点不属于下一策略分钟，不能补发过去的交易')
        receipts = self.store.pending_receipts(now_ms=now_ms)
        classes = dict(M784=MainReceipt, A787=AltReceipt, S800=StockReceipt)
        try:
            for item in receipts:
                r = classes[item['sleeve']](**item['receipt'])
                self.controller.receive(item['sleeve'], r, received_ms=r.received_ms)
            result = self.controller.advance(frame, decision_ms=now_ms, **packet)
            actions = []
            for sleeve, rows in result['strategy_actions'].items():
                for action in rows:
                    first = None
                    if sleeve == 'S800' and action.get('kind') == 'EXIT_REQUEST' and action.get('reason') == 'deadline':
                        first = self.controller.stock.book.orders[action['order_id']]['first_ms']
                    actions.append(normalize_action(sleeve, action, now_ms=now_ms, first_entry_ms=first))
            payload = encode(self.controller, profile='live')
            revision = self.store.save_state(payload, clock_ms=frame, decision_ms=now_ms,
                expected_revision=self.revision, actions=actions, consumed_receipts=[r['id'] for r in receipts])
        except BaseException:
            # Re-read one committed snapshot. Never replay an audit chain or
            # keep an in-memory graph partially changed by a failed minute.
            saved = self.store.state()
            if saved is not None: self._load(saved)
            raise
        self.payload = payload; self.revision = revision
        return dict(clock_ms=frame, decision_ms=now_ms, actions=actions,
                    consumed_receipts=len(receipts), revision=revision)

    def resume(self, sources, packet_builder, *, frame_ms, now_ms,
               perpetual_bars=(), spot_bars=(), weekly_closes=()):
        """Commit one CURRENT decision after source-only missed observations.

        This never executes the missed entry decisions. The original cash,
        orders, outcome history and opportunity-model state remain intact.
        Current state + actual consumed receipts + current intents commit once.
        """
        from .outage_receipts import stage_funded_receipts
        from .outage_state import compose_outage_state
        if self.controller is None or not callable(packet_builder):
            raise InvalidState('续接需要原策略状态和当前行情输入')
        stamp(now_ms);stamp(frame_ms)
        old=self.controller
        if old.cycles<1 or not frame_ms<=now_ms<frame_ms+MINUTE or frame_ms<old.last_clock+2*MINUTE:
            raise InvalidState('续接必须对应当前分钟，不能补发历史信号')
        pending=self.store.pending_receipts(now_ms=now_ms)
        pending.sort(key=lambda item:(item['receipt']['received_ms'],item['receipt']['available_ms'],item['id']))
        classes=dict(M784=MainReceipt,A787=AltReceipt,S800=StockReceipt)
        rows=[dict(seq=i+1,event_id=item['id'],kind='RECEIPT',received_ms=item['receipt']['received_ms'],
                   payload=dict(sleeve=item['sleeve'],receipt=classes[item['sleeve']](**item['receipt'])))
              for i,item in enumerate(pending)]
        funded,_=stage_funded_receipts(old,rows,input_after=0,known_ms=now_ms)
        prior_actions=funded.stock.book.actions[len(old.stock.book.actions):]
        current,_=compose_outage_state(old,funded,sources,resume_ms=frame_ms,known_ms=now_ms,
            perpetual_bars=perpetual_bars,spot_bars=spot_bars,weekly_closes=weekly_closes)
        packet=packet_builder(current)
        result=current.advance(frame_ms,decision_ms=now_ms,**packet)
        result['strategy_actions']['S800'] = list(prior_actions)+result['strategy_actions']['S800']
        actions=[]
        for sleeve,values in result['strategy_actions'].items():
            for raw in values:
                action=dict(raw,known_ms=now_ms,cycle_ms=frame_ms)
                first=None
                if sleeve=='S800' and action.get('kind')=='EXIT_REQUEST' and action.get('reason')=='deadline':
                    first=current.stock.book.orders[action['order_id']]['first_ms']
                actions.append(normalize_action(sleeve,action,now_ms=now_ms,first_entry_ms=first))
        payload=encode(current,profile='live')
        revision=self.store.save_state(payload,clock_ms=frame_ms,decision_ms=now_ms,
            expected_revision=self.revision,actions=actions,consumed_receipts=[r['id'] for r in pending])
        self.controller=current;self.payload=payload;self.revision=revision
        return dict(clock_ms=frame_ms,decision_ms=now_ms,actions=actions,revision=revision,
            consumed_receipts=len(pending),missed_minutes=(frame_ms-old.last_clock)//MINUTE-1)

    def replace_unstarted(self, controller, *, now_ms):
        """Move only a never-executed bootstrap when its first minute elapsed."""
        old=self.controller
        if (old is None or old.cycles or controller.cycles or old.halt_reason or controller.halt_reason
                or self.store.orders(statuses=('PENDING','SENDING','UNKNOWN','ACKNOWLEDGED','FINISHED','REJECTED'))
                or self.store.fills() or not controller.start_ms-MINUTE<=now_ms<controller.start_ms):
            raise InvalidState('已有交易或决策的策略不能重新初始化')
        old_initial=(old.main.funded.initial,old.alt.initial,old.stock.book.initial)
        new_initial=(controller.main.funded.initial,controller.alt.initial,controller.stock.book.initial)
        if old_initial!=new_initial:
            raise InvalidState('尚未开始的策略也不能重设已分配本金')
        validate_time(controller,clock_ms=controller.last_clock,received_ms=now_ms)
        payload=encode(controller,profile='live')
        revision=self.store.save_state(payload,clock_ms=controller.last_clock,decision_ms=now_ms,expected_revision=self.revision)
        self.controller=controller;self.payload=payload;self.revision=revision
        return revision
