"""M784's fixed opportunity model, part of the signal, never funded execution.

Unlike a funded paper venue, this shadow model is needed in both historical and
live decision engines. It is allowed in a live checkpoint only with mode=shadow.
Observed completed public minutes determine its synthetic occupancy/cooldowns.
"""
from .account import InvalidState
from .main_paper import MainPaperVenue
from .strategies.main import MainBook,MINUTE,SYMS


class MainShadowModel(MainPaperVenue):
    def __init__(self,book):
        if not isinstance(book,MainBook) or book.mode!='shadow' or book.initial!=1000.:raise InvalidState('机会模型只能绑定固定 1000 单位的影子账户')
        super().__init__(book)

    def observe_closed_minute(self,t,bars,*,received_ms,funding=()):
        if self.book.mode!='shadow' or self.book.initial!=1000.:raise InvalidState('影子模型不得接入真实资金策略账户')
        if type(received_ms) is not int or received_ms<t+MINUTE or set(bars)!=set(SYMS):raise InvalidState('影子模型分钟尚未完整收盘或缺币种')
        if any(b.received_ms>received_ms for b in bars.values()):raise InvalidState('影子模型不能查看尚未收到的分钟行情')
        for event in funding:
            if type(event.get('received_ms')) is not int or not event['time_ms']<=event['received_ms']<=received_ms:
                raise InvalidState('影子模型资金费尚未在当前决策前收到')
        return super().minute(t,bars,funding=funding,received_ms=received_ms)
