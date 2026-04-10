from strategy.core import StrategyCore
from strategy.positions import PositionsMixin
from strategy.scanner import ScannerMixin
from strategy.entry import EntryMixin
from strategy.exit import ExitMixin
from strategy.tp_sl import TpSlMixin
from strategy.monitor import MonitorMixin


class AutoExchangeStrategy(
    StrategyCore,
    PositionsMixin,
    ScannerMixin,
    EntryMixin,
    ExitMixin,
    TpSlMixin,
    MonitorMixin,
):
    pass
