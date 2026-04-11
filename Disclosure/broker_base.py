from dataclasses import dataclass
from typing import Protocol, Optional

@dataclass
class OrderResult:
    ok: bool
    msg: str
    raw: Optional[dict] = None

class BrokerClient(Protocol):
    def get_last_price(self, symbol: str) -> Optional[float]:
        ...

    def buy_market(self, symbol: str, qty: int) -> OrderResult:
        ...

    def sell_market(self, symbol: str, qty: int) -> OrderResult:
        ...
