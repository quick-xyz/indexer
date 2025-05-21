from ....decode.model.block import DecodedLog
from ...events.base import DomainEvent, TransactionContext
from ...events.trade import Trade
from ...events.staking import Staking
from ....utils.logger import get_logger
from ...events.parameters import Parameters, Parameter

from ...events.auction import Auction, LotStarted, LotCancelled

class AuctionTransformer:
    def __init__(self, contract, base_token):
        self.logger = get_logger(__name__)
        self.contract = contract
        self.underlying_token = base_token

    def get_param_name(self, log: DecodedLog) -> str:
        param_map = {
            "DecayRateChanged": "decayRate",
            "MinPriceChanged": "minPrice",
            "MinPurchaseChanged": "minPurchase",
            "MaxPurchaseChanged": "maxPurchase",
            "PriceMultipleChanged": "priceMultiple",
            "StartPriceChanged": "startPrice",
            "TreasuryChanged": "treasury",
            "FeeCollectorChanged": "feeCollector",
            "SaleStatusChanged": "saleActive",
        }
        return param_map.get(log.name, "")

    def handle_address_change(self, log: DecodedLog, context: TransactionContext) -> list[Parameters]:
        name = self.get_param_name(log)
        
        status = Parameter(
            parameter=name,
            value_type="address",
            new_value=log.attributes.get("newAddress").lower(),
            old_value=log.attributes.get("oldAddress").lower(),
        )

        parameters = Parameters(
            contract=log.contract,
            parameters=[status]
        )

        return [parameters]

    def handle_param_change(self, log: DecodedLog, context: TransactionContext) -> list[Parameters]:
        name = self.get_param_name(log)
        
        status = Parameter(
            parameter=name,
            value_type="int",
            new_value=int(log.attributes.get("newValue")),
            old_value=int(log.attributes.get("oldValue")),
        )

        parameters = Parameters(
            contract=log.contract,
            parameters=[status]
        )

        return [parameters]
    
    def handle_purchase(self, log: DecodedLog, context: TransactionContext) -> list[Trade]:
        auction = Auction(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            lot=log.attributes.get("lotNumber"),
            buyer=log.attributes.get("buyer").lower(),
            amount_base=int(log.attributes.get("tokensBought")),
            amount_quote=int(log.attributes.get("amountPaid")),
            price_avax=int(log.attributes.get("currentPrice")),
        )

        trade = Trade(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            taker=log.attributes.get("buyer").lower(),
            direction="buy",
            base_token=self.underlying_token,
            base_amount=int(log.attributes.get("tokensBought")),
            quote_token=self.contract,
            quote_amount=int(log.attributes.get("amountPaid")),
            trade_type="auction",
            swaps=[auction]
        )

        return [trade]
    
    def handle_lot_start(self, log: DecodedLog, context: TransactionContext) -> list[LotStarted]:
        lot = LotStarted(
            lot=int(log.attributes.get("lotNumber")),
            start_price=int(log.attributes.get("startPrice")),
            start_time=log.attributes.get("timestamp"),
        )

        return [lot]
    
    def handle_lot_cancel(self, log: DecodedLog, context: TransactionContext) -> list[LotCancelled]:
        lot = LotCancelled(
            lot=log.attributes.get("lotNumber"),
            end_price=float(log.attributes.get("currentPrice")),
            end_time=context.timestamp,
        )

        return [lot]
    
    def handle_auction_status(self, log: DecodedLog, context: TransactionContext) -> list[Parameters]:
        name = self.get_param_name(log)

        status = Parameter(
            parameter=name,
            value_type="bool",
            new_value=bool(log.attributes.get("isActive"))
        )

        parameters = Parameters(
            contract=log.contract,
            parameters=[status]
        )

        return [parameters]   

    def transform_log(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        events = []
        if log.name == "TokensPurchased":
            events.append(self.handle_purchase(log, context))  
        elif log.name == "NewLotStarted":
            events.append(self.handle_lot_start(log, context))   
        elif log.name == "LotCancelled":
            events.append(self.handle_lot_cancel(log, context))
        elif log.name == "SaleStatusChanged":
            events.append(self.handle_auction_status(log, context))  
        elif log.name in (
            "TreasuryChanged",
            "FeeCollectorChanged"
        ):
            events.append(self.handle_address_change(log, context))  
        elif log.name in (
            "DecayRateChanged",
            "MinPriceChanged",
            "MinPurchaseChanged",
            "MaxPurchaseChanged",
            "PriceMultipleChanged",
            "StartPriceChanged"
        ):
            events.append(self.handle_param_change(log, context))  

        return events

