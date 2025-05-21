from typing import List

from ....decode.model.block import DecodedLog
from ...events.base import DomainEvent, TransactionContext
from ...events.transfer import Transfer
from ...events.staking import Staking
from ....utils.logger import get_logger
from ...events.parameters import Parameters, Parameter

ZERO_ADDRESS = "0x" + "0" * 40

class WesmolWrapperTransformer:
    def __init__(self, contract, base_token):
        self.logger = get_logger(__name__)
        self.contract = contract
        self.underlying_token = base_token

    def handle_transfer(self, log: DecodedLog, context: TransactionContext) -> List[Transfer]:
        transfers = []
        staking = []

        transfer = Transfer(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            token=log.contract,
            amount=log.attributes.get("value"),
            from_address=log.attributes.get("from"),
            to_address=log.attributes.get("to"),
        )
        transfers.append(transfer)

        if log.attributes.get("from") == ZERO_ADDRESS:
            mint = Staking(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                contract=log.contract,
                staker=log.attributes.get("to"),
                token=self.underlying_token,
                amount=log.attributes.get("value"),
                event_tag="deposit",
                receipt_token=log.contract,
                amount_receipt=log.attributes.get("value"),
                transfers= transfers
            )
            staking.append(mint)

        if log.attributes.get("to") == ZERO_ADDRESS:
            burn = Staking(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                contract=log.contract,
                staker=log.attributes.get("from"),
                token=self.underlying_token,
                amount=log.attributes.get("value"),
                event_tag="withdraw",
                receipt_token=log.contract,
                amount_receipt=log.attributes.get("value"),
                transfers= transfers  
            )
            staking.append(burn)

        return staking

    def handle_deposit_status(self, log: DecodedLog, context: TransactionContext) -> Staking:
        
        deposits = []
        parameters = []

        status = Parameter(
            parameter="depositStatus",
            value_type="bool",
            new_value=log.attributes.get("enabled"),
        )
        deposits.append(status)

        parameter = Parameters(
            contract=log.contract,
            parameters=deposits
        )
        parameters.append(parameter)

        return parameters

    def handle_withdrawal_status(self, log: DecodedLog, context: TransactionContext) -> Staking:
        
        withdrawals = []
        parameters = []

        status = Parameter(
            parameter="withdrawalStatus",
            value_type="bool",
            new_value=log.attributes.get("enabled"),
        )
        withdrawals.append(status)

        parameter = Parameters(
            contract=log.contract,
            parameters=withdrawals
        )
        parameters.append(parameter)

        return parameters
    
    def transform_log(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        events = []
        if log.name == "Transfer":
            events.append(self.handle_transfer(log, context))
        elif log.name == "DepositStatusChanged":
            events.append(self.handle_deposit_status(log, context))
        elif log.name == "WithdrawalStatusChanged":
            events.append(self.handle_withdrawal_status(log, context))    

        return events

