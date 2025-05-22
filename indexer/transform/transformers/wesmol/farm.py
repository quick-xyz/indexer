from ....decode.model.block import DecodedLog
from ...events.base import DomainEvent, TransactionContext
from ...events.trade import Trade
from ...events.transfer import Transfer
from ...events.staking import Staking
from ....utils.logger import get_logger
from ...events.parameters import Parameters, Parameter
from ...events.farm_ops import (
    FarmAdd, 
    FarmSet,
    FarmDeposit,
    FarmWithdraw,
    UpdateFarm,
    FarmHarvest,
    FarmBatchHarvest,
    EmergencyWithdraw,
    FarmSkim
)


class FarmTransformer:
    def __init__(self, contract, base_token):
        self.logger = get_logger(__name__)
        self.contract = contract.lower()

    def event_add(self, log: DecodedLog, context: TransactionContext) -> list[FarmAdd]:
        add = FarmAdd(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            contract=log.contract.lower(),
            farm_id=int(log.attributes.get("pid")),
            reward_rate=int(log.attributes.get("allocPoint")),
            deposit_token=log.attributes.get("apToken").lower(),
            rewarder_address=log.attributes.get("rewarder").lower(),
        )
        return [add]

    def event_set(self, log: DecodedLog, context: TransactionContext) -> list[FarmSet]:
        set = FarmSet(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            contract=log.contract.lower(),
            farm_id=int(log.attributes.get("pid")),
            reward_rate=int(log.attributes.get("allocPoint")),
            rewarder_address=log.attributes.get("rewarder").lower(),
            overwrite=bool(log.attributes.get("overwrite")),
        )
        return [set]

    def event_deposit(self, log: DecodedLog, context: TransactionContext) -> list[FarmDeposit]:
        deposit = FarmDeposit(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            contract=log.contract.lower(),
            farm_id=int(log.attributes.get("pid")),
            staker=log.attributes.get("user").lower(),
            amount=int(log.attributes.get("amount"))
        )
        return [deposit]

    def event_withdraw(self, log: DecodedLog, context: TransactionContext) -> list[FarmWithdraw]:
        withdrawal = FarmWithdraw(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            contract=log.contract.lower(),
            farm_id=int(log.attributes.get("pid")),
            staker=log.attributes.get("user").lower(),
            amount=int(log.attributes.get("amount"))
        )
        return [withdrawal]
    
    def event_update_farm(self, log: DecodedLog, context: TransactionContext) -> list[UpdateFarm]:
        update = UpdateFarm(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            contract=log.contract.lower(),
            farm_id=int(log.attributes.get("pid")),
            last_reward_timestamp=int(log.attributes.get("lastRewardTimestamp")),
            deposit_balance=int(log.attributes.get("lpSupply")),
            acc_reward_per_share=int(log.attributes.get("accWeSmolPerShare"))
        )
        return [update]
    
    def event_harvest(self, log: DecodedLog, context: TransactionContext) -> list[FarmHarvest]:
        harvest = FarmHarvest(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            contract=log.contract.lower(),
            farm_id=int(log.attributes.get("pid")),
            staker=log.attributes.get("user").lower(),
            amount_received=int(log.attributes.get("amount")),
            amount_owed=int(log.attributes.get("unpaidAmount"))
        )
        return [harvest]
    
    def event_batch_harvest(self, log: DecodedLog, context: TransactionContext) -> list[FarmBatchHarvest]:
        batch_harvest = FarmBatchHarvest(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            contract=log.contract.lower(),
            farm_ids=[int(pid) for pid in log.attributes.get("pids")]
        )
        return [batch_harvest]

    def event_emergency_wd(self, log: DecodedLog, context: TransactionContext) -> list[EmergencyWithdraw]:
        emergency_withdraw = EmergencyWithdraw(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            contract=log.contract.lower(),
            farm_id=int(log.attributes.get("pid")),
            staker=log.attributes.get("user").lower(),
            amount=int(log.attributes.get("amount"))
        )
        return [emergency_withdraw]

    def event_skim(self, log: DecodedLog, context: TransactionContext) -> list[FarmSkim]:
        skim = FarmSkim(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            contract=log.contract.lower(),
            token=log.attributes.get("token").lower(),
            to=log.attributes.get("to").lower(),
            amount=int(log.attributes.get("amount"))
        )
        return [skim]

    def _get_transfer(self, tx_events: list[DomainEvent], target: TransactionContext):
        for event in tx_events:
            if isinstance(event, Transfer) and event.to_address == self.contract:
                return event
        return None



    


    def build_staking(self, tx_events: list[DomainEvent], context: TransactionContext) -> list[Staking]:
        for event in tx_events:
            if isinstance(event, Transfer) and event.to_address == self.contract:
                return event

        if deposit:
            staking = Staking(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                contract=self.contract,
                staker=event.from_address,
                token=self.contract,
                amount=event.amount,
                event_tag="deposit",
                receipt_token=self.contract,
                amount_receipt=event.amount,
                transfers=[event]
            )
        
        return [staking]



    def build_claim(self, log: DecodedLog, context: TransactionContext) -> list[Trade]:





    def transform_log(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        events = []
        if log.name == "Add":
            events.append(self.event_add(log, context))
        elif log.name == "Set":
            events.append(self.event_set(log, context))
        elif log.name == "Deposit":
            events.append(self.event_deposit(log, context))
        elif log.name == "Withdraw":
            events.append(self.event_withdraw(log, context))
        elif log.name == "UpdateFarm":
            events.append(self.event_update_farm(log, context))
        elif log.name == "Harvest":
            events.append(self.event_harvest(log, context))
        elif log.name == "BatchHarvest":
            events.append(self.event_batch_harvest(log, context))
        elif log.name == "EmergencyWithdraw":
            events.append(self.event_emergency_wd(log, context))
        elif log.name == "Skim":
            events.append(self.event_skim(log, context))

        return events
    
    def transform_tx_events(self, tx_events: list[DomainEvent], context: TransactionContext) -> list[DomainEvent]:
        event_processors = {
            "FarmAdd": self.event_add,
            "FarmSet": self.event_set,
            "FarmDeposit": self.event_deposit,
            "FarmWithdraw": self.event_withdraw,
            "UpdateFarm": self.event_update_farm,
            "FarmHarvest": self.event_harvest,
            "FarmBatchHarvest": self.event_batch_harvest,
            "EmergencyWithdraw": self.event_emergency_wd,
            "FarmSkim": self.event_skim
        }

        remaining_events = tx_events.copy()
        new_events = []
        processed_indices = set()
        
        for i, event in enumerate(tx_events):
            if i in processed_indices:
                continue

            processor = event_processors.get(type(event))
            if processor:
                # Find related events and process them
                related_events, indices_to_remove = self._find_related_events(
                    event, remaining_events, i
                )
                
                if related_events:  # Only process if we found what we need
                    created_events = processor(related_events, context)
                    new_events.extend(created_events)
                    processed_indices.update(indices_to_remove)

        # Build final list: unprocessed events + new events
        final_events = [
            event for i, event in enumerate(tx_events) 
            if i not in processed_indices
        ]
        final_events.extend(new_events)
        
        return final_events
    
    def _find_related_events(self, trigger_event: DomainEvent, all_events: list[DomainEvent], 
                            trigger_index: int) -> Tuple[List[DomainEvent], Set[int]]:
        """Find events related to the trigger event."""
        related_events = [trigger_event]
        indices_to_remove = {trigger_index}
        
        # Example: if we have FarmDeposit, look for related FarmHarvest events
        if isinstance(trigger_event, FarmDeposit):
            for i, event in enumerate(all_events):
                if i != trigger_index and isinstance(event, FarmHarvest):
                    if self._events_are_related(trigger_event, event):
                        related_events.append(event)
                        indices_to_remove.add(i)
        
        return related_events, indices_to_remove

    def _events_are_related(self, event1: DomainEvent, event2: DomainEvent) -> bool:
        """Determine if two events are related (same farm, user, etc.)"""
        # Implement your business logic here
        return (hasattr(event1, 'farm_id') and hasattr(event2, 'farm_id') and
                event1.farm_id == event2.farm_id)

    def _process_farm_deposit_group(self, events: List[DomainEvent], 
                                context: TransactionContext) -> List[DomainEvent]:
        """Process a group of related farm events."""
        # Your business logic here
        # Example: combine deposit + harvest into a compound event
        deposit_events = [e for e in events if isinstance(e, FarmDeposit)]
        harvest_events = [e for e in events if isinstance(e, FarmHarvest)]
        
        return [FarmCompoundEvent(deposits=deposit_events, harvests=harvest_events)]