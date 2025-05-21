from ....decode.model.block import DecodedLog
from ...events.base import DomainEvent, TransactionContext
from ...events.trade import Trade
from ...events.staking import Staking
from ....utils.logger import get_logger
from ...events.parameters import Parameters, Parameter
from ...events.farm_ops import Farm



class FarmTransformer:
    def __init__(self, contract, base_token):
        self.logger = get_logger(__name__)
        self.contract = contract
        self.reward_token = base_token

    def handle_add_rewarder(self, log: DecodedLog, context: TransactionContext) -> list[Parameters]:
        farm = Farm(
            contract=log.contract,
            farm_id=int(log.attributes.get("pid")),
            deposit_token=log.attributes.get("apToken").lower(),
            reward_token=self.reward_token,
            reward_rate=log.attributes.get("rewardRate").lower(),
        )

    event Add(uint256 indexed pid, uint256 allocPoint, IERC20 indexed apToken, IRewarder indexed rewarder);

    def handle_set_reward_rate(self, log: DecodedLog, context: TransactionContext) -> list[Parameters]:
        pass
    def handle_deposit(self, log: DecodedLog, context: TransactionContext) -> list[Staking]:
        pass
    def handle_withdraw(self, log: DecodedLog, context: TransactionContext) -> list[Staking]:
        pass
    def handle_update_farm(self, log: DecodedLog, context: TransactionContext) -> list[Parameters]:
        pass
    def handle_harvest(self, log: DecodedLog, context: TransactionContext) -> list[Trade]:
        pass
    def handle_emergency_wd(self, log: DecodedLog, context: TransactionContext) -> list[Staking]:
        pass
    def handle_skim(self, log: DecodedLog, context: TransactionContext) -> list[Parameters]:
        pass

    def transform_log(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        events = []
        if log.name == "Add":
            events.append(self.handle_add_rewarder(log, context))
        elif log.name == "Set":
            events.append(self.handle_set_reward_rate(log, context))
        elif log.name == "Deposit":
            events.append(self.handle_deposit(log, context))
        elif log.name == "Withdraw":
            events.append(self.handle_withdraw(log, context))
        elif log.name == "UpdateFarm":
            events.append(self.handle_update_farm(log, context))
        elif log.name in ("Harvest","BatchHarvest"):
            events.append(self.handle_harvest(log, context))
        elif log.name == "EmergencyWithdraw":
            events.append(self.handle_emergency_wd(log, context))
        elif log.name == "Skim":
            events.append(self.handle_skim(log, context))

        return events

    event Add(uint256 indexed pid, uint256 allocPoint, IERC20 indexed apToken, IRewarder indexed rewarder);
    event Set(uint256 indexed pid, uint256 allocPoint, IRewarder indexed rewarder, bool overwrite);
    event Deposit(address indexed user, uint256 indexed pid, uint256 amount);
    event Withdraw(address indexed user, uint256 indexed pid, uint256 amount);
    event UpdateFarm(uint256 indexed pid, uint256 lastRewardTimestamp, uint256 lpSupply, uint256 accWeSmolPerShare);
    event Harvest(address indexed user, uint256 indexed pid, uint256 amount, uint256 unpaidAmount);
    event BatchHarvest(address indexed user, uint256[] pids);
    event EmergencyWithdraw(address indexed user, uint256 indexed pid, uint256 amount);
    event Skim(address indexed token, address indexed to, uint256 amount);