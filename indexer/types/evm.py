# indexer/types/evm.py

from msgspec import Struct,field
from typing import Optional, Any

from .new import HexStr, HexInt, EvmAddress, EvmHash


class EvmLog(Struct):
    address: EvmAddress
    blockHash: EvmHash
    blockNumber: HexStr
    data: HexStr
    logIndex: HexInt
    removed: bool # False, when it hasn't been removed during reorg
    topics: list[EvmHash]
    transactionHash: EvmHash
    transactionIndex: HexStr

class EvmTxReceipt(Struct):
    blockHash: EvmHash
    blockNumber: HexInt
    cumulativeGasUsed: HexStr
    effectiveGasPrice: HexStr
    from_: EvmAddress = field(name="from")  # from is protected word in python
    gasUsed: HexStr
    logs: list[EvmLog]
    logsBloom: Any
    status: HexInt # 1 (Success) or 2 (Failure)
    transactionHash: EvmHash
    transactionIndex: HexInt
    type: HexStr
    contractAddress: Optional[EvmAddress] = None
    to: Optional[EvmAddress] = None

class EvmTransaction(Struct):
    blockHash: EvmHash
    blockNumber: HexInt
    from_: EvmAddress = field(name="from")  # from is protected word in python
    gas: HexStr
    gasPrice: HexStr
    hash: EvmHash
    input: HexStr
    nonce: HexInt
    r: EvmHash
    s: EvmHash
    transactionIndex: HexInt
    type: HexInt
    v: HexInt
    value: HexInt
    accessList: Optional[list[Any]] = None
    chainId: Optional[HexInt] = None
    maxFeePerGas: Optional[HexStr] = None
    maxPriorityFeePerGas: Optional[HexStr] = None
    to: Optional[EvmAddress] = None

class EvmFilteredBlock(Struct):
    block: HexStr
    timestamp: HexInt # unix timestamp in hexadecimal
    transactions: list[Optional[EvmTransaction]]
    receipts: list[Optional[EvmTxReceipt]]