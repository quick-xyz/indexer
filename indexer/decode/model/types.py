from typing import NewType

""" TODO: Add character checking on types for run time validation """

BlockID = NewType("BlockID", int)
""" Integer that represents a valid block number on a chain """

DateTimeStr = NewType("DateTimeStr", str)
""" String with following format, '2024-07-01T01:18:57Z' """

IntStr = NewType("IntStr", str)
""" Integer value (uint64) contained within string quotes """

BaseStr = NewType("BaseStr", str)
""" String that represents a base64 encoded value [variable length] """

HexStr = NewType("HexStr", str)
""" String that represents a hex encoded value. Has prefix of  "0x" [variable length """

HexInt = NewType("HexInt", str)
""" String that represents a hex encoded integer. Has prefix of  "0x" [variable length """

EvmHash = NewType("EvmHash", HexStr)
""" A lowercase 32-byte hex string with a prefix '0x'. [64 char total] """

HexAddress = NewType("HexAddress", EvmHash)
""" A lowercase 32-byte hex string with a prefix of '0x' [64 char total] """

EvmAddress = NewType("EvmAddress", HexStr)
""" A lowercase 20-byte hex string with a prefix of '0x' [42 char total] """

ChecksumAddress  = NewType("ChecksumAddress", HexStr)
""" A mixedcase 20-byte hex string with a prefix of '0x' [42 char total] """

TxEventId = NewType("TxEventId", str)
""" Unique ID for Blockchain Events: txhash_index """