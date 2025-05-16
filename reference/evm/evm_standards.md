# EVM Event Standards

## Event Structure Fundamentals
- Events consist of topics and data field
- First topic (topic0) is always the event signature hash
- Maximum of 4 topics total (including signature)
- Data field has no size restriction

## Parameter Storage
- Each slot is 32 bytes
- Multiple values can be packed into a single slot
- Values are packed from right to left
- Must specify offset when values share a slot

## Event Parameters Rules
- Indexed parameters become topics (max 3 excluding signature)
- Non-indexed parameters stored in data field
- Indexed parameters always padded to 32 bytes
- Dynamic types in indexed position store their keccak256 hash

## Understanding Offsets
- Offsets are specified in bytes but apply to hex strings
- "0x" prefix is never counted in offset calculations
- Each byte = 2 hex characters
- For a 32-byte word:
  - Full word = 64 hex characters (not including "0x")
  - offset = 16 means:
    - Skip "0x" prefix
    - Skip first 32 hex characters (16 bytes)
    - Read next 32 hex characters (16 bytes)

## References
- Solidity Events: https://docs.soliditylang.org/en/latest/contracts.html#events
- EVM Encoding: https://docs.soliditylang.org/en/latest/abi-spec.html
- Contract ABI: https://ethereum.org/en/developers/docs/apis/json-rpc/