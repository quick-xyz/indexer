# EVM Data Types and Encoding

## Hexadecimal Representation
- All EVM data is represented in hexadecimal format
- Each byte is represented by 2 hex characters (0-9, a-f)
- Standard prefix "0x" indicates hexadecimal
- 32 bytes = 64 hex characters (not including "0x" prefix)
- Numbers are stored in big-endian format

## Fixed-Size Types

### Unsigned Integers
- uint<N>: N-bit unsigned integer (N from 8 to 256, in steps of 8)
- Always left padded with zeros
- Always encoded as 32 bytes (64 hex characters) in topics
- Common sizes:
- Common sizes in hex representation:
  - uint8:   1 byte  = 2 hex chars    (0x00 to 0xff)
  - uint16:  2 bytes = 4 hex chars    (0x0000 to 0xffff)
  - uint32:  4 bytes = 8 hex chars    (0x00000000 to 0xffffffff)
  - uint64:  8 bytes = 16 hex chars
  - uint128: 16 bytes = 32 hex chars
  - uint256: 32 bytes = 64 hex chars

### Address
- 20 bytes = 40 hex chars without "0x" prefix
- Left padded with zeros to 32 bytes (64 hex characters) when in topics
- Treated as uint160 in many contexts
- Example: 0x742d35Cc6634C0532925a3b844Bc454e4438f44e
- In topics: 0x000000000000000000000000742d35Cc6634C0532925a3b844Bc454e4438f44e

### Boolean
- Represented as uint8: 1 byte (2 hex characters)
- Left padded with zeros to 32 bytes (64 hex characters)
- Values: 0 (false) or 1 (true)

## Dynamic Types
- Not allowed in indexed parameters (topics)
- When indexed, store keccak256 hash (32 bytes, 64 hex characters)
- string: Dynamic-sized UTF-8 string
- bytes: Dynamic-sized byte sequence
- arrays: Dynamic or fixed-size arrays

## Type-Specific Encoding Rules
- All types padded to 32 bytes (64 hex characters) in topics
- Fixed-size types are left padded
- Dynamic types have special encoding in data field