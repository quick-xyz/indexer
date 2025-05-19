
'''
Liquidty Book Byte32 Decoder
LB introduced packing both token_x and token_y in a single byte32
This decoder unpacks the byte32 into two separate token amounts
'''

def decode_amounts(amounts: bytes) -> tuple:
    """
    Decodes the amounts bytes input as 2 integers.

    :param amounts: amounts to decode.
    :return: tuple of ints with the values decoded.
    """

    amounts = bytes.fromhex(amounts)

    # Read the right 128 bits of the 256 bits
    amounts_x = int.from_bytes(amounts, byteorder="big") & (2 ** 128 - 1)

    # Read the left 128 bits of the 256 bits
    amounts_y = int.from_bytes(amounts, byteorder="big") >> 128

    return (amounts_x, amounts_y)