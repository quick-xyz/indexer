from indexer.types.model.nfp import NfpCollectSignal

test = NfpCollectSignal(
    log_index=1,
    contract="0x1234567890abcdef1234567890abcdef12345678",
    token_id=10,
    recipient="0xabcdef1234567890abcdef1234567890abcdef123456",
    amount0="500",
    amount1="200"
)

print(test)
print(test.contract)
print(test.__class__)
print(test.__struct_config__)
