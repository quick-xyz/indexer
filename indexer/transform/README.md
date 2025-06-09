## Liquidity Book
### Swap
python testing/test_pipeline.py 63269916
python testing/scripts/debug_session.py block 63269916  
python testing/diagnostics/quick_diagnostic.py

### DepositedToBins
python testing/test_pipeline.py 58570137
python testing/scripts/debug_session.py analyze 0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779 58570137

### WithdrawnFromBins
python testing/test_pipeline.py 58376529
python testing/scripts/debug_session.py analyze 0x06cd109690c39408a5882ba47703146dd1cffe2ca8bce98a8b91349b27731d03 58376529


## Pharaoh CL
### Swap
62288120 / 0x9710a135af0ec571bcc465c53bf364d7f0c5e8f567c7e27c881ed4c7ebfd6ed7
### Mint
58584879 / 0xdacb291825e75a8ca4b3857fa375bd964e953916546cf5a6c3945cef9d979e44
### Burn
60941433 / 0x4981decb6d8a501e9f591cf1453a19d2a74266b1b065f98e9726d640615c8274
### Collect
63286088 / 0x5301d020b92e36b46681ae28bb703886c3d7deabae8e6258af705bc4de4964d6
### CollectProtocol
63454863 / 0xee83f105e50f788e7e024e374728555523a6c2a72457925c197a6fd6faf58b13



NEED TO ADD HANDLING IN TX MANAGER FOR THE COLLECT SIGNAL RETURN, EXTRA LAYER
if isinstance(signal_value, dict):
    # Handle multiple signals
    for key, signal in signal_value.items():
        process_signal(signal)
else:
    # Handle single signal
    process_signal(signal_value)