import uuid

def create_event_uuid(tx_hash, timestamp, addresses, amounts):
    """
    Create a deterministic UUID5 from blockchain event data
    """
    # Normalize and sort inputs for consistency
    addresses = sorted(addr.lower() for addr in addresses if addr)
    amounts = sorted(int(amt) for amt in amounts if amt is not None)
    
    # Create a consistent string representation
    # Use a separator unlikely to appear in the data
    components = [
        tx_hash.lower() if tx_hash else "",
        str(timestamp) if timestamp is not None else "0",
        "-".join(addresses),
        "-".join(str(amt) for amt in amounts)
    ]
    data_string = "||".join(components)
    
    # Generate UUID5 using NAMESPACE_URL
    return uuid.uuid5(uuid.NAMESPACE_URL, data_string)