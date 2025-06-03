class TokenTransformer(BaseTransformer):
    """Base transformer for simple token contracts (ERC20, ERC721, etc.)"""
    
    def __init__(self, contract: EvmAddress):
        super().__init__(contract_address=contract)

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[
        Optional[Dict[DomainEventId, Transfer]], Optional[Dict[ErrorId, ProcessingError]]
    ]:
        """Standard ERC20 Transfer processing"""
        transfers = {}
        errors = {}

        for log in logs:
            try:
                if log.name == "Transfer":
                    transfer = self._build_transfer_from_log(log, tx)
                    if transfer:
                        transfers[transfer.content_id] = transfer
                        
            except Exception as e:
                self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)
                
        return transfers if transfers else None, errors if errors else None

    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[
        Optional[Dict[DomainEventId, Transfer]], 
        Optional[Dict[DomainEventId, DomainEvent]], 
        Optional[Dict[ErrorId, ProcessingError]]
    ]:
        """Basic token transformers typically don't create domain events"""
        return None, None, None