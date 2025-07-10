🧪 HYBRID END-TO-END PIPELINE TEST
Block: 58570137
Model: blub_test vv2
================================================================================

1️⃣ Retrieving block from GCS...
   Source: quicknode-blub
   ✅ Retrieved block with 1 transactions

2️⃣ Decoding block...
   ✅ Decoded 1 transactions with 4 logs

3️⃣ Transforming block...
{"timestamp":"2025-07-10T09:37:43.428667","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":57,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:37:43.428812","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":60,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:37:43.428853","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:37:43.428934","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Invalid values parameter for null validation","context":{"log_index":58,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:37:43.429197","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Exception during batch transfer validation","context":{"log_index":58}}
{"timestamp":"2025-07-10T09:37:43.429311","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Log processing exception","context":{"tx_hash":null,"contract_address":"0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6","log_index":58,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:37:43.429349","level":"WARNING","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"LB transfer validation failed, skipping signal creation","context":{"log_index":58}}
{"timestamp":"2025-07-10T09:37:43.429494","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Invalid values parameter for null validation","context":{"log_index":59,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:37:43.429531","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Exception during liquidity validation","context":{"log_index":59}}
{"timestamp":"2025-07-10T09:37:43.429577","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Log processing exception","context":{"tx_hash":null,"contract_address":"0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6","log_index":59,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:37:43.429609","level":"WARNING","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"LB mint validation failed, skipping signal creation","context":{"log_index":59}}
{"timestamp":"2025-07-10T09:37:43.429634","level":"INFO","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Log processing completed","context":{"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:37:43.429655","level":"WARNING","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Error type breakdown","context":{"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:37:43.429678","level":"ERROR","logger":"indexer.transform.context.TransformContext","message":"Errors added to context","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779"}}
{"timestamp":"2025-07-10T09:37:43.429707","level":"ERROR","logger":"indexer.transform.manager.TransformManager","message":"Transformer generated errors","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779","contract_address":"0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6","transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:37:43.429739","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"Signal generation phase failed","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779"}}
{"timestamp":"2025-07-10T09:37:43.429866","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Unmatched transfer reconciliation completed","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779"}}
{"timestamp":"2025-07-10T09:37:43.429904","level":"ERROR","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed with failures","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779","event_count":2}}
   ✅ Generated 2 signals → 2 events
   ⚠️ 2 errors during transformation

4️⃣ Persisting to database...
DEBUG: _write_events called with 2 events
DEBUG: Processing event 5b203ca75b52, type: UnknownTransfer
DEBUG: Getting repository for UnknownTransfer
DEBUG: Got repository: <indexer.database.indexer.repositories.transfer_repository.TransferRepository object at 0x10f033650>
DEBUG: Checking if event exists: 5b203ca75b52
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'from_address': '0xf835925589ac11df66690b3aa343de649f38cd83', 'to_address': '0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6', 'amount': '346638774748005349693304'}
DEBUG: Final event data: {'token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'from_address': '0xf835925589ac11df66690b3aa343de649f38cd83', 'to_address': '0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6', 'amount': '346638774748005349693304', 'content_id': '5b203ca75b52', 'tx_hash': '0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779', 'block_number': 58570137, 'timestamp': 1741708903}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
DEBUG: Processing event 7a658cae1290, type: UnknownTransfer
DEBUG: Getting repository for UnknownTransfer
DEBUG: Got repository: <indexer.database.indexer.repositories.transfer_repository.TransferRepository object at 0x10f033650>
DEBUG: Checking if event exists: 7a658cae1290
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'from_address': '0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6', 'to_address': '0xf835925589ac11df66690b3aa343de649f38cd83', 'amount': '24'}
DEBUG: Final event data: {'token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'from_address': '0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6', 'to_address': '0xf835925589ac11df66690b3aa343de649f38cd83', 'amount': '24', 'content_id': '7a658cae1290', 'tx_hash': '0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779', 'block_number': 58570137, 'timestamp': 1741708903}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
   ✅ Persisted 2 events, 0 positions

5️⃣ Saving to GCS storage...
   ✅ Saved processing block
   ✅ Saved complete block

6️⃣ Verifying results...
   📊 Database: 1 transactions processed
   ☁️ GCS: Processing ❌, Complete ✅

🎉 HYBRID END-TO-END TEST PASSED!

🧪 HYBRID END-TO-END PIPELINE TEST
Block: 58584385
Model: blub_test vv2
================================================================================

1️⃣ Retrieving block from GCS...
   Source: quicknode-blub
   ✅ Retrieved block with 1 transactions

2️⃣ Decoding block...
   ✅ Decoded 1 transactions with 7 logs

3️⃣ Transforming block...
{"timestamp":"2025-07-10T09:38:23.282290","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":4,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:38:23.282400","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:38:23.282464","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":6,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:38:23.282496","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Log processing completed","context":{"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:38:23.282541","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool transfer signal created successfully","context":{"log_index":7,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:38:23.282575","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool transfer signal created successfully","context":{"log_index":8,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:38:23.282623","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool mint signal created successfully","context":{"log_index":10,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:38:23.282651","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:38:23.282891","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed successfully","context":{"tx_hash":"0x1c0d070e0483701d5473a5bfd08be138c70c2ba7700c12b9c8de938ee7290bb2","event_count":2}}
   ✅ Generated 5 signals → 2 events

4️⃣ Persisting to database...
DEBUG: _write_events called with 2 events
DEBUG: Processing event b8271326ec47, type: Liquidity
DEBUG: Getting repository for Liquidity
DEBUG: Got repository: <indexer.database.indexer.repositories.liquidity_repository.LiquidityRepository object at 0x10d9328d0>
DEBUG: Checking if event exists: b8271326ec47
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'pool': '0x39888258d60fed9228f89e13eb57a92f1fa832eb', 'provider': '0xd70f5733da864ca282c5e7dd9ffece2bf46ef4d0', 'base_token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'base_amount': '846041545259096309303', 'quote_token': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'quote_amount': '334415089076497730', 'action': <LiquidityAction.ADD: 'add'>}
DEBUG: Final event data: {'pool': '0x39888258d60fed9228f89e13eb57a92f1fa832eb', 'provider': '0xd70f5733da864ca282c5e7dd9ffece2bf46ef4d0', 'base_token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'base_amount': '846041545259096309303', 'quote_token': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'quote_amount': '334415089076497730', 'action': <LiquidityAction.ADD: 'add'>, 'content_id': 'b8271326ec47', 'tx_hash': '0x1c0d070e0483701d5473a5bfd08be138c70c2ba7700c12b9c8de938ee7290bb2', 'block_number': 58584385, 'timestamp': 1741733435}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
DEBUG: Processing event 7d325272e389, type: Reward
DEBUG: Getting repository for Reward
DEBUG: Got repository: <indexer.database.indexer.repositories.reward_repository.RewardRepository object at 0x10d932ba0>
DEBUG: Checking if event exists: 7d325272e389
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'contract': '0x39888258d60fed9228f89e13eb57a92f1fa832eb', 'recipient': '0x60233142befce7d4ed73e7793ead2d6190fccaab', 'token': '0x39888258d60fed9228f89e13eb57a92f1fa832eb', 'amount': '810969532182238971628', 'reward_type': 'fee'}
DEBUG: Final event data: {'contract': '0x39888258d60fed9228f89e13eb57a92f1fa832eb', 'recipient': '0x60233142befce7d4ed73e7793ead2d6190fccaab', 'token': '0x39888258d60fed9228f89e13eb57a92f1fa832eb', 'amount': '810969532182238971628', 'reward_type': 'fee', 'content_id': '7d325272e389', 'tx_hash': '0x1c0d070e0483701d5473a5bfd08be138c70c2ba7700c12b9c8de938ee7290bb2', 'block_number': 58584385, 'timestamp': 1741733435}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
   ✅ Persisted 2 events, 6 positions

5️⃣ Saving to GCS storage...
   ✅ Saved processing block
   ✅ Saved complete block

6️⃣ Verifying results...
   📊 Database: 1 transactions processed
   ☁️ GCS: Processing ❌, Complete ✅

🎉 HYBRID END-TO-END TEST PASSED!


🧪 HYBRID END-TO-END PIPELINE TEST
Block: 63269916
Model: blub_test vv2
================================================================================

1️⃣ Retrieving block from GCS...
   Source: quicknode-blub
   ✅ Retrieved block with 1 transactions

2️⃣ Decoding block...
   ✅ Decoded 1 transactions with 29 logs

3️⃣ Transforming block...
{"timestamp":"2025-07-10T09:38:47.026841","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":4,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:38:47.026983","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":10,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027042","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":13,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027089","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":16,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027132","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":20,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027174","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Log processing completed","context":{"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027249","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":17,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027300","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":21,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027345","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":24,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027386","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":27,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027421","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027510","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool swap signal created successfully","context":{"log_index":19,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027545","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027603","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool swap signal created successfully","context":{"log_index":23,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027636","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027687","level":"WARNING","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Missing 'to' address in ODOS swap","context":{"log_index":25,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027729","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Route signal created successfully","context":{"log_index":25,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027762","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Log processing completed","context":{"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027813","level":"INFO","logger":"indexer.transform.transformers.aggregators.lfj_aggregator.LfjAggregatorTransformer","message":"Route signal created successfully","context":{"log_index":28,"transformer_name":"LfjAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:38:47.027847","level":"INFO","logger":"indexer.transform.transformers.aggregators.lfj_aggregator.LfjAggregatorTransformer","message":"Log processing completed","context":{"transformer_name":"LfjAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:38:47.028058","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"PoolSwap event created successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:38:47.028107","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"Completed swap signal processing","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:38:47.028197","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"PoolSwap event created successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:38:47.028234","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"Completed swap signal processing","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:38:47.028356","level":"INFO","logger":"indexer.transform.processors.trade_processor.TradeProcessor","message":"Trade processing completed successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:38:47.028402","level":"WARNING","logger":"indexer.transform.registry.TransformRegistry","message":"Pattern not found"}
{"timestamp":"2025-07-10T09:38:47.028433","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"No pattern found for signal","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b","log_index":25}}
{"timestamp":"2025-07-10T09:38:47.028464","level":"WARNING","logger":"indexer.transform.registry.TransformRegistry","message":"Pattern not found"}
{"timestamp":"2025-07-10T09:38:47.028489","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"No pattern found for signal","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b","log_index":28}}
{"timestamp":"2025-07-10T09:38:47.028627","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Unmatched transfer reconciliation completed","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:38:47.028670","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b","event_count":6}}
   ✅ Generated 13 signals → 6 events

4️⃣ Persisting to database...
DEBUG: _write_events called with 6 events
DEBUG: Processing event 3e3f4e86a9ac, type: UnknownTransfer
DEBUG: Getting repository for UnknownTransfer
DEBUG: Got repository: <indexer.database.indexer.repositories.transfer_repository.TransferRepository object at 0x123232e40>
DEBUG: Checking if event exists: 3e3f4e86a9ac
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'from_address': '0xc04f291347d21dc663f7646056db22bff8ce8430', 'to_address': '0x9c7ad86e0836d72809d33dfe5673b1141382963f', 'amount': '222475226850925389101097'}
DEBUG: Final event data: {'token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'from_address': '0xc04f291347d21dc663f7646056db22bff8ce8430', 'to_address': '0x9c7ad86e0836d72809d33dfe5673b1141382963f', 'amount': '222475226850925389101097', 'content_id': '3e3f4e86a9ac', 'tx_hash': '0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b', 'block_number': 63269916, 'timestamp': 1749010993}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
DEBUG: Processing event c22d84c95f5d, type: Trade
DEBUG: Getting repository for Trade
DEBUG: Got repository: <indexer.database.indexer.repositories.trade_repository.TradeRepository object at 0x1232327e0>
DEBUG: Checking if event exists: c22d84c95f5d
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'taker': '0x9c7ad86e0836d72809d33dfe5673b1141382963f', 'direction': <TradeDirection.BUY: 'buy'>, 'base_token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'base_amount': '222475226850925389101097', 'trade_type': <TradeType.TRADE: 'trade'>, 'router': '0x45a62b090df48243f12a21897e7ed91863e2c86b', 'swap_count': 2}
DEBUG: Final event data: {'taker': '0x9c7ad86e0836d72809d33dfe5673b1141382963f', 'direction': <TradeDirection.BUY: 'buy'>, 'base_token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'base_amount': '222475226850925389101097', 'trade_type': <TradeType.TRADE: 'trade'>, 'router': '0x45a62b090df48243f12a21897e7ed91863e2c86b', 'swap_count': 2, 'content_id': 'c22d84c95f5d', 'tx_hash': '0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b', 'block_number': 63269916, 'timestamp': 1749010993}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
DEBUG: Processing event 3fcf6651c7ac, type: UnknownTransfer
DEBUG: Getting repository for UnknownTransfer
DEBUG: Got repository: <indexer.database.indexer.repositories.transfer_repository.TransferRepository object at 0x123232e40>
DEBUG: Checking if event exists: 3fcf6651c7ac
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'token': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'from_address': '0xb5fa743b43e44e2cb74c200c81f274b0ca8e9d34', 'to_address': '0x5f3454fa53e5866a4f69bbc2e6b9a041092d9ad7', 'amount': '16424948551223205287'}
DEBUG: Final event data: {'token': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'from_address': '0xb5fa743b43e44e2cb74c200c81f274b0ca8e9d34', 'to_address': '0x5f3454fa53e5866a4f69bbc2e6b9a041092d9ad7', 'amount': '16424948551223205287', 'content_id': '3fcf6651c7ac', 'tx_hash': '0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b', 'block_number': 63269916, 'timestamp': 1749010993}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
DEBUG: Processing event 5377cb98386c, type: UnknownTransfer
DEBUG: Getting repository for UnknownTransfer
DEBUG: Got repository: <indexer.database.indexer.repositories.transfer_repository.TransferRepository object at 0x123232e40>
DEBUG: Checking if event exists: 5377cb98386c
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'token': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'from_address': '0xeed1c0ba2c17855288c3a2b36f3b5068346fa2bd', 'to_address': '0x5f3454fa53e5866a4f69bbc2e6b9a041092d9ad7', 'amount': '72513796347297425'}
DEBUG: Final event data: {'token': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'from_address': '0xeed1c0ba2c17855288c3a2b36f3b5068346fa2bd', 'to_address': '0x5f3454fa53e5866a4f69bbc2e6b9a041092d9ad7', 'amount': '72513796347297425', 'content_id': '5377cb98386c', 'tx_hash': '0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b', 'block_number': 63269916, 'timestamp': 1749010993}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
DEBUG: Processing event da9c89db7968, type: UnknownTransfer
DEBUG: Getting repository for UnknownTransfer
DEBUG: Got repository: <indexer.database.indexer.repositories.transfer_repository.TransferRepository object at 0x123232e40>
DEBUG: Checking if event exists: da9c89db7968
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'token': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'from_address': '0x12b50bfaa528f967cd7f249c78b44ee3818e972d', 'to_address': '0x5f3454fa53e5866a4f69bbc2e6b9a041092d9ad7', 'amount': '60283450357135453'}
DEBUG: Final event data: {'token': '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'from_address': '0x12b50bfaa528f967cd7f249c78b44ee3818e972d', 'to_address': '0x5f3454fa53e5866a4f69bbc2e6b9a041092d9ad7', 'amount': '60283450357135453', 'content_id': 'da9c89db7968', 'tx_hash': '0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b', 'block_number': 63269916, 'timestamp': 1749010993}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
DEBUG: Processing event 1882f6db7df4, type: UnknownTransfer
DEBUG: Getting repository for UnknownTransfer
DEBUG: Got repository: <indexer.database.indexer.repositories.transfer_repository.TransferRepository object at 0x123232e40>
DEBUG: Checking if event exists: 1882f6db7df4
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'from_address': '0x88de50b233052e4fb783d4f6db78cc34fea3e9fc', 'to_address': '0xc04f291347d21dc663f7646056db22bff8ce8430', 'amount': '222475226850925389101097'}
DEBUG: Final event data: {'token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'from_address': '0x88de50b233052e4fb783d4f6db78cc34fea3e9fc', 'to_address': '0xc04f291347d21dc663f7646056db22bff8ce8430', 'amount': '222475226850925389101097', 'content_id': '1882f6db7df4', 'tx_hash': '0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b', 'block_number': 63269916, 'timestamp': 1749010993}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
   ✅ Persisted 6 events, 18 positions

5️⃣ Saving to GCS storage...
   ✅ Saved processing block
   ✅ Saved complete block

6️⃣ Verifying results...
   📊 Database: 1 transactions processed
   ☁️ GCS: Processing ❌, Complete ✅

🎉 HYBRID END-TO-END TEST PASSED!


🧪 HYBRID END-TO-END PIPELINE TEST
Block: 61090576
Model: blub_test vv2
================================================================================

1️⃣ Retrieving block from GCS...
   Source: quicknode-blub
   ✅ Retrieved block with 1 transactions

2️⃣ Decoding block...
   ✅ Decoded 1 transactions with 7 logs

3️⃣ Transforming block...
{"timestamp":"2025-07-10T09:39:11.751446","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":0,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:39:11.751571","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:39:11.751658","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":1,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:39:11.751699","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Log processing completed","context":{"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:39:11.751758","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool swap signal created successfully","context":{"log_index":3,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:39:11.751787","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:39:11.751845","level":"WARNING","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Missing 'to' address in ODOS swap","context":{"log_index":6,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:39:11.751902","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Route signal created successfully","context":{"log_index":6,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:39:11.751967","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Log processing completed","context":{"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:39:11.752183","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"PoolSwap event created successfully","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a"}}
{"timestamp":"2025-07-10T09:39:11.752222","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"Completed swap signal processing","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a"}}
{"timestamp":"2025-07-10T09:39:11.752294","level":"INFO","logger":"indexer.transform.processors.trade_processor.TradeProcessor","message":"Trade processing completed successfully","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a"}}
{"timestamp":"2025-07-10T09:39:11.752331","level":"WARNING","logger":"indexer.transform.registry.TransformRegistry","message":"Pattern not found"}
{"timestamp":"2025-07-10T09:39:11.752353","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"No pattern found for signal","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a","log_index":6}}
{"timestamp":"2025-07-10T09:39:11.752386","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed successfully","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a","event_count":1}}
   ✅ Generated 4 signals → 1 events

4️⃣ Persisting to database...
DEBUG: _write_events called with 1 events
DEBUG: Processing event def8ca5bb568, type: Trade
DEBUG: Getting repository for Trade
DEBUG: Got repository: <indexer.database.indexer.repositories.trade_repository.TradeRepository object at 0x116f36330>
DEBUG: Checking if event exists: def8ca5bb568
DEBUG: Existing event check result: None
DEBUG: Extracting event data
DEBUG: Extracted event data: {'taker': '0x5f3454fa53e5866a4f69bbc2e6b9a041092d9ad7', 'direction': <TradeDirection.SELL: 'sell'>, 'base_token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'base_amount': '-42154817322819680507676', 'trade_type': <TradeType.TRADE: 'trade'>, 'router': None, 'swap_count': 1}
DEBUG: Final event data: {'taker': '0x5f3454fa53e5866a4f69bbc2e6b9a041092d9ad7', 'direction': <TradeDirection.SELL: 'sell'>, 'base_token': '0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd', 'base_amount': '-42154817322819680507676', 'trade_type': <TradeType.TRADE: 'trade'>, 'router': None, 'swap_count': 1, 'content_id': 'def8ca5bb568', 'tx_hash': '0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a', 'block_number': 61090576, 'timestamp': 1745844478}
DEBUG: About to call repository.create
DEBUG: repository.create succeeded
   ✅ Persisted 1 events, 4 positions

5️⃣ Saving to GCS storage...
   ✅ Saved processing block
   ✅ Saved complete block

6️⃣ Verifying results...
   📊 Database: 1 transactions processed
   ☁️ GCS: Processing ❌, Complete ✅

🎉 HYBRID END-TO-END TEST PASSED!