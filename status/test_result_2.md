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
{"timestamp":"2025-07-10T09:07:03.258197","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":57,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:07:03.258298","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":60,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:07:03.258335","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:07:03.258409","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Invalid values parameter for null validation","context":{"log_index":58,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:07:03.258668","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Exception during batch transfer validation","context":{"log_index":58}}
{"timestamp":"2025-07-10T09:07:03.258838","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Log processing exception","context":{"tx_hash":null,"contract_address":"0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6","log_index":58,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:07:03.258888","level":"WARNING","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"LB transfer validation failed, skipping signal creation","context":{"log_index":58}}
{"timestamp":"2025-07-10T09:07:03.259052","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Invalid values parameter for null validation","context":{"log_index":59,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:07:03.259088","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Exception during liquidity validation","context":{"log_index":59}}
{"timestamp":"2025-07-10T09:07:03.259130","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Log processing exception","context":{"tx_hash":null,"contract_address":"0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6","log_index":59,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:07:03.259162","level":"WARNING","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"LB mint validation failed, skipping signal creation","context":{"log_index":59}}
{"timestamp":"2025-07-10T09:07:03.259191","level":"INFO","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Log processing completed","context":{"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:07:03.259214","level":"WARNING","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Error type breakdown","context":{"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:07:03.259241","level":"ERROR","logger":"indexer.transform.context.TransformContext","message":"Errors added to context","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779"}}
{"timestamp":"2025-07-10T09:07:03.259272","level":"ERROR","logger":"indexer.transform.manager.TransformManager","message":"Transformer generated errors","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779","contract_address":"0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6","transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T09:07:03.259304","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"Signal generation phase failed","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779"}}
{"timestamp":"2025-07-10T09:07:03.259423","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Unmatched transfer reconciliation completed","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779"}}
{"timestamp":"2025-07-10T09:07:03.259458","level":"ERROR","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed with failures","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779","event_count":2}}
   ✅ Generated 2 signals → 2 events
   ⚠️ 2 errors during transformation

4️⃣ Persisting to database...
{"timestamp":"2025-07-10T09:07:03.551550","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T09:07:03.552058","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
   ✅ Persisted 0 events, 0 positions

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
{"timestamp":"2025-07-10T09:07:38.931579","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":4,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:07:38.931682","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":10,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:07:38.931721","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":13,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:07:38.931752","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":16,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:07:38.931780","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":20,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:07:38.931809","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Log processing completed","context":{"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:07:38.931860","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":17,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:07:38.931891","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":21,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:07:38.931919","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":24,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:07:38.931945","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":27,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:07:38.931967","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:07:38.932023","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool swap signal created successfully","context":{"log_index":19,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:07:38.932048","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:07:38.932089","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool swap signal created successfully","context":{"log_index":23,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:07:38.932112","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:07:38.932151","level":"WARNING","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Missing 'to' address in ODOS swap","context":{"log_index":25,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:07:38.932183","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Route signal created successfully","context":{"log_index":25,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:07:38.932206","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Log processing completed","context":{"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:07:38.932246","level":"INFO","logger":"indexer.transform.transformers.aggregators.lfj_aggregator.LfjAggregatorTransformer","message":"Route signal created successfully","context":{"log_index":28,"transformer_name":"LfjAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:07:38.932269","level":"INFO","logger":"indexer.transform.transformers.aggregators.lfj_aggregator.LfjAggregatorTransformer","message":"Log processing completed","context":{"transformer_name":"LfjAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:07:38.932443","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"PoolSwap event created successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:07:38.932478","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"Completed swap signal processing","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:07:38.932543","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"PoolSwap event created successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:07:38.932570","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"Completed swap signal processing","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:07:38.932664","level":"INFO","logger":"indexer.transform.processors.trade_processor.TradeProcessor","message":"Trade processing completed successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:07:38.932698","level":"WARNING","logger":"indexer.transform.registry.TransformRegistry","message":"Pattern not found"}
{"timestamp":"2025-07-10T09:07:38.932719","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"No pattern found for signal","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b","log_index":25}}
{"timestamp":"2025-07-10T09:07:38.932740","level":"WARNING","logger":"indexer.transform.registry.TransformRegistry","message":"Pattern not found"}
{"timestamp":"2025-07-10T09:07:38.932758","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"No pattern found for signal","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b","log_index":28}}
{"timestamp":"2025-07-10T09:07:38.932858","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Unmatched transfer reconciliation completed","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T09:07:38.932888","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b","event_count":6}}
   ✅ Generated 13 signals → 6 events

4️⃣ Persisting to database...
{"timestamp":"2025-07-10T09:07:39.210193","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T09:07:39.289972","level":"ERROR","logger":"indexer.database.repository.trade","message":"Error getting record by content_id"}
{"timestamp":"2025-07-10T09:07:39.297146","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T09:07:39.297367","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T09:07:39.297468","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T09:07:39.297635","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T09:07:39.297742","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
   ✅ Persisted 0 events, 0 positions

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
{"timestamp":"2025-07-10T09:08:19.032759","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":4,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:08:19.032893","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:08:19.032984","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":6,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:08:19.033028","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Log processing completed","context":{"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:08:19.033095","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool transfer signal created successfully","context":{"log_index":7,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:08:19.033145","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool transfer signal created successfully","context":{"log_index":8,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:08:19.033204","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool mint signal created successfully","context":{"log_index":10,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:08:19.033246","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:08:19.033572","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed successfully","context":{"tx_hash":"0x1c0d070e0483701d5473a5bfd08be138c70c2ba7700c12b9c8de938ee7290bb2","event_count":2}}
   ✅ Generated 5 signals → 2 events

4️⃣ Persisting to database...
{"timestamp":"2025-07-10T09:08:19.412842","level":"ERROR","logger":"indexer.database.repository.liquidity","message":"Error getting Liquidity by content_id b8271326ec47: 'add' is not among the defined enum values. Enum name: liquidityaction. Possible values: ADD, REMOVE, UPDATE"}
{"timestamp":"2025-07-10T09:08:19.420706","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T09:08:19.504351","level":"ERROR","logger":"indexer.database.repository.reward","message":"Error getting Reward by content_id 7d325272e389: 'fee' is not among the defined enum values. Enum name: rewardtype. Possible values: FEES, REWARDS"}
{"timestamp":"2025-07-10T09:08:19.509431","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
   ✅ Persisted 0 events, 0 positions

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
{"timestamp":"2025-07-10T09:08:41.356746","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":0,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:08:41.356838","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T09:08:41.356897","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":1,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:08:41.356929","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Log processing completed","context":{"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T09:08:41.356990","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool swap signal created successfully","context":{"log_index":3,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:08:41.357016","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T09:08:41.357054","level":"WARNING","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Missing 'to' address in ODOS swap","context":{"log_index":6,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:08:41.357092","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Route signal created successfully","context":{"log_index":6,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:08:41.357117","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Log processing completed","context":{"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T09:08:41.357275","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"PoolSwap event created successfully","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a"}}
{"timestamp":"2025-07-10T09:08:41.357309","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"Completed swap signal processing","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a"}}
{"timestamp":"2025-07-10T09:08:41.357376","level":"INFO","logger":"indexer.transform.processors.trade_processor.TradeProcessor","message":"Trade processing completed successfully","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a"}}
{"timestamp":"2025-07-10T09:08:41.357411","level":"WARNING","logger":"indexer.transform.registry.TransformRegistry","message":"Pattern not found"}
{"timestamp":"2025-07-10T09:08:41.357435","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"No pattern found for signal","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a","log_index":6}}
{"timestamp":"2025-07-10T09:08:41.357465","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed successfully","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a","event_count":1}}
   ✅ Generated 4 signals → 1 events

4️⃣ Persisting to database...
{"timestamp":"2025-07-10T09:08:41.731408","level":"ERROR","logger":"indexer.database.repository.trade","message":"Error getting record by content_id"}
{"timestamp":"2025-07-10T09:08:41.738741","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
   ✅ Persisted 0 events, 0 positions

5️⃣ Saving to GCS storage...
   ✅ Saved processing block
   ✅ Saved complete block

6️⃣ Verifying results...
   📊 Database: 1 transactions processed
   ☁️ GCS: Processing ❌, Complete ✅

🎉 HYBRID END-TO-END TEST PASSED!