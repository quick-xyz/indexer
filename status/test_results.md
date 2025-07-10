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
{"timestamp":"2025-07-10T08:31:02.050448","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":57,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:31:02.050616","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":60,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:31:02.050659","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:31:02.050807","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Invalid values parameter for null validation","context":{"log_index":58,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T08:31:02.051049","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Exception during batch transfer validation","context":{"log_index":58}}
{"timestamp":"2025-07-10T08:31:02.051189","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Log processing exception","context":{"tx_hash":null,"contract_address":"0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6","log_index":58,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T08:31:02.051247","level":"WARNING","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"LB transfer validation failed, skipping signal creation","context":{"log_index":58}}
{"timestamp":"2025-07-10T08:31:02.051397","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Invalid values parameter for null validation","context":{"log_index":59,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T08:31:02.051435","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Exception during liquidity validation","context":{"log_index":59}}
{"timestamp":"2025-07-10T08:31:02.051480","level":"ERROR","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Log processing exception","context":{"tx_hash":null,"contract_address":"0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6","log_index":59,"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T08:31:02.051514","level":"WARNING","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"LB mint validation failed, skipping signal creation","context":{"log_index":59}}
{"timestamp":"2025-07-10T08:31:02.051540","level":"INFO","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Log processing completed","context":{"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T08:31:02.051564","level":"WARNING","logger":"indexer.transform.transformers.pools.lb_pair.LbPairTransformer","message":"Error type breakdown","context":{"transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T08:31:02.051589","level":"ERROR","logger":"indexer.transform.context.TransformContext","message":"Errors added to context","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779"}}
{"timestamp":"2025-07-10T08:31:02.051619","level":"ERROR","logger":"indexer.transform.manager.TransformManager","message":"Transformer generated errors","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779","contract_address":"0xcb4c6fdcfb7d868df5f1e3d0018438205b243fd6","transformer_name":"LbPairTransformer"}}
{"timestamp":"2025-07-10T08:31:02.051652","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"Signal generation phase failed","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779"}}
{"timestamp":"2025-07-10T08:31:02.051767","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Unmatched transfer reconciliation completed","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779"}}
{"timestamp":"2025-07-10T08:31:02.051797","level":"ERROR","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed with failures","context":{"tx_hash":"0x59e5135a0123ec5f7b88c47b5e0d0537c0230f9b72d789ab1cb33af998b02779","event_count":2}}
   ✅ Generated 2 signals → 2 events
   ⚠️ 2 errors during transformation

4️⃣ Persisting to database...
DEBUG: Creating new record with status: TransactionStatus.PROCESSING
DEBUG: Status value: processing
DEBUG: Status name: PROCESSING
{"timestamp":"2025-07-10T08:31:02.348980","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T08:31:02.349952","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
   ✅ Persisted 0 events, 4 positions

5️⃣ Saving to GCS storage...
   ✅ Saved processing block
   ✅ Saved complete block

6️⃣ Verifying results...
   📊 Database: 1 transactions processed
   ☁️ GCS: Processing ❌, Complete ✅

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
{"timestamp":"2025-07-10T08:32:36.560174","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":4,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:32:36.560264","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:32:36.560324","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":6,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T08:32:36.560353","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Log processing completed","context":{"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T08:32:36.560402","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool transfer signal created successfully","context":{"log_index":7,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T08:32:36.560436","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool transfer signal created successfully","context":{"log_index":8,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T08:32:36.560486","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool mint signal created successfully","context":{"log_index":10,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T08:32:36.560515","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T08:32:36.560743","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed successfully","context":{"tx_hash":"0x1c0d070e0483701d5473a5bfd08be138c70c2ba7700c12b9c8de938ee7290bb2","event_count":2}}
   ✅ Generated 5 signals → 2 events

4️⃣ Persisting to database...
DEBUG: Creating new record with status: TransactionStatus.PROCESSING
DEBUG: Status value: processing
DEBUG: Status name: PROCESSING
   ✅ Persisted 2 events, 6 positions

5️⃣ Saving to GCS storage...
   ✅ Saved processing block
   ✅ Saved complete block

6️⃣ Verifying results...
   📊 Database: 1 transactions processed
   ☁️ GCS: Processing ❌, Complete ✅



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
{"timestamp":"2025-07-10T08:34:21.216125","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":0,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:34:21.216220","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:34:21.216275","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":1,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T08:34:21.216307","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Log processing completed","context":{"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T08:34:21.216372","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool swap signal created successfully","context":{"log_index":3,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T08:34:21.216398","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T08:34:21.216436","level":"WARNING","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Missing 'to' address in ODOS swap","context":{"log_index":6,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T08:34:21.216473","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Route signal created successfully","context":{"log_index":6,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T08:34:21.216497","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Log processing completed","context":{"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T08:34:21.216931","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"PoolSwap event created successfully","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a"}}
{"timestamp":"2025-07-10T08:34:21.217087","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"Completed swap signal processing","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a"}}
{"timestamp":"2025-07-10T08:34:21.217193","level":"INFO","logger":"indexer.transform.processors.trade_processor.TradeProcessor","message":"Trade processing completed successfully","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a"}}
{"timestamp":"2025-07-10T08:34:21.217239","level":"WARNING","logger":"indexer.transform.registry.TransformRegistry","message":"Pattern not found"}
{"timestamp":"2025-07-10T08:34:21.217264","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"No pattern found for signal","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a","log_index":6}}
{"timestamp":"2025-07-10T08:34:21.217304","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed successfully","context":{"tx_hash":"0xc6a25db4259e7133ee399c1981a80bba1b378519c72dd0cd5479fbb8bd8dde4a","event_count":1}}
   ✅ Generated 4 signals → 1 events

4️⃣ Persisting to database...
DEBUG: Creating new record with status: TransactionStatus.PROCESSING
DEBUG: Status value: processing
DEBUG: Status name: PROCESSING
   ✅ Persisted 1 events, 4 positions

5️⃣ Saving to GCS storage...
   ✅ Saved processing block
   ✅ Saved complete block

6️⃣ Verifying results...
   📊 Database: 1 transactions processed
   ☁️ GCS: Processing ❌, Complete ✅




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
{"timestamp":"2025-07-10T08:36:13.417376","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":4,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417478","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":10,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417525","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":13,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417559","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":16,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417595","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Transfer signal created successfully","context":{"log_index":20,"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417628","level":"INFO","logger":"indexer.transform.transformers.tokens.wavax.WavaxTransformer","message":"Log processing completed","context":{"transformer_name":"WavaxTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417680","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":17,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417715","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":21,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417750","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":24,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417783","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Transfer signal created successfully","context":{"log_index":27,"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417812","level":"INFO","logger":"indexer.transform.transformers.tokens.token_base.TokenTransformer","message":"Log processing completed","context":{"transformer_name":"TokenTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417883","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool swap signal created successfully","context":{"log_index":19,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417916","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417966","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Pool swap signal created successfully","context":{"log_index":23,"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T08:36:13.417998","level":"INFO","logger":"indexer.transform.transformers.pools.lfj_pool.LfjPoolTransformer","message":"Log processing completed","context":{"transformer_name":"LfjPoolTransformer"}}
{"timestamp":"2025-07-10T08:36:13.418053","level":"WARNING","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Missing 'to' address in ODOS swap","context":{"log_index":25,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T08:36:13.418093","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Route signal created successfully","context":{"log_index":25,"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T08:36:13.418120","level":"INFO","logger":"indexer.transform.transformers.aggregators.odos_aggregator.OdosAggregatorTransformer","message":"Log processing completed","context":{"transformer_name":"OdosAggregatorTransformer"}}
{"timestamp":"2025-07-10T08:36:13.418164","level":"INFO","logger":"indexer.transform.transformers.aggregators.lfj_aggregator.LfjAggregatorTransformer","message":"Route signal created successfully","context":{"log_index":28,"transformer_name":"LfjAggregatorTransformer"}}
{"timestamp":"2025-07-10T08:36:13.418190","level":"INFO","logger":"indexer.transform.transformers.aggregators.lfj_aggregator.LfjAggregatorTransformer","message":"Log processing completed","context":{"transformer_name":"LfjAggregatorTransformer"}}
{"timestamp":"2025-07-10T08:36:13.418407","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"PoolSwap event created successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T08:36:13.418448","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"Completed swap signal processing","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T08:36:13.418522","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"PoolSwap event created successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T08:36:13.418555","level":"INFO","logger":"indexer.transform.patterns.trading.Swap_A","message":"Completed swap signal processing","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T08:36:13.418654","level":"INFO","logger":"indexer.transform.processors.trade_processor.TradeProcessor","message":"Trade processing completed successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T08:36:13.418694","level":"WARNING","logger":"indexer.transform.registry.TransformRegistry","message":"Pattern not found"}
{"timestamp":"2025-07-10T08:36:13.418719","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"No pattern found for signal","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b","log_index":25}}
{"timestamp":"2025-07-10T08:36:13.418744","level":"WARNING","logger":"indexer.transform.registry.TransformRegistry","message":"Pattern not found"}
{"timestamp":"2025-07-10T08:36:13.418766","level":"WARNING","logger":"indexer.transform.manager.TransformManager","message":"No pattern found for signal","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b","log_index":28}}
{"timestamp":"2025-07-10T08:36:13.418880","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Unmatched transfer reconciliation completed","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"}}
{"timestamp":"2025-07-10T08:36:13.418914","level":"INFO","logger":"indexer.transform.manager.TransformManager","message":"Transaction processing completed successfully","context":{"tx_hash":"0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b","event_count":6}}
   ✅ Generated 13 signals → 6 events

4️⃣ Persisting to database...
DEBUG: Creating new record with status: TransactionStatus.PROCESSING
DEBUG: Status value: processing
DEBUG: Status name: PROCESSING
{"timestamp":"2025-07-10T08:36:13.711434","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T08:36:13.885333","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T08:36:13.885890","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T08:36:13.886021","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
{"timestamp":"2025-07-10T08:36:13.886145","level":"ERROR","logger":"indexer.database.writers.domain_event_writer","message":"Failed to write event"}
   ✅ Persisted 1 events, 18 positions

5️⃣ Saving to GCS storage...
   ✅ Saved processing block
   ✅ Saved complete block

6️⃣ Verifying results...
   📊 Database: 1 transactions processed
   ☁️ GCS: Processing ❌, Complete ✅

🎉 HYBRID END-TO-END TEST PASSED!