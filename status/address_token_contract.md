**`addresses`**
- `id` (Primary Key)
- `address` (EVM address)
- `name` (Address name/label)
- `project` (Associated project)
- `description` (Address description)
- `type` (eoa/contract)
- `subtype` (treasury/lp/token/etc)
- `tags` (List,configurable for future groupings)
- `status` (active/inactive)
- `created_at`, `updated_at`

**`contracts`**
- `id` (Primary Key)
- `address_id` (Foreign Key)
- `block_created` (block number when created)
- `abi_dir` (where abi can be found)
- `abi_file` (abi filename)
- `transformer` (transformer class name)
- `transform_init` (dict specific to transformer)
- `status` (active/inactive)
- `created_at`, `updated_at`

**`tokens`**
- `id` (Primary Key)
- `address_id` (Foreign Key)
- `symbol` (token symbol)
- `decimals` (token decimals)
- `status` (active/inactive)
- `created_at`, `updated_at`

**`pools`**
- `id` (Primary Key)
- `address_id` (Foreign Key)
- `base_token` (token frame of reference)
- `pricing_default` ( = pricing_strategy_default)
- `pricing_start`  ( = pricing_start_block, block where to use for pricing, global)
- `pricing_end` ( = pricing_end_block, block to stop using for pricing, global)
- `status` (active/inactive)
- `created_at`, `updated_at`