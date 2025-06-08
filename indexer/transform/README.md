# BaseTransformer vs Applied Transformers
## Move to BaseTransformer:

✅ General validation (_validate_not_null, _create_log_exception)
✅ Error creation utilities
✅ Common logging patterns
✅ Basic signal creation helpers (if patterns emerge)

## Keep in Applied Transformers:

✅ Attribute extraction (_get_transfer_attributes) - domain-specific
✅ Signal creation (_create_transfer_signal) - type-specific
✅ Business validation - each transformer knows its rules
✅ Main processing logic (process_logs) - keeps it readable

## Benefits of This Split:

* Fast comprehension - main logic stays visible in applied transformers
* Reduced duplication - common utilities in base
* Clear ownership - domain logic stays with domain transformers
* Easy maintenance - common patterns can be refactored to base later

This approach gives you both clarity (logic stays visible) and efficiency (shared utilities) without over-abstracting too early.