# YAML Configuration Refactor - Task Document

## Overview
Refactor from two-YAML approach to single-YAML approach with declaration-based shared resource management. YAML declares expectations for shared resources; import process validates they match existing records or creates them if they don't exist.

## Core Changes Needed

### 1. YAML Structure Changes
- **Update to nested structure**: All configs use `model:`, `contracts:`, `tokens:`, etc. sections
- **Remove legacy flat structure support**: Deprecate `from_yaml_dict` fallback logic
- **Single YAML per model**: Combine shared and model-specific configuration

### 2. Configuration Types (msgspec structs)
**Files to update:**
- `indexer/types/configs/model.py` - Add `network` field, fix `target_asset` type
- `indexer/types/configs/contract.py` - Ensure all shared fields are included
- `indexer/types/configs/token.py` - Ensure all shared fields are included
- `indexer/types/configs/address.py` - Ensure all shared fields are included
- `indexer/types/configs/pricing.py` - Align with database model

**Changes needed:**
- Include ALL database fields in structs (no partial definitions)
- Add validation methods to each struct
- Remove legacy `from_yaml_dict` fallback logic

### 3. Import Logic Refactor
**Files to update:**
- `indexer/cli/commands/config.py` - Main import command logic
- `indexer/core/config_service.py` - Service layer for config operations

**Changes needed:**
- Implement declaration-based validation logic
- Add shared resource existence/match checking
- Fail fast on shared resource mismatches with clear error messages
- Add impact analysis for shared resource changes

### 4. Database Model Alignment
**Files to update:**
- `indexer/database/shared/tables/config.py` - Ensure all relationships are correct
- Add missing junction tables if needed (e.g., `ModelPool` if referenced)

### 5. CLI Commands Enhancement
**Files to update:**
- `indexer/cli/commands/config.py` - Model import command
- Create new commands for explicit shared resource management

**New commands needed:**
- `indexer config update-shared-contract` - Update shared contract with impact analysis
- `indexer config update-shared-token` - Update shared token with impact analysis
- `indexer config validate-model` - Validate model YAML against existing shared resources
- `indexer config export-contracts` - Export existing contracts to YAML from address list
- `indexer config export-tokens` - Export existing tokens to YAML from address list
- `indexer config export-addresses` - Export existing addresses to YAML from address list
- `indexer config export-model-template` - Generate model YAML template from resource lists

### 6. Validation System
**Files to update:**
- `indexer/core/config_service.py` - Add shared resource validation methods
- Each config struct's validation methods

**Validation logic needed:**
- Check if shared resources exist
- Compare YAML expectations with database reality
- Generate clear error messages for mismatches
- Impact analysis for proposed changes

### 7. Error Handling & User Experience
**Files to update:**
- `indexer/cli/commands/config.py` - Enhanced error messages
- Add confirmation prompts for shared resource changes

**UX improvements:**
- Clear error messages when shared resources don't match
- Show which models would be affected by shared changes
- Require explicit confirmation for shared resource updates

### 8. Configuration Export System
**Files to update:**
- `indexer/cli/commands/config.py` - Add export commands
- `indexer/core/config_service.py` - Add export methods

**Export functionality needed:**
- Export existing contracts/tokens/addresses to YAML from address lists
- Generate model YAML templates from resource lists
- Ensure exported YAML matches import validation expectations
- Support both individual resource exports and complete model templates

**Example usage:**
```bash
# Export specific contracts to YAML
indexer config export-contracts --addresses 0xabc,0xdef --output contracts.yaml

# Export tokens for model setup
indexer config export-tokens --addresses 0x123,0x456 --output tokens.yaml

# Generate complete model template from resource lists
indexer config export-model-template --name new_model --contracts 0xabc,0xdef --tokens 0x123,0x456 --output new_model.yaml
```

## Implementation Order

### Phase 1: Structure Changes
1. Update msgspec structs to include all database fields
2. Remove legacy YAML loading support
3. Standardize on nested YAML structure

### Phase 2: Validation Logic
1. Add shared resource existence checking
2. Add shared resource match validation
3. Implement clear error reporting

### Phase 3: Export System
1. Add export commands for contracts, tokens, addresses
2. Add model template generation from resource lists
3. Ensure exported YAML format matches import expectations

### Phase 4: Enhanced Commands
1. Update model import with new validation
2. Add explicit shared resource update commands
3. Add impact analysis and confirmation system

### Phase 5: Testing & Documentation
1. Update existing YAML files to new structure
2. Test shared resource validation scenarios
3. Test export/import roundtrip functionality
4. Update CLI help and documentation

## Key Design Principles
- **YAML as declaration**: Describes what should exist, not mutations
- **Fail fast**: Stop on shared resource mismatches with clear errors
- **Explicit changes**: Shared resource updates require separate commands
- **Impact awareness**: Always show which models are affected
- **Confirmation required**: Shared changes need explicit user confirmation