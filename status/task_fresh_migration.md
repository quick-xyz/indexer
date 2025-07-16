Migration Task - Fresh Database Setup
Current Status
Objective: Create completely fresh databases (indexer_shared_v2 and blub_test_v2) using current codebase schema, then migrate existing data from blub_test to the new structure.
Current State:

Enhanced migration manager created but appears to be creating tables with incorrect/incomplete schema
Config import failing with "column does not exist" errors for fields that should exist in current code
Fresh databases exist but schema doesn't match current table definitions
Original blub_test database contains processed data that needs to be preserved

What We're Trying to Do

Fresh Migration: Create new databases with current schema without any migration history
Data Preservation: Keep existing blub_test data intact for later migration
Schema Accuracy: Ensure new tables match current code definitions exactly
Config Import: Successfully import YAML configurations into new databases

Suspected Issues
Primary Issue: Enhanced migration manager may not be creating schema that matches current code definitions. Possible causes:

Migration state conflicts between old and new systems
Enhanced migration manager not properly reading current table definitions
Cached/stale schema being used instead of current code

Secondary Issue: Import process expects fields that may not exist in created tables, despite existing in current code.
Possible Solutions
Option 1: Fix Enhanced Migration Manager

Debug why enhanced migration manager isn't creating correct schema
Ensure it reads current table definitions from code
Resolve any migration state conflicts

Option 2: Start Fresh (Recommended Path)

Scrap all migration systems
Create simple schema generation from current code
Use direct SQLAlchemy create_all() approach
Focus on data migration from old to new databases

Option 3: Use Existing Migration Manager

Leverage working reference migration manager
Adapt it for fresh database creation
Ensure compatibility with current codebase

Data Migration Strategy
Critical Requirement: Preserve existing blub_test data (days of processing work)

Keep original blub_test database untouched
Create mapping between old and new schema structures
Build data migration scripts to transfer processed data

Development Preferences (From Previous Chat)
Communication Style

Ask before developing: Don't jump straight into fixes without understanding the actual problem
One step at a time: Break complex tasks into manageable items
Confirm before generating: Always ask before creating files, especially large ones
Step-by-step methodology: Work incrementally with clear explanations

Development Approach

Verify current state: Check actual files/tables instead of inferring from documentation
Small targeted changes: Update specific methods rather than rewriting entire classes
Question mismatches: When two things don't match, ask which should be changed rather than deciding
Hands-on collaboration: Involve user in design decisions

Code Preferences

Repository patterns: Use established repository patterns for database access
Error handling: Graceful failure handling, log warnings but continue processing
No assumptions: Don't generate code for cases that aren't confirmed to be true
Dependency injection: All new development must use DI patterns

What Doesn't Work

Jumping to solutions: Don't immediately start developing without understanding the problem
Making assumptions: Don't infer problems exist without checking actual code
Multiple versions: Don't generate 9 versions of fixes without dialogue
Ignoring guidance: Don't proceed when told a different approach is needed

Next Steps

Assess current table schema: Verify what was actually created by enhanced migration manager
Compare with code definitions: Identify specific discrepancies
Choose migration approach: Decide between fixing enhanced manager vs. starting fresh
Plan data migration: Design strategy for preserving existing blub_test data