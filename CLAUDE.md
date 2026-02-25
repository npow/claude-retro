
## Claude Retro Suggestions
<!-- claude-retro-auto -->
- Before implementing features requiring data queries, always ask 'what does the UI actually display with this data?' and verify the data source can provide it, rather than building the feature and discovering mid-implementation that the data structure doesn't support it.
- When debugging backend data bugs, check the data source (database query, API response, log output) directly with a quick query FIRST before investigating application code.
- When the user says 'do it', 'continue', 'just finish it', or 'DO EVERYTHING', work autonomously without asking for clarification or pausing for approval. Only return when blocked or asking would save significant wasted work.
- Before proposing large rewrites (UI redesigns, architectural changes, multi-file refactors), spend the first 1-2 turns validating the core problem with the user: 'What are the 2-3 specific things that need to change?' rather than proposing comprehensive redesigns.
- When setting up external tool integrations (MCP, Jira, credentials) in the middle of a session, immediately ask the user for configuration details rather than attempting multiple failed calls. A 2-minute credential clarification beats 60 turns of failed attempts.
- After implementing data-flow features (especially those involving LLM calls or schema changes), always run end-to-end integration tests with actual data before declaring success. Don't rely on code inspection — verify the data actually gets written to tables, and check for silent failures like guard conditions (`if count > 0`) that skip execution without error messages.
- When a user says 'make it look like X', ask in the first turn: 'Are we optimizing visuals only, or also the underlying data/content quality?' If the report/output matters more than the UI, prioritize that analysis first.
- When implementing data queries that will be exposed through multiple API endpoints, define a shared filtering utility (e.g., `apply_standard_session_filters()`) to prevent count and visibility discrepancies. Verify filter consistency across endpoints before declaring features complete.
<!-- claude-retro-auto -->
