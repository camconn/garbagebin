
- Keep comments terse. Assume the reader knows what they're doing.
- Don't write numbers that are likely to go stale in comments or docs (e.g., "12 test files", "4 routes")
- Long-form documentation and design decisions belong in `docs/`, not in the code
- Follow the design documents
- The user will push and deploy, or they will ask you to. Don't do it directly
- KISS - Keep it Simple, Stupid
- There should be tests + integration tests. Smoke tests should run in fewer than 10 minutes.
- Make sure code lints and passes 
- Any work which is planned but deferred should be noted in the `TODO` folder
