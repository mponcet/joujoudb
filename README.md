# JoujouDB

A toy SQL database for learning systems programming concepts in Rust.

DBMS development is an excellent project to teach yourself about system programming, error handling, data structures, parsing, network programming and more.

## Roadmap

### Buffer pool and storage
- [x] Page-based storage system
- [x] Buffer pool manager with eviction
- [x] File organization and access methods

### Indexing & storage models
- [x] Slotted page with basic tuples
- [x] B+ tree implementation
- [ ] Hash indexes
- [ ] B+ tree variants (B-link trees, etc.)

### Query Execution Layer
- [ ] SQL Parser
  - [ ] Lexer for SQL tokens
  - [ ] Parser for basic SQL statements (SELECT, INSERT, UPDATE, DELETE)
  - [ ] AST representation of SQL queries
- [ ] Query Planner/Optimizer
  - [ ] Convert parsed SQL to logical plan
  - [ ] Cost estimation for different access paths
  - [ ] Plan optimization (join reordering, etc.)
- [ ] Execution Engine
  - [ ] Physical operators framework
  - [ ] Table scan operator
  - [ ] Index scan operator
  - [ ] Filter operator (WHERE clauses)
  - [ ] Projection operator (SELECT columns)
  - [ ] Sort operator (ORDER BY)
  - [ ] Aggregate operator (GROUP BY)
  - [ ] Join operators (nested loop, hash join, merge join)
- [ ] Expression Evaluation
  - [ ] Runtime evaluation of WHERE conditions
  - [ ] Computation of SELECT expressions
  - [ ] Built-in functions (string, math, date functions)

### Advanced Features
- [ ] Transactions and Concurrency Control
- [ ] Recovery and logging
- [ ] Views and triggers
- [ ] Stored procedures
- [ ] Network interface (PostgreSQL protocol compatibility)

## Implementation Approach

1. **Start with a Simple Parser**
   - Use a parser generator or write a recursive descent parser
   - Begin with basic SELECT * FROM table syntax

2. **Create Basic Execution Operators**
   - Table scan operator
   - Simple projection operator
   - Basic filter operator

3. **Build Execution Context**
   - Query execution state management
   - Result set handling

4. **Iteratively Add Features**
   - WHERE clauses
   - Column selection
   - Joins
   - Aggregations
   - More complex SQL constructs

## References
- https://15445.courses.cs.cmu.edu/fall2024/
- https://howqueryengineswork.com/
- PostgreSQL 14 Internals by Egor Rogov
- Talking with LLMs
