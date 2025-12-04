-- Write SQL queries for validation here
"""
transformation.sql:-
Each table has one complete transformation SQL — not multiple small queries.
That single SQL builds the expected dataset with all joins, filters,
     case, decode, substring, etc.
Then, the framework compares expected (SQL output) vs actual (target table) using
      different validation modules (count, duplicate, null, data compare).
"""

select * from [dbo].[customer] where customerId = 1