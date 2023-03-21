# COSC 404 - Database System Implementation<br/>Lab 8 - Transactions with Microsoft SQL Server

This lab practices programming with transactions and isolation levels using Microsoft SQL Server.

## Transactions in Microsoft SQL Server (15 marks)

[Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server) implements many transaction [isolation levels](https://docs.microsoft.com/en-us/sql/connect/jdbc/understanding-isolation-levels) including read uncommitted, read committed, snapshot, repeatable read, and serializable. To use transactions in Java and JDBC you must:

- For a JDBC connection, `setAutoCommit` to false so that transactions may consist of multiple statements.  Example: `con.setAutoCommit(false);`
- Set the transaction isolation level.  Example: `con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);`
- Execute statements as usual.  To commit call `con.commit()` and to rollback call `con.rollback();`

In this lab, you will write two transactions and for a given sequence of queries/updates, determine the correct isolation level to produce the expected output.

- +3 marks - for transaction `readBalance()` to read a balance for a given account
- +7 marks - for transaction `transfer()` to transfer an amount from one account to another
- +5 marks - for 5 transaction isolation level tests (`setIsolationLevelTest`)

### References

- [Java Connection API](https://docs.oracle.com/en/java/javase/15/docs/api/java.sql/java/sql/Connection.html)
- [SQL Server Isolation Levels](https://docs.microsoft.com/en-us/sql/connect/jdbc/understanding-isolation-levels)
- [SQL Server Isolation Levels by Example](https://gavindraper.com/2012/02/18/sql-server-isolation-levels-by-example/)
- [Enabling Snapshot Isolation with SQL Server](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server)

## Submission

The lab can be marked immediately by the professor or TA by showing the output of the JUnit tests and by a quick code review.  Otherwise, submit the URL of your GitHub repository on Canvas. **Make sure to commit and push your updates to GitHub.**
