package trans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Performs various bank transactions on Microsoft SQL Server using different
 * SQL isolation levels.
 */
public class DatabaseTransactions {
	/**
	 * Connection to database
	 */
	private Connection con;

	/**
	 * Customer table name
	 */
	public static final String CUSTOMER_TABLE_NAME = "Customer";

	/**
	 * Account table name
	 */
	public static final String ACCOUNT_TABLE_NAME = "Account";

	/**
	 * Database name
	 */
	public static final String DATABASE_NAME = "tempdb";

	/**
	 * Main method
	 * 
	 * @param args
	 *             no arguments required
	 */
	public static void main(String[] args) throws Exception {
		DatabaseTransactions dbtrans = new DatabaseTransactions();
		dbtrans.connect();
		dbtrans.load();
		System.out.println(DatabaseTransactions.resultSetToString(dbtrans.query_customer(), 100));
		System.out.println(DatabaseTransactions.resultSetToString(dbtrans.query_account(), 100));

		// Query some account balances
		System.out.println(dbtrans.queryBalance(5));
		System.out.println(dbtrans.queryBalance(150));

		// Perform some transfers
		System.out.println(dbtrans.doTransfer(100, 7, 107));
		System.out.println(dbtrans.doTransfer(1000, 8, 108));
		System.out.println(dbtrans.doTransfer(1000, 17, 119));
	}

	/**
	 * Requests an account balance given the id and returns result in a String form
	 * for convenience.
	 * 
	 * @param acctId
	 *               account id
	 * @return
	 *         success message if transfer was successful or error if failed
	 */
	public String queryBalance(int acctId) {
		try {
			int value = readBalance(acctId);
			return "Balance for account " + acctId + ": " + value;
		} catch (SQLException e) {
			return e.toString();
		}
	}

	/**
	 * Performs an amount transfer from one account to another and returns result in
	 * String form.
	 * 
	 * @param amount
	 *                 amount to transfer, negative values are possible
	 * @param acctFrom
	 *                 id of account to transfer from
	 * @param acctTo
	 *                 id of account to transfer to
	 * @return
	 *         success message or error if failed
	 */
	public String doTransfer(int amount, int acctFrom, int acctTo) {
		try {
			transfer(amount, acctFrom, acctTo);
			return "Transferred " + amount + " from account " + acctFrom + " to " + acctTo;
		} catch (SQLException e) {
			return e.toString();
		}
	}

	/**
	 * Connects to Microsoft SQL Server database and returns connection.
	 * 
	 * @return
	 *         connection
	 */
	public Connection connect() throws SQLException {
		String url = "jdbc:sqlserver://localhost:1433;DatabaseName=" + DATABASE_NAME;
		String uid = "sa";
		String pw = "MSsql404!";

		System.out.println("Connecting to database.");
		con = DriverManager.getConnection(url, uid, pw);

		// Important: Setting autoCommit() to false requires you to commit or rollback
		// explicitly rather than driver committing after every statement.
		con.setAutoCommit(false);
		return con;
	}

	/**
	 * Loads data into SQL Server.
	 */
	public void load() throws Exception {
		executeScript("bin/data/drop.sql");
		executeScript("bin/data/bank.sql");

		// Enable SNAPSHOT_ISOLATION for the database
		Statement stmt = con.createStatement();
		stmt.execute("ALTER DATABASE " + DATABASE_NAME + " SET ALLOW_SNAPSHOT_ISOLATION ON;");
		con.commit();
	}

	/**
	 * Performs a query that prints out all customer data.
	 */
	public ResultSet query_customer() throws SQLException {
		Statement stmt = con.createStatement();
		System.out.println("\nCustomer data: ");
		return stmt.executeQuery("SELECT * FROM " + CUSTOMER_TABLE_NAME);
	}

	/**
	 * Performs a query that prints out all account data.
	 */
	public ResultSet query_account() throws SQLException {
		Statement stmt = con.createStatement();
		System.out.println("\nAccount data: ");
		return stmt.executeQuery("SELECT * FROM " + ACCOUNT_TABLE_NAME);
	}

	/**
	 * Creates a new account for given customer.
	 * 
	 * @param custId
	 *                customer id
	 * @param acctype
	 *                account type
	 * @param acctId
	 *                account id
	 * @param acctBal
	 *                account balance
	 */
	public void createAccount(Connection con, int custId, int acctype, int acctId, int acctBal) {
		PreparedStatement stmt = null;

		try {
			stmt = con.prepareStatement("INSERT INTO Account (acctId, cid, amount, acctype) VALUES (?, ?, ?, ?);");
			stmt.setInt(1, acctId);
			stmt.setInt(2, custId);
			stmt.setInt(3, acctBal);
			stmt.setInt(4, acctype);
			System.out.println("Performing INSERT.");
			stmt.executeUpdate();
			System.out.println("INSERT done.");
			con.commit();
			System.out.println("New account created successfully.");
		} catch (SQLException e) {
			System.out.println("Failed to create new customer account account: " + acctId + " Exception: " + e);
			try {
				con.rollback(); // Undo any changes if have an exception
			} catch (SQLException ex) {
				System.err.println("SQLException: " + ex);
			}
		} finally {
			if (stmt != null) {
				try {
					stmt.close(); // Close statement
				} catch (SQLException ex) {
					System.err.println("SQLException: " + ex);
				}
			}
		}
	}

	/**
	 * Performs two queries that computes the total of all accounts. Used to test
	 * isolation levels.
	 * 
	 * @param con
	 *            connection to use
	 * @return
	 *         integer array with first value being total returned by first query
	 *         and second value being total returned by second query
	 * @throws SQLException
	 *                      if an error occurs
	 */
	public int[] getTotals(Connection con) throws SQLException {
		try {
			PreparedStatement stmt = con.prepareStatement("SELECT sum(amount) FROM Account");
			System.out.println("Getting first total.");
			ResultSet rs = stmt.executeQuery();
			if (!rs.next()) {
				con.rollback();
				throw new SQLException("No account records found.");
			}
			int[] values = new int[2];
			int value = (int) rs.getInt(1);
			values[0] = value;

			// Perform a pause between queries
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				// Ignore any exception
			}

			System.out.println("Getting second total.");
			rs = stmt.executeQuery();
			System.out.println("Second query complete.");
			if (!rs.next()) {
				con.rollback();
				throw new SQLException("No account records found.");
			}
			value = (int) rs.getInt(1);
			values[1] = value;

			con.commit();
			System.out.println("Transaction committed.");
			return values;
		} catch (SQLException e) {
			con.rollback();
			throw e;
		}
	}

	/**
	 * Updates an account balance with a delay before commit. Used to test
	 * transaction isolation levels.
	 * 
	 * @param con
	 *               connection to use
	 * @param acctId
	 *               account id to update
	 * @param amount
	 *               amount to add to account
	 * @param delay
	 *               delay in milliseconds between executing update and performing
	 *               commit
	 * @throws SQLException
	 *                      if an error occurs
	 */
	public void updateAccountBalance(Connection con, int acctId, int amount, int delay) throws SQLException {
		try {
			PreparedStatement stmt = con.prepareStatement("UPDATE Account SET amount = ? WHERE acctId = ?");
			stmt.setInt(1, amount);
			stmt.setInt(2, acctId);
			System.out.println("Updating balance in account.");
			stmt.executeUpdate();
			System.out.println("Update executed.");

			// Perform a pause
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				// Ignore any exception
			}

			System.out.println("Committing update.");
			con.commit();
			System.out.println("Update committed.");
		} catch (SQLException e) {
			con.rollback();
			throw e;
		}
	}

	/*
	 * Modify code starting here.
	 */

	/**
	 * Reads and return balance (integer) from account with given id.
	 * 
	 * @param acctId
	 *               account id
	 * @return
	 *         account balance (integer)
	 * @throws SQLException
	 *                      if an error occurs
	 */
	public int readBalance(int acctId) throws SQLException {
		// TODO: Read balance from given account id using a PreparedStatement
		// Throw the following exception if the account is not found:
		// throw new SQLException("Account "+acctId+" not found.");
		// Use the con instance variable for the JDBC connection.
		// Make sure to commit() if success or rollback if exception or account is not
		// found.
		try{
			PreparedStatement stmt = con.prepareStatement("SELECT amount FROM Account WHERE acctId = ?");
			stmt.setInt(1, acctId);
			System.out.println("Returning balance in account.");
			ResultSet rs = stmt.executeQuery();
			if (!rs.next()) {
				con.rollback();
				throw new SQLException("No account records found.");
			}
			int[] values = new int[2];
			int value = (int) rs.getInt(1);
			values[0] = value;


			System.out.println("Update executed.");	

			System.out.println("Committing update.");
			con.commit();
			System.out.println("Update committed.");
			return value;
		}catch(SQLException e){
			con.rollback();
			throw new SQLException("Account " + acctId + " not found.");
		}
	}

	/**
	 * Transfers a given amount from account acct1 to account acct2.
	 * 
	 * @param amount
	 *                 amount to transfer
	 * @param acctFrom
	 *                 id of account transfer from
	 * @param acctTo
	 *                 id of account transfer to
	 */
	public void transfer(int amount, int acctFrom, int acctTo) throws SQLException {
		// TODO: Perform transfer using PreparedStatements.
		// Use the con instance variable for the JDBC connection.
		// Make sure to commit() if success or rollback if exception, account is not
		// found, or insufficient funds.

		try {
			// TODO: Retrieve information on first account
			// TODO: Throw the following exception if the account is not found:
			// throw new SQLException("Account "+acctId+" not found.");

			// TODO: Retrieve information on second account
			// TODO: Throw the following exception if the account is not found:
			// throw new SQLException("Account "+acctId+" not found.");

			// TODO: Verify accounts belong to same customer id
			// TODO: Throw the following exception if the accounts do not belong to the same
			// customer
			// throw new SQLException("Transfer failed because accounts do not belong to
			// same customer.");

			// TODO: Verify that have sufficient funds to perform transfer
			// TODO: Throw the following exception if there are insufficient funds to
			// perform the transfer
			// throw new SQLException("Transfer failed because of insufficient balance.");

			// TODO: Update balances and commit transaction

		} catch (Exception e) {
			con.rollback();
			throw e;
		}
	}

	/*
	 * Modify these methods to pass isolation level tests. See
	 * TestTransactions.testIsolationLevel for more details.
	 * Note: To set SQL Server snapshot isolation use:
	 * con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED + 4094);
	 */

	public void setIsolationLevelTest1(Connection con) throws SQLException, InterruptedException {
		// TODO: Set isolation level of connection so that the following
		/*
		 * Submitted sequence of operations:
		 * - con1: read query 1
		 * - con1: wait 2 seconds
		 * - con2: update
		 * - con2: wait 3 seconds
		 * - con1: read query 2 (same query)
		 * - con1: commit read
		 * - con2: commit update
		 * - Expected output: First value: 2578180 Second value: 5577160
		 * - Read query 2 must not be blocked waiting on update.
		 */
		con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);


	}

	public void setIsolationLevelTest2(Connection con) throws SQLException {
		// TODO: Set isolation level of connection so that the following
		/*
		 * Submitted sequence of operations:
		 * - con1: read query 1
		 * - con1: wait 2 seconds
		 * - con2: update
		 * - con2: wait 3 seconds
		 * - con1: read query 2 (same query)
		 * - con1: commit read
		 * - con2: commit update
		 * - Expected output: First value: 2578180 Second value: 2578180
		 * - Read query 2 must not be blocked waiting on update.
		 * - Update must not be blocked on read.
		 */
		con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED + 4094);
	}

	public void setIsolationLevelTest3(Connection con) throws SQLException {
		// TODO: Set isolation level of connection so that the following
		/*
		 * Submitted sequence of operations:
		 * - con1: read query 1
		 * - con1: wait 2 seconds
		 * - con2: update
		 * - con2: wait 3 seconds
		 * - con1: read query 2 (same query)
		 * - con1: commit read
		 * - con2: commit update
		 * - Expected output: First value: 2578180 Second value: 5577160
		 * - Read query 2 must not be blocked waiting on update.
		 * - Update is not blocked waiting for read.
		 */
		// Note: This is an example. It is not correct.																			
		con.setTransactionIsolation((Connection.TRANSACTION_READ_COMMITTED));
	}

	public void setIsolationLevelTest4(Connection con) throws SQLException {
		// TODO: Set isolation level of connection so that the following
		/*
		 * Submitted sequence of operations:
		 * - con1: read query 1
		 * - con1: wait 2 seconds
		 * - con2: update
		 * - con2: wait 3 seconds
		 * - con1: read query 2 (same query)
		 * - con1: commit read
		 * - con2: commit update
		 * - Expected output: First value: 2578180 Second value: 2578180
		 * - Read query 2 must not be blocked waiting on update.
		 * - Update IS blocked waiting for read.
		 */
		con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
	}

	public void setIsolationLevelTest5(Connection con) throws SQLException {
		// TODO: Set isolation level of connection so that the following
		/*
		 * Submitted sequence of operations:
		 * - con1: read query 1
		 * - con1: wait 2 seconds
		 * - con2: insert
		 * - con2: commit update
		 * - con1: read query 2 (same query)
		 * - con1: commit read
		 * - Expected output: First value: 2578180 Second value: 2578180
		 * - Read query 2 must not be blocked waiting on update.
		 * - Update IS blocked waiting for read.
		 */
		con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
	}

	/*
	 * Do not change anything below here.
	 */
	/**
	 * Converts a ResultSet to a string with a given number of rows displayed.
	 * Total rows are determined but only the first few are put into a string.
	 * 
	 * @param rst
	 *                ResultSet
	 * @param maxrows
	 *                maximum number of rows to display
	 * @return
	 *         String form of results
	 * @throws SQLException
	 *                      if a database error occurs
	 */
	public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
		StringBuffer buf = new StringBuffer(5000);
		int rowCount = 0;
		ResultSetMetaData meta = rst.getMetaData();
		buf.append("Total columns: " + meta.getColumnCount());
		buf.append('\n');
		if (meta.getColumnCount() > 0)
			buf.append(meta.getColumnName(1));
		for (int j = 2; j <= meta.getColumnCount(); j++)
			buf.append(", " + meta.getColumnName(j));
		buf.append('\n');

		while (rst.next()) {
			if (rowCount < maxrows) {
				for (int j = 0; j < meta.getColumnCount(); j++) {
					Object obj = rst.getObject(j + 1);
					buf.append(obj);
					if (j != meta.getColumnCount() - 1)
						buf.append(", ");
				}
				buf.append('\n');
			}
			rowCount++;
		}
		buf.append("Total results: " + rowCount);
		return buf.toString();
	}

	/**
	 * Reads a SQL script from a file and executes the commands.
	 * 
	 * @param fileName
	 *                 file name
	 * @throws Exception
	 *                   if a file I/O or database error occurs
	 */
	public void executeScript(String fileName) throws Exception {
		BufferedReader reader = null;
		StringBuilder sql = new StringBuilder();

		reader = new BufferedReader(new FileReader(new File(fileName)));

		while (reader.ready()) {
			String line = reader.readLine();
			if (line != null) {
				sql.append(line);
				sql.append("\n");
			}
		}

		// System.out.println("SQL to execute: ");
		// System.out.println(sql);

		if (reader != null)
			reader.close();

		Statement stmt = con.createStatement();
		stmt.execute(sql.toString());
		con.commit();
	}
}
