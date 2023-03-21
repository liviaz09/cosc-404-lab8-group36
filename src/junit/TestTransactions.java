package junit;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

import trans.DatabaseTransactions;

/**
 * Tests database transactions and isolation levels using Microsoft SQL Server.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestTransactions {
	/**
	 * Class being tested
	 */
	private static DatabaseTransactions dt;

	/**
	 * Requests a connection to the server.
	 * 
	 * @throws Exception
	 *                   if an error occurs
	 */
	@BeforeAll
	public static void init() throws Exception {
		dt = new DatabaseTransactions();
		dt.connect();
	}

	/**
	 * Tests read balance with a valid account id.
	 */
	@Test
	@Order(1)
	public void testReadBalanceValidId() throws Exception {
		dt.load();

		String response = dt.queryBalance(5);

		assertEquals("Balance for account 5: 100", response);
	}

	/**
	 * Tests read balance with a invalid account id.
	 */
	@Test
	@Order(2)
	public void testReadBalanceInvalidId() throws Exception {
		dt.load();

		String response = dt.queryBalance(150);

		assertEquals("java.sql.SQLException: Account 150 not found.", response);
	}

	/**
	 * Tests transfer between accounts with valid data.
	 */
	@Test
	@Order(3)
	public void testTransferValid() throws Exception {
		dt.load();

		String response = dt.doTransfer(100, 7, 107);

		assertEquals("Transferred 100 from account 7 to 107", response);

		// Verify transfer
		assertEquals("Balance for account 7: 1000", dt.queryBalance(7));
		assertEquals("Balance for account 107: 1200", dt.queryBalance(107));
	}

	/**
	 * Tests transfer between accounts with invalid amount (insufficient funds).
	 */
	@Test
	@Order(4)
	public void testTransferInvalidAmount() throws Exception {
		dt.load();

		String response = dt.doTransfer(1000, 8, 108);

		assertEquals("java.sql.SQLException: Transfer failed because of insufficient balance.", response);
	}

	/**
	 * Tests transfer between accounts with invalid accounts that belong to
	 * different customers.
	 */
	@Test
	@Order(5)
	public void testTransferInvalidAccountsDifferentCustomer() throws Exception {
		dt.load();

		String response = dt.doTransfer(1000, 17, 119);

		assertEquals("java.sql.SQLException: Transfer failed because accounts do not belong to same customer.",
				response);
	}

	/**
	 * Tests isolation level for pair of connections (transactions).
	 */
	@Test
	@Order(6)
	public void testIsolationLevel1() throws Exception {
		dt.load();

		Connection con1 = dt.connect();
		Connection con2 = dt.connect();

		// Request to set isolation level for connection 1
		dt.setIsolationLevelTest1(con1);

		// Start transaction to get totals and update an account balance at the same
		// time
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
		StringBuffer firstCommitted = new StringBuffer();
		GetTotals gt = new GetTotals(con1, firstCommitted);
		Thread totalsThread = new Thread(gt);
		totalsThread.start();

		// Perform a pause to help guarantee first query is performed before update
		Thread.sleep(500);

		dt.updateAccountBalance(con2, 115, 3000000, 3000);
		if (firstCommitted.length() == 0)
			firstCommitted.append("UPDATE"); // Update connection, con2, committed first

		// Wait for threads to complete before looking at their output
		totalsThread.join();

		// Check for correct values
		int[] values = gt.values;
		System.out.println("First value: " + values[0] + " Second value: " + values[1]);
		assertEquals(2578180, values[0]);
		assertEquals(5577160, values[1]);

		// Check who was committed first
		assertEquals("QUERY", firstCommitted.toString());
	}

	/**
	 * Tests isolation level for pair of connections (transactions).
	 */
	@Test
	@Order(7)
	public void testIsolationLevel2() throws Exception {
		dt.load();

		Connection con1 = dt.connect();
		Connection con2 = dt.connect();

		// Request to set isolation levels
		dt.setIsolationLevelTest2(con1);

		// Start transaction to get totals and update an account balance at the same
		// time
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
		StringBuffer firstCommitted = new StringBuffer();
		GetTotals gt = new GetTotals(con1, firstCommitted);
		Thread totalsThread = new Thread(gt);
		totalsThread.start();

		// Perform a pause to help guarantee first query is performed before update
		Thread.sleep(500);

		dt.updateAccountBalance(con2, 115, 3000000, 10);
		if (firstCommitted.length() == 0)
			firstCommitted.append("UPDATE"); // Update connection, con2, committed first

		// Wait for threads to complete before looking at their output
		totalsThread.join();

		// Check for correct values
		int[] values = gt.values;
		System.out.println("First value: " + values[0] + " Second value: " + values[1]);
		assertEquals(2578180, values[0]);
		assertEquals(2578180, values[1]);

		// Check who was committed first
		assertEquals("UPDATE", firstCommitted.toString());
	}

	/**
	 * Tests isolation level for pair of connections (transactions).
	 */
	@Test
	@Order(8)
	public void testIsolationLevel3() throws Exception {
		dt.load();

		Connection con1 = dt.connect();
		Connection con2 = dt.connect();

		// Request to set isolation levels
		dt.setIsolationLevelTest3(con1);

		// Start transaction to get totals and update an account balance at the same
		// time
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
		StringBuffer firstCommitted = new StringBuffer();
		GetTotals gt = new GetTotals(con1, firstCommitted);
		Thread totalsThread = new Thread(gt);
		totalsThread.start();

		// Perform a pause to help guarantee first query is performed before update
		Thread.sleep(500);

		dt.updateAccountBalance(con2, 115, 3000000, 10);
		if (firstCommitted.length() == 0)
			firstCommitted.append("UPDATE"); // Update connection, con2, committed first

		// Wait for threads to complete before looking at their output
		totalsThread.join();

		// Check for correct values
		int[] values = gt.values;
		System.out.println("First value: " + values[0] + " Second value: " + values[1]);
		assertEquals(2578180, values[0]);
		assertEquals(5577160, values[1]);

		// Check who was committed first
		assertEquals("UPDATE", firstCommitted.toString());
	}

	/**
	 * Tests isolation level for pair of connections (transactions).
	 */
	@Test
	@Order(9)
	public void testIsolationLevel4() throws Exception {
		dt.load();

		Connection con1 = dt.connect();
		Connection con2 = dt.connect();

		// Request to set isolation levels
		dt.setIsolationLevelTest4(con1);

		// Start transaction to get totals and update an account balance at the same
		// time
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
		StringBuffer firstCommitted = new StringBuffer();
		GetTotals gt = new GetTotals(con1, firstCommitted);
		Thread totalsThread = new Thread(gt);
		totalsThread.start();

		// Perform a pause to help guarantee first query is performed before update
		Thread.sleep(500);

		dt.updateAccountBalance(con2, 115, 3000000, 10);
		if (firstCommitted.length() == 0)
			firstCommitted.append("UPDATE"); // Update connection, con2, committed first

		// Wait for threads to complete before looking at their output
		totalsThread.join();

		// Check for correct values
		int[] values = gt.values;
		System.out.println("First value: " + values[0] + " Second value: " + values[1]);
		assertEquals(2578180, values[0]);
		assertEquals(2578180, values[1]);

		// Check who was committed first
		assertEquals("QUERY", firstCommitted.toString());
	}

	/**
	 * Tests isolation level for pair of connections (transactions).
	 */
	@Test
	@Order(10)
	public void testIsolationLevel5() throws Exception {
		dt.load();

		Connection con1 = dt.connect();
		Connection con2 = dt.connect();

		// Request to set isolation levels
		dt.setIsolationLevelTest5(con1);

		// Start transaction to get totals and update an account balance at the same
		// time
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
		StringBuffer firstCommitted = new StringBuffer();
		GetTotals gt = new GetTotals(con1, firstCommitted);
		Thread totalsThread = new Thread(gt);
		totalsThread.start();

		// Create an account
		Thread.sleep(250);
		dt.createAccount(con2, 10, 1, 25, 9000000);
		if (firstCommitted.length() == 0)
			firstCommitted.append("UPDATE"); // Update connection, con2, committed first

		// Wait for threads to complete before looking at their output
		totalsThread.join();

		// Check for correct values
		int[] values = gt.values;
		System.out.println("First value: " + values[0] + " Second value: " + values[1]);
		assertEquals(2578180, values[0]);
		assertEquals(2578180, values[1]);

		// Check who was committed first
		assertEquals("QUERY", firstCommitted.toString());
	}

	/**
	 * A thread to perform query to retrieve account totals twice.
	 */
	public class GetTotals implements Runnable {
		/**
		 * JDBC connection to use
		 */
		private Connection con;

		/**
		 * Flag for determining commit order
		 */
		private StringBuffer firstCommitted;

		/**
		 * Account balances returned
		 */
		public int[] values;

		/**
		 * Constructor.
		 * 
		 * @param con
		 *                       JDBC connection
		 * @param firstCommitted
		 *                       for determining commit order
		 */
		public GetTotals(Connection con, StringBuffer firstCommitted) {
			this.con = con;
			this.firstCommitted = firstCommitted;
		}

		@Override
		public void run() {
			try {
				values = dt.getTotals(con);
				if (firstCommitted.length() == 0)
					firstCommitted.append("QUERY");
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
