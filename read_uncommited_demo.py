import mysql.connector
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from datetime import datetime
import os


# Connection settings
HOST = 'localhost'
USER = 'root'
PASSWORD = 'qwerty1234!'
DATABASE = 'homework'


def create_connection():
    try:
        connection = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error: {e}")
    return None


def read_uncommited_demo():
    """
    Shows how READ UNCOMMITED isolation level works.
    Shows dirty read.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Read Uncommitted
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='READ UNCOMMITTED')
        cursor1.execute("UPDATE accounts SET balance = 1000 WHERE name = 'Alice'")

        # Transaction 2: Read Uncommitted
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='READ UNCOMMITTED')
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_dirty_read = cursor2.fetchone()[0]

        print(f"Dirty Read (READ UNCOMMITTED): Alice's balance = {balance_dirty_read}")

        print(f"Transaction 1 rollback(): {datetime.now()}")
        connection1.rollback()

        print(f"Transaction 2 commit(): {datetime.now()}")
        connection2.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()


def read_committed():
    """
    Shows how READ COMMITTED isolation level works.
    Prevents dirty reads.
    Non
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Read Committed
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='READ COMMITTED')
        cursor1.execute("UPDATE accounts SET balance = 9999 WHERE name = 'Alice'")

        # Transaction 2: Read Committed
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_read_committed = cursor2.fetchone()[0]

        print(f"Read Committed: Alice's balance = {balance_read_committed}")

        print(f"Transaction 1 rollback(): {datetime.now()}")
        connection1.rollback()

        print(f"Transaction 2 commit(): {datetime.now()}")
        connection2.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()

def repeatable_read():
    """
    Shows how REPEATABLE READ isolation level works.
    Prevents non-repeatable reads.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Repeatable Read
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='REPEATABLE READ')
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_before_update = cursor1.fetchone()[0]
        print(f"Initial balance read in Transaction 1: {balance_before_update}")

        # Transaction 2: Update Alice's balance
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        cursor2.execute("UPDATE accounts SET balance = 4444 WHERE name = 'Alice'")
        connection2.commit()
        print("Transaction 2 committed.")

        # Transaction 1 reads Alice's balance again
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_repeatable_read = cursor1.fetchone()[0]

        print(f"Repeatable Read: Alice's balance before and after update = {balance_repeatable_read}")

        print(f"Transaction 1 commit(): {datetime.now()}")
        connection1.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()


def non_repeatable_read():
    """
    Shows how Non-Repeatable Read works.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1: Read Committed
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction(isolation_level='READ COMMITTED')
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_before_update = cursor1.fetchone()[0]
        print(f"Initial balance read in Transaction 1: {balance_before_update}")

        # Transaction 2: Update Alice's balance
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction(isolation_level='READ COMMITTED')
        cursor2.execute("UPDATE accounts SET balance = 5000 WHERE name = 'Alice'")
        connection2.commit()
        print("Transaction 2 committed.")

        # Transaction 1 reads Alice's balance again
        cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
        balance_after_update = cursor1.fetchone()[0]

        print(f"Non-Repeatable Read: Alice's balance before and after update = {balance_before_update} -> {balance_after_update}")

        print(f"Transaction 1 commit(): {datetime.now()}")
        connection1.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()


def deadlock():
    """
    Demonstrates deadlock.
    Transaction 1 and 2 will try to update each other's rows and deadlock.
    :return: void
    """
    connection1 = create_connection()
    connection2 = create_connection()

    try:
        cursor1 = connection1.cursor()
        cursor2 = connection2.cursor()

        # Transaction 1 locks row 1
        print(f"Transaction 1 started: {datetime.now()}")
        connection1.start_transaction()
        cursor1.execute("UPDATE accounts SET balance = 1111 WHERE name = 'Alice'")

        # Transaction 2 locks row 2
        print(f"Transaction 2 started: {datetime.now()}")
        connection2.start_transaction()
        cursor2.execute("UPDATE accounts SET balance = 2222 WHERE name = 'Bob'")

        # Now, both transactions try to lock the other's row
        print("Transaction 1 attempts to update Bob's balance.")
        cursor1.execute("UPDATE accounts SET balance = 6666 WHERE name = 'Bob'")

        print("Transaction 2 attempts to update Alice's balance.")
        cursor2.execute("UPDATE accounts SET balance = 5555 WHERE name = 'Alice'")

        connection1.commit()
        connection2.commit()

    except Error as e:
        print(f"Error: {e}")
        print("Deadlock detected!")
    finally:
        if cursor1:
            cursor1.close()
        if connection1 and connection1.is_connected():
            connection1.close()
        if cursor2:
            cursor2.close()
        if connection2 and connection2.is_connected():
            connection2.close()


if __name__ == "__main__":
    print("READ_UNCOMMITTED")
    read_uncommited_demo()

    print("\nREAD_COMMITTED")
    read_committed()

    print("\nREPEATABLE READ")
    repeatable_read()

    print("\nNON REPEATABLE READ")
    non_repeatable_read()

    print("\nDEADLOCK")
    deadlock()
