ReadMe for Transaction Processing Project

Overview
This project processes transaction data from a CSV file (transactions_data.csv), loads product and customer data from a MySQL database, and inserts the processed transaction data back into the FACT_TRANSACTION_OUTPUT table in the database. The program uses a sliding window technique to process and manage the fact data, joining it with product and customer dimension tables to enrich the information.

Prerequisites
Before running the program, ensure the following:

Java Development Kit (JDK): Ensure that JDK 8 or higher is installed.

MySQL Database: Ensure you have a MySQL database set up with the following tables:
PRODUCTS_DIM: Contains product details such as PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, etc.
CUSTOMERS_DIM: Contains customer details like CUSTOMER_ID, CUSTOMER_NAME, GENDER, etc.
FACT_TRANSACTION_OUTPUT: The fact table where the transaction data will be inserted.

MySQL JDBC Driver: Ensure that the MySQL JDBC driver (mysql-connector-java.jar) is added to your project classpath for connecting to the MySQL database.

CSV Files: You should have a CSV file named transactions_data.csv that contains transaction records, which will be processed.


Steps to Set Up and Run the Project

1. Configure Your MySQL Database
Set up MySQL Database: Ensure that the Metro_DW database is set up in your MySQL instance.
Example schema:
PRODUCTS_DIM: PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME
CUSTOMERS_DIM: CUSTOMER_ID, CUSTOMER_NAME, GENDER
FACT_TRANSACTION_OUTPUT: CUSTOMER_ID, CUSTOMER_NAME, GENDER, PRODUCT_ID, PRODUCT_NAME, PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME, ORDER_ID, ORDER_DATE, QUANTITY_ORDERED, TOTAL_SALES

2. Prepare the CSV Files
Ensure the CSV file transactions_data.csv is present at the specified location (E:/Downloads/DW project/transactions_data.csv or update the path as necessary).
The CSV file should contain the following columns:
ORDER_ID, ORDER_DATE, PRODUCT_ID, CUSTOMER_ID, QUANTITY_ORDERED

3. Compile the Java Code
Compile the Java code using your IDE (such as Eclipse or IntelliJ) or the command line using javac.
If using the command line, run:
javac -cp ".;path_to_mysql_connector.jar" Transaction.java

4. Run the Program
Open a command prompt or terminal window.
Navigate to the directory containing the compiled Transaction.class.
Run the Java program:
java -cp ".;path_to_mysql_connector.jar" project.Transaction

5. Enter Database Credentials
When prompted, enter your database credentials:
Enter database URL (e.g., jdbc:mysql://localhost:3306/Metro_DW): jdbc:mysql://localhost:3306/Metro_DW
Enter database username: root
Enter database password: password

6. Process the Data
The program will:
Load product and customer data from the MySQL database.
Process the transactions data from the transactions_data.csv file.
Aggregate product, customer, and transaction data into a final processed dataset.
Insert the processed records into the FACT_TRANSACTION_OUTPUT table in your database.

7. Output
The program will output:
A CSV file (output_data.csv) containing the processed data in the format:
CUSTOMER_ID, CUSTOMER_NAME, GENDER, PRODUCT_ID, PRODUCT_NAME, PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME, ORDER_ID, ORDER_DATE, QUANTITY_ORDERED, TOTAL_SALES
Debugging information, such as which records were processed and inserted, will be printed to the console.
You can also check your MySQL database to confirm that the FACT_TRANSACTION_OUTPUT table has been populated with the data.

8. Troubleshooting
Missing Data: If the program encounters missing product or customer details for certain transactions, those transactions will be skipped with a message indicating the missing data.
Database Connectivity: Ensure your MySQL server is running, and that the credentials provided are correct. Check the database logs for additional error details if necessary.

Conclusion
Once the program completes, the processed data will be inserted into the MySQL database, and the output file will contain the processed transactions. This process helps with enriching transaction data and ensuring that it's correctly joined with product and customer dimension data for analysis.

Customization Options
Change File Paths: If your files are located elsewhere, change the factFile and outputFile paths accordingly.
Adjust Window Size: Modify the windowSize variable to change how many rows are processed in each sliding window