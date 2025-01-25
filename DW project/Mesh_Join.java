package project;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Scanner;

public class Mesh_Join {

    public static void main(String[] args) {
        String factFile = "E:/Downloads/DW project/transactions_data.csv";  // Fact table file path
        String outputFile = "E:/Downloads/DW project/output_data.csv";  // Output file path

        int windowSize = 5;  // Sliding window size for fact table
        Queue<String[]> slidingWindow = new LinkedList<>(); // Sliding window queue for fact table

        Map<Integer, String[]> dimensionTable = new HashMap<>(); // Hash map for product dimension table
        Map<Integer, String[]> customerTable = new HashMap<>(); // Hash map for customer dimension table

        Scanner scanner = new Scanner(System.in);

        // Ask the user for database credentials
        System.out.print("Enter database URL (e.g., jdbc:mysql://localhost:3306/Metro_DW): ");
        String dbUrl = scanner.nextLine();

        System.out.print("Enter database username: ");
        String dbUsername = scanner.nextLine();

        System.out.print("Enter database password: ");
        String dbPassword = scanner.nextLine();

        try {
            // Step 1: Load the dimension tables from SQL into memory
            loadProductDataFromSQL(dimensionTable, dbUrl, dbUsername, dbPassword);
            loadCustomerDataFromSQL(customerTable, dbUrl, dbUsername, dbPassword);

            // Step 2: Process the fact table with the sliding window
            processFactTable(factFile, slidingWindow, dimensionTable, customerTable, windowSize, outputFile, dbUrl, dbUsername, dbPassword);

        } catch (Exception e) {
            e.printStackTrace();  // Handle exceptions
        } finally {
            scanner.close();  // Close the scanner
        }
    }

    private static void loadProductDataFromSQL(Map<Integer, String[]> dimensionTable, String dbUrl, String dbUsername, String dbPassword) throws Exception {
        String query = "SELECT PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME FROM PRODUCTS_DIM";

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                int productID = rs.getInt("PRODUCT_ID");
                String[] productDetails = new String[] {
                        String.valueOf(productID),  // Product ID
                        rs.getString("PRODUCT_NAME"),  // Product Name
                        rs.getString("PRODUCT_PRICE"),  // Product Price
                        rs.getString("SUPPLIER_ID"),  // Supplier ID
                        rs.getString("SUPPLIER_NAME"),  // Supplier Name
                        rs.getString("STORE_ID"),  // Store ID
                        rs.getString("STORE_NAME")  // Store Name
                };
                dimensionTable.put(productID, productDetails);
            }
        }
    }

    private static void loadCustomerDataFromSQL(Map<Integer, String[]> customerTable, String dbUrl, String dbUsername, String dbPassword) throws Exception {
        String query = "SELECT CUSTOMER_ID, CUSTOMER_NAME, GENDER FROM CUSTOMERS_DIM";

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                int customerID = rs.getInt("CUSTOMER_ID");
                String[] customerDetails = new String[] {
                        String.valueOf(customerID),  // Customer ID
                        rs.getString("CUSTOMER_NAME"),  // Customer Name
                        rs.getString("GENDER")  // Gender
                };
                customerTable.put(customerID, customerDetails);
            }
        }
    }

    private static void processFactTable(String factFile, Queue<String[]> slidingWindow, Map<Integer, String[]> dimensionTable,
            Map<Integer, String[]> customerTable, int windowSize, String outputFile, String dbUrl, String dbUsername, String dbPassword) throws Exception {

try (BufferedReader factReader = new BufferedReader(new FileReader(factFile));
BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {

String factLine;

writer.write("CUSTOMER_ID,CUSTOMER_NAME,GENDER,PRODUCT_ID,PRODUCT_NAME,PRICE,SUPPLIER_ID,SUPPLIER_NAME,STORE_ID,STORE_NAME,ORDER_ID,ORDER_DATE,QUANTITY_ORDERED,TOTAL_SALES\n");

// Skip the header row of the fact file
factReader.readLine();

int recordCount = 0;

while ((factLine = factReader.readLine()) != null) {
if (factLine.trim().isEmpty()) {
continue;  // Skip empty lines
}

String[] factData = parseCSVLine(factLine);

// If sliding window exceeds window size, remove the oldest entry
if (slidingWindow.size() >= windowSize) {
slidingWindow.poll();
}

// Add the current row to the sliding window
slidingWindow.add(factData);

// Process each transaction within the sliding window
for (String[] transaction : slidingWindow) {
int productID = Integer.parseInt(transaction[2]);
int customerID = Integer.parseInt(transaction[4]);
double quantity = Double.parseDouble(transaction[3]);

String[] productDetails = dimensionTable.get(productID);
String[] customerDetails = customerTable.get(customerID);

if (productDetails != null && customerDetails != null) {
String priceString = productDetails[2].replaceAll("[^0-9.]", ""); // Remove non-numeric characters
double price = Double.parseDouble(priceString);

// Format the ORDER_DATE string to MySQL's DATETIME format
String orderDateString = transaction[1];
SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy H:mm");
SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
Date orderDate = inputFormat.parse(orderDateString);
String formattedOrderDate = outputFormat.format(orderDate);

double totalSales = price * quantity;

// Debugging output for joined data
printJoinedResult(transaction, productDetails, customerDetails, totalSales, formattedOrderDate);

// Insert into the SQL database
insertIntoSQL(transaction, productDetails, customerDetails, totalSales, formattedOrderDate, dbUrl, dbUsername, dbPassword);

// Write the processed data to the output file
writer.write(String.join(",", customerDetails[0], customerDetails[1], customerDetails[2],
                    productDetails[0], productDetails[1], productDetails[2],
                    productDetails[3], productDetails[4], productDetails[5],
                    productDetails[6], transaction[0], formattedOrderDate,
                    transaction[3], String.valueOf(totalSales)) + "\n");

recordCount++;
} else {
System.out.println("Skipping transaction due to missing data for Product ID: " 
+ transaction[2] + " or Customer ID: " + transaction[4]);
}
}

// Clear the sliding window after each iteration to reset the data for the next run
slidingWindow.clear();

System.out.println("Total records processed: " + recordCount); // Output final record count
}
}
}


    private static String[] parseCSVLine(String csvLine) {
        return csvLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    }

    private static void insertIntoSQL(String[] transaction, String[] productDetails, String[] customerDetails, double totalSales, String orderDate, String dbUrl, String dbUsername, String dbPassword) throws Exception {
        String query = "INSERT INTO FACT_TRANSACTION_OUTPUT (CUSTOMER_ID, CUSTOMER_NAME, GENDER, PRODUCT_ID, PRODUCT_NAME, PRICE, " +
                       "SUPPLIER_ID, SUPPLIER_NAME, STORE_ID, STORE_NAME, ORDER_ID, ORDER_DATE, QUANTITY_ORDERED, TOTAL_SALES) " +
                       "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setInt(1, Integer.parseInt(customerDetails[0]));
            stmt.setString(2, customerDetails[1]);
            stmt.setString(3, customerDetails[2]);
            stmt.setInt(4, Integer.parseInt(productDetails[0]));
            stmt.setString(5, productDetails[1]);
            stmt.setDouble(6, Double.parseDouble(productDetails[2]));
            stmt.setInt(7, Integer.parseInt(productDetails[3]));
            stmt.setString(8, productDetails[4]);
            stmt.setInt(9, Integer.parseInt(productDetails[5]));
            stmt.setString(10, productDetails[6]);
            stmt.setInt(11, Integer.parseInt(transaction[0]));
            stmt.setString(12, orderDate);  // Use the formatted order date
            stmt.setInt(13, Integer.parseInt(transaction[3]));
            stmt.setDouble(14, totalSales);

            // Print out the data being inserted
            System.out.println("Inserting Data: ");
            System.out.println("Customer ID: " + customerDetails[0] + ", Product ID: " + productDetails[0] + ", Order Date: " + orderDate);
            
            stmt.executeUpdate();
        }
    }

    private static void printJoinedResult(String[] transaction, String[] productDetails, String[] customerDetails, double totalSales, String orderDate) {
        System.out.println("Joined Result:");
        System.out.println("Customer ID: " + customerDetails[0]);
        System.out.println("Customer Name: " + customerDetails[1]);
        System.out.println("Gender: " + customerDetails[2]);
        System.out.println("Product ID: " + productDetails[0]);
        System.out.println("Product Name: " + productDetails[1]);
        System.out.println("Price: " + productDetails[2]);
        System.out.println("Supplier ID: " + productDetails[3]);
        System.out.println("Supplier Name: " + productDetails[4]);
        System.out.println("Store ID: " + productDetails[5]);
        System.out.println("Store Name: " + productDetails[6]);
        System.out.println("Order ID: " + transaction[0]);
        System.out.println("Order Date: " + orderDate);
        System.out.println("Quantity Ordered: " + transaction[3]);
        System.out.println("Total Sales: $" + totalSales);
        System.out.println("------------");
    }
}