package project;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBconnection {

    // Method to establish and return a database connection
    public static Connection getConnection() {
        try {
            String url = "jdbc:mysql://localhost:3306/Metro_DW"; // Replace with your database name
            String user = "root"; // Replace with your MySQL username
            String password = "password"; // Replace with your MySQL password
            return DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
            return null; // Return null if the connection fails
        }
    }

    // Main method to test the database connection
    public static void main(String[] args) {
        Connection connection = getConnection();
        if (connection != null) {
            System.out.println("Database connection established successfully!");
        } else {
            System.out.println("Failed to connect to the database.");
        }
    }
}


