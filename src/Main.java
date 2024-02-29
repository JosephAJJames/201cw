import java.sql.*;

public class Main {
        public static void main(String[] args) throws SQLException, ClassNotFoundException {
        System.out.println("Trying to connect....");
        String url = "jdbc:mysql://localhost:3306/";
        String username = "YES";
        String password = "YES";

        Class.forName("com.mysql.cj.jdbc.Driver");

        connect(url, username, password);
    }

    public static void connect(String url, String username, String password) throws SQLException {
        Connection con = DriverManager.getConnection(url, username, password);
    }
}