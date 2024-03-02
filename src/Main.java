import java.sql.*;

public class Main {
        public static void main(String[] args) throws SQLException, ClassNotFoundException {
        System.out.println("Trying to connect....");
        String url = "jdbc:mysql://localhost:3306/";

        Connection con = connect(url);
    }

    public static Connection connect(String url) throws SQLException {
            try {
                Connection con = DriverManager.getConnection(url);
                return con;
            } catch (Exception e) {
                System.out.println("Failed to connect...:\n"+ e);
            }
            return null;
    }
}