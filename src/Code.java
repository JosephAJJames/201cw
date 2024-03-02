import java.io.FileReader;
import java.sql.*;
import java.io.BufferedReader;

public class Code {
        public static void main(String[] args) throws SQLException, ClassNotFoundException {
        System.out.println("Trying to connect....");
        String url = "jdbc:mysql://localhost:3306/";


        csvReader reader = new csvReader("38639416");

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

class csvReader {
    BufferedReader bufferedReader;

    public csvReader(String fileName)
    {
        bufferedReader = new BufferedReader(new FileReader());
    }
}
