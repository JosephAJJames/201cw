import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.io.BufferedReader;

public class Code {

    String[] joesSQLStatments = {};

    String[] dannysSqlStatments = {};

    String[] scottsSQLStatments = {};
    public static void main(String[] args) throws SQLException {

        Code code = new Code();
        String url = "jdbc:mysql://localhost:3306/";
        System.out.println("Trying to connect....");
        Connection con = connect(url);

        String[] csvArray = {"38639416.csv", "38790475.csv", "1234567.csv"};
        for (String filenumber: csvArray) {

            csvReader reader = new csvReader("/home/jamesj9/IdeaProjects/201cw/src/38639416.csv");

            String[][] statments = code.getSQLStatments();

            for (String[] statmentsSQL : statments) { //starting on each set of querys for each person








            }

        }

    }

    public String[][] getSQLStatments() {
        String[][] sqlStatements = {
                this.joesSQLStatments,
                this.dannysSqlStatments,
                this.scottsSQLStatments
        };

        return sqlStatements;
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
        try {
            bufferedReader = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }
}
