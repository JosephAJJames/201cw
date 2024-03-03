import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.io.BufferedReader;

public class Code {

    String[] joesSQLStatments = {"CREATE DATABASE SCC210;", "USE SCC201;", "SHOW TABLES;"};

    String[] dannysSqlStatments = {};

    String[] scottsSQLStatments = {};
    public static void main(String[] args) throws SQLException {

        Code code = new Code();
        String url = "jdbc:mysql://localhost:3306/";
        System.out.println("Trying to connect....");
        Connection con = connect(url);

        if (con == null) {
            System.exit(0);
        }

        String[] csvArray = {"38639416.csv", "38790475.csv", "1234567.csv"};
        for (String filenumber: csvArray) {

            csvReader reader = new csvReader("/home/jamesj9/IdeaProjects/201cw/src/38639416.csv");

            String[][] statments = code.getSQLStatments();

            for (String[] statmentsSQL : statments) { //starting on each set of querys for each person
                try {
                    Statement statment = con.createStatement();

                    for (String query: statmentsSQL) {

                        String SQLQuery = query;
                        statment.executeUpdate(SQLQuery);

                        System.out.println(SQLQuery + "this statement has been executed");

                    }


                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("finished");
                }
            }

        }

    }

    public String[][] getSQLStatments() {

        return new String[][]{
                this.joesSQLStatments,
                this.dannysSqlStatments,
                this.scottsSQLStatments
        };
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
