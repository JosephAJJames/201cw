import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

public class Code {

    String[] joesSQLStatments = {"CREATE DATABASE LOTSOFTABLESS;", "USE LOTSOFTABLESS;"};

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

        System.out.println("Connected Succesfully");

        String[] csvArray = {"38639416.csv"};
        for (String filenumber: csvArray) {


            CsvReader reader = new CsvReader("src/38639416.csv");

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
                    //System.out.println("finished");
                }
            }

        }

    }

    public String[][] getSQLStatments()
    {

        return new String[][]{
                this.joesSQLStatments,
                this.dannysSqlStatments,
                this.scottsSQLStatments
        };
    }


    public static Connection connect(String url) throws SQLException
    {
            try {
                Connection con = DriverManager.getConnection(url);
                return con;
            } catch (Exception e) {
                System.out.println("Failed to connect...:\n"+ e);
            }
            return null;
    }

}

class CsvReader {
    BufferedReader bufferedReader;

    List<String[]> allRows = new ArrayList<>();

    public CsvReader(String fileName)
    {
        try {
            bufferedReader = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        CsvReader reader = new CsvReader("src/38639416.csv");
    }


    public void readInFile()
    {
        String line;
        try {
            while ((line = bufferedReader.readLine()) != null) {
                
                String[] row = line.split(",");
                allRows.add(row);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
