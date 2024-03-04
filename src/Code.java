import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

public class Code {

    String[] joesSQLStatments = {"DROP DATABASE PremGames;" , "CREATE DATABASE PremGames;", "USE PremGames;",

            "CREATE TABLE Tactics(tID INT PRIMARY KEY NOT NULL,cID INT , PosStyle VARCHAR(10), Formation VARCHAR(10), TacticName VARCHAR(25), FOREIGN KEY cID REFERENCES Coaches(cID));",

            "CREATE TABLE Coaches(cID INT PRIMARY KEY NOT NULL, FirstName VARCHAR(25), LastName VARCHAR(25), NumberOfTrophies INT, HasTeam BOOLEAN)",

            "CREATE TABLE Referees(rID INT PRIMARY KEY NOT NULL, FirstName VARCHAR(25), LastName VARCHAR(25), yCardsThisYear INT, rCardsThisYear INT)",

            "CREATE TABLE Fixtures(fID INT PRIMARY KEY NOT NULL, KickOff DATETIME, WeatherConditions VARCHAR (25), HomeScore INT, AwayScore INT, rID INT, FOREIGN KEY rID REFERENCES Referees(rID), tmID Integer NOT NULL UNIQUE, FOREIGN KEY tmID REFERENCES TeamMatchups(tmID))",   //referee would be NOT NULL however, referees for future games have not yet been decided

            "CREATE TABLE Teams(tID INT PRIMARY KEY NOT NULL, TeamName VARCHAR(25), Region VARCHAR(25), YearFounded INT, PlaysInUCL BOOLEAN, cID INT UNIQUE, Owner VARCHAR(30))",

            "CREATE TABLE TeamMatchups(tmID INT PRIMARY KEY NOT NULL, HomeTeam INT NOT NULL, AwayTeam INT NOT NULL, FOREIGN KEY HomeTeam REFERENCES Teams(tID), FOREIGN KEY AwayTeam REFERENCES Teams(tID));",

            "CREATE TABLE Players(pID INT PRIMARY KEY NOT NULL, tID INT, Position VARCHAR(5),DOB (DATE) , HeightCM INT), ShirtNum INT, FOREIGN KEY tID REFERENCES Teams(tID);",

            "DROP DATABSE PremGames"};

    String[] dannysSqlStatments = {"DROP DATABASE PremBusiness" ,"CREATE DATABASE PremBusiness;", "USE PremBusiness;", "DROP DATABASE PremBusiness"};


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
