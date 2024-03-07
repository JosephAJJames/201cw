import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

public class Code {

    String[] joesSQLStatments = {"DROP DATABASE IF EXISTS PremGames" ,"CREATE DATABASE PremGames;", "USE PremGames;",

            "CREATE TABLE Coaches(cID INT PRIMARY KEY NOT NULL, FirstName VARCHAR(25), LastName VARCHAR(25), NumberOfTrophies INT, HasTeam BOOLEAN)",

            "CREATE TABLE Tactics(tID INT PRIMARY KEY NOT NULL,cID INT , PosStyle VARCHAR(10), Formation VARCHAR(10), TacticName VARCHAR(25), FOREIGN KEY (cID) REFERENCES Coaches(cID));",

            "CREATE TABLE Teams(tID INT PRIMARY KEY NOT NULL, TeamName VARCHAR(25), Region VARCHAR(25), YearFounded INT, PlaysInUCL BOOLEAN, cID INT UNIQUE, Owner VARCHAR(30))",

            "CREATE TABLE Referees(rID INT PRIMARY KEY NOT NULL, FirstName VARCHAR(25), LastName VARCHAR(25), yCardsThisYear INT, rCardsThisYear INT)",

            "CREATE TABLE TeamMatchups(tmID INT PRIMARY KEY NOT NULL, HomeTeam INT NOT NULL, AwayTeam INT NOT NULL, FOREIGN KEY (HomeTeam) REFERENCES Teams(tID), FOREIGN KEY (AwayTeam) REFERENCES Teams(tID));",

            "CREATE TABLE Fixtures(fID INT PRIMARY KEY NOT NULL, KickOff DATETIME, WeatherConditions VARCHAR (25), HomeScore INT, AwayScore INT, rID INT, FOREIGN KEY (rID) REFERENCES Referees(rID), tmID Integer NOT NULL UNIQUE, FOREIGN KEY (tmID) REFERENCES TeamMatchups(tmID));",   //referee would be NOT NULL however, referees for future games have not yet been decided

            "CREATE TABLE Players(pID INT PRIMARY KEY NOT NULL, tID INT, Position VARCHAR(5), DOB DATE, HeightCM INT, ShirtNum INT, FOREIGN KEY (tID) REFERENCES Teams(tID));"

            };

    String[] dannysSqlStatments = {

        //"DROP DATABASE IF EXISTS PremBusiness", "CREATE DATABASE PremBusiness;", "USE PremBusiness;",

        
        //"CREATE TABLE Stadiums (sID INTEGER PRIMARY KEY NOT NULL, Capacity INTEGER NOT NULL, StadiumName VARCHAR(50) NOT NULL, YearBuilt INTEGER NOT NULL, PostCode VARCHAR(10) NOT NULL, IsActive BOOLEAN NOT NULL, AverageTicketSales INTEGER);",
        //"CREATE TABLE ShirtSponsors (ssID INTEGER PRIMARY KEY NOT NULL, ShirtSponsorName VARCHAR(50), CountryFounded VARCHAR(50), Owner VARCHAR(50), Website VARCHAR(50));",
        //"CREATE TABLE Teams (tID INTEGER PRIMARY KEY NOT NULL, TeamName VARCHAR(30), sID INTEGER, ssID INTEGER, YearFounded INTEGER, Website VARCHAR(60), FOREIGN KEY (sID) REFERENCES Stadiums(sID), FOREIGN KEY (ssID) REFERENCES ShirtSponsors(ssID));",
        //"CREATE TABLE Players(pID INT PRIMARY KEY NOT NULL, tID INT, StrongFoot VARCHAR(1), DOB DATE, WeightKG INT, ShirtNum INT, FOREIGN KEY (tID) REFERENCES Teams(tID));",
        //"CREATE TABLE Contracts (cID INTEGER PRIMARY KEY, pID INTEGER UNIQUE, ContractType VARCHAR(20), WeeklySalaryUSD DECIMAL, DateSigned DATE, ExpiryDate DATE, Active BOOLEAN, FOREIGN KEY (pID) REFERENCES Players(pID));",
        //"CREATE TABLE Injuries (iID INTEGER PRIMARY KEY, pID INTEGER NOT NULL, DateOfInjury DATE NOT NULL, DateOfRecovery DATE, TypeOfInjury VARCHAR(50), FOREIGN KEY (pID) REFERENCES Players(pID));",
        //"CREATE TABLE TeamMerchandise (mID INTEGER PRIMARY KEY, tID INTEGER, ProductName VARCHAR(30), PriceUSD DECIMAL, UnitsSold INTEGER, InStock BOOLEAN, DateOfNextShipment DATE, FOREIGN KEY (tID) REFERENCES Teams(tID));"
    };




    String[] scottsSQLStatments = {};

    public static void main(String[] args) throws SQLException {

        Code code = new Code();
        String url = "jdbc:mysql://localhost:3306/";
        System.out.println("Trying to connect....");
        Connection con = connect(url);

        if (con == null) {
            System.out.print("Issue with connecting to mySQL server\nProgram closing...");
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


                try { //inserting records
                    List<String[]> recordList = reader.returnRecords();

                    String SqlQuery = "INSERT INTO (";

                    for (String[] record: recordList) {
                        int x = 0;
                        
                        for (String valueInRecord : record) {

                            System.out.println("valueInRecord: " + valueInRecord);
                            switch (x) {
                                case 0:
                                    SqlQuery = SqlQuery + valueInRecord + ") " + "VALUES" + "(";
                                    break;
                            
                                default:
                                    if (code.isLastElementInArray(record, valueInRecord)) {
                                        SqlQuery = SqlQuery + ", " + valueInRecord + ")";
                                    } else {
                                        SqlQuery = SqlQuery + ", " + valueInRecord;
                                    }
                                    break;
                            }
                            x++;
                        }
                    }

                    System.out.println(SqlQuery);

                } catch (Exception e) {
                    System.out.println("Heavy");
                } finally {}
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

    public boolean isLastElementInArray(String[] array, String element)
    {
        return (array[array.length - 1].equals(element));
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

    public List<String[]> returnRecords()
    {
        return this.allRows;
    }
}
