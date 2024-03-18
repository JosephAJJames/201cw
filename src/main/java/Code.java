import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

public class Code {

    String[] joesSQLStatments = {"DROP DATABASE IF EXISTS PremGames" ,"CREATE DATABASE PremGames;", "USE PremGames;", 

            "CREATE TABLE Coaches(cID INT PRIMARY KEY NOT NULL, FirstName VARCHAR(25), LastName VARCHAR(25), NumberOfTrophies INT, HasTeam BOOLEAN)", //parent table, //cant delete coach if they are being used in the teams table,
            
            "CREATE TABLE Stadiums(sID INT PRIMARY KEY NOT NULL, Name VARCHAR(25), Capacity INT NOT NULL, NoOfStands INT NOT NULL, tID INT UNIQUE)", //child of the Teams table, tid can be null as a stadium might not be in use

            "CREATE TABLE Teams(tID INT PRIMARY KEY NOT NULL, sID INT, TeamName VARCHAR(25), Region VARCHAR(25), YearFounded INT, PlaysInUCL BOOLEAN, cID INT UNIQUE, Owner VARCHAR(30), FOREIGN KEY (sID) REFERENCES Stadiums(sID) ON DELETE RESTRICT ,FOREIGN KEY (cID) REFERENCES Coaches(cID) ON DELETE RESTRICT)", //child table of coaches, can't delete a team if its in use in players

            "CREATE TABLE Referees(rID INT PRIMARY KEY NOT NULL, FirstName VARCHAR(25), LastName VARCHAR(25), yCardsThisYear INT, rCardsThisYear INT)", //parent table
            
            "CREATE TABLE Fixtures(fID INT PRIMARY KEY NOT NULL, KickOff DATETIME, WeatherConditions VARCHAR (25), HomeScore INT, AwayScore INT, rID INT, FOREIGN KEY (rID) REFERENCES Referees(rID) ON DELETE RESTRICT);", //parent table   //referee would be NOT NULL however, referees for future games have not yet been decided

            "CREATE TABLE TeamMatchups(tmID INT PRIMARY KEY NOT NULL, HomeTeam INT NOT NULL, AwayTeam INT NOT NULL, fID INT, FOREIGN KEY (HomeTeam) REFERENCES Teams(tID) ON DELETE RESTRICT, FOREIGN KEY (AwayTeam) REFERENCES Teams(tID) ON DELETE RESTRICT, FOREIGN KEY (fID) REFERENCES Fixtures(fID) ON DELETE RESTRICT);", //child table of teams

            "CREATE TABLE Players(pID INT PRIMARY KEY NOT NULL, tID INT, Position VARCHAR(5), DOB DATE, HeightCM INT, ShirtNum INT, FOREIGN KEY (tID) REFERENCES Teams(tID) ON DELETE RESTRICT);", //Parent table, can't delete a team thats being used

            };


    String[] joesSELECTQuerys = {
                                "SELECT Teams.Region, SUM(Stadiums.Capacity) AS TotalCapacity FROM Teams JOIN Stadiums ON Teams.sID = Stadiums.sID GROUP BY (Teams.Region) HAVING SUM(Stadiums.Capacity) > 0;",
                                };

    String[] joesDELETEQuerys = {"DELETE FROM Teams WHERE Teams.tID = 1;"};

    String[] dannysSELECTQuerys = {"SELECT * FROM Teams"};

    String[] scottsSELECTQuerys = {""};

    String[] dannysSqlStatments = {

        "DROP DATABASE IF EXISTS PremBusiness", "CREATE DATABASE PremBusiness;", "USE PremBusiness;",

        "CREATE TABLE Stadiums (sID INTEGER PRIMARY KEY NOT NULL, Capacity INTEGER NOT NULL CHECK (Capacity >= 0), StadiumName VARCHAR(50) NOT NULL, YearBuilt INTEGER NOT NULL CHECK (YearBuilt >= 1861), PostCode VARCHAR(10) NOT NULL, IsActive BOOLEAN NOT NULL, AverageTicketSales INTEGER);",

        "CREATE TABLE ShirtSponsors (ssID INTEGER PRIMARY KEY NOT NULL, ShirtSponsorName VARCHAR(50), CountryFounded VARCHAR(50), Owner VARCHAR(50), Website VARCHAR(50));",

        "CREATE TABLE Teams (tID INTEGER PRIMARY KEY NOT NULL, TeamName VARCHAR(30), sID INTEGER, ssID INTEGER, YearFounded INTEGER NOT NULL CHECK (YearFounded >= 1857), Website VARCHAR(60), FOREIGN KEY (sID) REFERENCES Stadiums(sID), FOREIGN KEY (ssID) REFERENCES ShirtSponsors(ssID));",

        "CREATE TABLE Players(pID INT PRIMARY KEY NOT NULL, tID INT, StrongFoot VARCHAR(1), DOB DATE, WeightKG INT, ShirtNum INT, FOREIGN KEY (tID) REFERENCES Teams(tID));",

        "CREATE TABLE Contracts (cID INTEGER PRIMARY KEY, pID INTEGER UNIQUE, ContractType VARCHAR(20), WeeklySalaryUSD DECIMAL, DateSigned DATE, ExpiryDate DATE, Active BOOLEAN, FOREIGN KEY (pID) REFERENCES Players(pID));",

        "CREATE TABLE Injuries (iID INTEGER PRIMARY KEY, pID INTEGER NOT NULL, DateOfInjury DATE NOT NULL, DateOfRecovery DATE, TypeOfInjury VARCHAR(50), FOREIGN KEY (pID) REFERENCES Players(pID));",

        "CREATE TABLE TeamMerchandise (mID INTEGER PRIMARY KEY, tID INTEGER, ProductName VARCHAR(30), PriceUSD DECIMAL, UnitsSold INTEGER, InStock BOOLEAN, DateOfNextShipment DATE, FOREIGN KEY (tID) REFERENCES Teams(tID));"
    };

    




    String[] scottsSQLStatments = {};

    public static void main(String[] args) throws SQLException {

        Code code = new Code();
        String url = "jdbc:mysql://localhost:3306/";
        Connection con = connect(url);

        if (con == null) { //was connection succesfull?
            System.out.print("Issue with connecting to mySQL server\nProgram closing...");
            System.exit(0); //close program, if server isnt connected then nothing else is going to work
        }

        String[] csvArray = {"/main/resources/38639416.csv", "/main/resources/38790475.csv"};
        for (String filenumber: csvArray) { //loop over the csv file names array

            CsvReader reader = new CsvReader("src/" + filenumber); //make new csv reader with current csv file
            String[] schema = {}; //will hold the sql statments to make current group members schema
            String[][] statements = code.getSQLStatments();

            switch (filenumber) { //whos schema are we making?
                case "38639416.csv":
                    schema = statements[0]; //make the schema array Joe's SQL statments
                    break;
                case "38790475.csv":
                    schema = statements[1]; //make the schema array Danny's SQL statments
                    break;
                default:
                    System.out.println("Scott you need to put all your shite in here please get coding soon");
            }

            code.constructSchema(schema, con);

            code.constructINSERTQuerys(reader, con, filenumber);

            code.performSELECTS(con, filenumber);

            code.performDeletions(con, filenumber);
        }
    }

    public void performDeletions(Connection con, String filename)
    {
        String[] queries = this.getDELETEQuerys(filename);
        for (String query : queries) {
            try {
                Statement statment = con.createStatement();
                Integer rows = statment.executeUpdate(query);
                System.out.println("Rows Deleted: " + rows.toString());
            } catch (Exception e) {
                System.out.println("An error occured: " + e.getMessage());
            }
        }
    }

    public String[] getDELETEQuerys(String filename)
    {
        if (filename.equals(new String("38639416.csv"))) {
            return this.joesDELETEQuerys;
        } else {
            return null;
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

    public String[] getSELECTQueries(String filename)
    {
        if (filename.equals(new String("38639416.csv"))) {
            return this.joesSELECTQuerys;
        } else if (filename.equals(new String("38790475.csv"))) {
            return this.dannysSELECTQuerys;
        } else {
            return this.scottsSELECTQuerys;
        }
    }


    public static Connection connect(String url) throws SQLException
    {
        System.out.println("Trying to connect....");
        try {
            Connection con = DriverManager.getConnection(url);
            System.out.println("Connected Succesfully");
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

    public void constructSchema(String[] schema, Connection con) //Builds the current DB schema
    {
        System.out.println("Making Schema....."); 
        try {
            Statement statment = con.createStatement();

            for (String query: schema) {

                String SQLQuery = query;

                statment.executeUpdate(SQLQuery);

                System.out.println(SQLQuery + " this statement has been executed");

            }
        } catch (Exception e) {
                e.printStackTrace();
        }
        System.out.println("Schema has been constructed");   
    }

    public void constructINSERTQuerys(CsvReader reader, Connection con, String filename) //reads from the csv and creates INSERT INTO Querys
    {
        try { //inserting records
            List<String[]> recordList = reader.returnRecords();

 

            for (String[] record: recordList) {
                ArrayList<String> valuesToInsert = new ArrayList<String>();
                String tableName = "";
                int x = 0;
                for (String valueInRecord : record) {
                    if (x == 0) {   //first element of a record will always be table name
                        tableName = valueInRecord; 
                        System.out.println(tableName);
                    }

                    valuesToInsert.add(valueInRecord);
                
                    x++;
                }

                switch (filename) {
                    case "38639416.csv":
                        this.makeJoesINSERTS(tableName, valuesToInsert, con);
                        break;
                    
                    case "38790475.csv":
                        this.makeDannysINSERTS(tableName, valuesToInsert, con);
                        break;
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {}
    }

    public static PreparedStatement statetmentBuilder(PreparedStatement prepState, ArrayList<String> valuesToInsert)throws SQLException{ 
        for (int x = 1; x < valuesToInsert.size(); x++) {
            prepState.setString(x, valuesToInsert.get(x));
        }
        return prepState;
    }

    public void makeDannysINSERTS(String tableName, ArrayList<String> valuesToInsert, Connection con) throws SQLException
    {
        String sql = "";
        PreparedStatement statement = con.prepareStatement(" ");
        switch (tableName) {
            case "Stadiums":
                sql = "INSERT INTO  " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?)";
                break;
        
            case "ShirtSponsors":
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
                break;

            case "Teams":
                sql = "INSERT INTO " + tableName + " VALUES(? ,?, ?, ?, ?, ?, ?, ?)";
                break;
            
            case "Players":
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?)";
                break;
            
            case "Contracts":
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?)";
                break;

            case "Injuries":
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
                break;

            case "TeamMerchandise":
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?)";
                break;
        }
        statement = con.prepareStatement(sql);
        statetmentBuilder(statement, valuesToInsert);
        Integer rowsAffected = statement.executeUpdate();
        System.out.println("Number of rows affected: " + rowsAffected.toString());

    }

    public void makeJoesINSERTS(String tableName, ArrayList<String> valuesToInsert, Connection con) throws SQLException
    {
        String sql = "";
        PreparedStatement statement = con.prepareStatement(" ");
        switch (tableName) {
            case "Coaches":
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
                break;
            case "Referees":
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
                break;
            case "Teams":
                sql = "INSERT INTO " + tableName + " VALUES(? ,?, ?, ?, ?, ?, ?, ?)";            
                break;
            case "Fixtures":
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?)";
                break;
            case "Players":
            for (int x = 0; x < valuesToInsert.size(); x++) {
                System.out.println("value: " + valuesToInsert.get(x));
            }
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?)";
                break;
            case "Stadiums":
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
                break;
            case "TeamMatchups":
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?)";
                break; 
        }
        statement = con.prepareStatement(sql);
        statetmentBuilder(statement, valuesToInsert);
        Integer rowsAffected = statement.executeUpdate();
        System.out.println("Number of rows affeced: " + rowsAffected.toString());
    }


    public void performSELECTS(Connection con, String filename) throws SQLException
    {
        String[] querys = this.getSELECTQueries(filename);
        Statement currentStatment = con.createStatement();
        for (String currentQuery: querys) {
            ResultSet rSet = currentStatment.executeQuery(currentQuery);
            printResultSet(rSet);
        }
    }

    public static void printResultSet(ResultSet rs) {
        try {
            
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();

            
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) System.out.print(",  ");
                System.out.print(rsmd.getColumnName(i));
            }
            System.out.println();

            
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) System.out.print(",  ");
                    System.out.print(rs.getString(i));
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
        this.readInFile();
        return this.allRows;
    }
}
