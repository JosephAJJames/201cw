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

            "CREATE TABLE Teams(tID INT PRIMARY KEY NOT NULL, sID INT NOT NULL, TeamName VARCHAR(25), Region VARCHAR(25), YearFounded INT, PlaysInUCL BOOLEAN, cID INT NOT NULL UNIQUE, Owner VARCHAR(30), FOREIGN KEY (sID) REFERENCES Stadiums(sID) ON DELETE RESTRICT ,FOREIGN KEY (cID) REFERENCES Coaches(cID) ON DELETE RESTRICT)", //child table of coaches, can't delete a team if its in use in players

            "CREATE TABLE Referees(rID INT PRIMARY KEY NOT NULL, FirstName VARCHAR(25), LastName VARCHAR(25), yCardsThisYear INT, rCardsThisYear INT)", //parent table

            "CREATE TABLE TeamMatchups(tmID INT PRIMARY KEY NOT NULL, Team1 INT NOT NULL, Team2 INT NOT NULL, FOREIGN KEY (Team1) REFERENCES Teams(tID) ON DELETE RESTRICT, FOREIGN KEY (Team2) REFERENCES Teams(tID) ON DELETE RESTRICT);", //child table of teams
            
            "CREATE TABLE Fixtures(fID INT PRIMARY KEY NOT NULL, tmID INT NOT NULL , KickOff DATETIME, WeatherConditions VARCHAR (25), Team1Score INT, Team2Score INT, rID INT NOT NULL, sID INT NOT NULL, FOREIGN KEY (rID) REFERENCES Referees(rID) ON DELETE RESTRICT, FOREIGN KEY (tmID) REFERENCES TeamMatchups (tmID) ON DELETE RESTRICT, FOREIGN KEY (sID) REFERENCES Stadiums(sID) ON DELETE RESTRICT);", //parent table   //referee would be NOT NULL however, referees for future games have not yet been decided

            "CREATE TABLE Players(pID INT PRIMARY KEY NOT NULL, tID INT, Position VARCHAR(5), DOB DATE, HeightCM INT, ShirtNum INT, FOREIGN KEY (tID) REFERENCES Teams(tID) ON DELETE RESTRICT);", //Parent table, can't delete a team thats being used

            };


    String[] joesSELECTQuerys = {
                                "SELECT Teams.Region, SUM(Stadiums.Capacity) AS TotalCapacity FROM Teams JOIN Stadiums ON Teams.sID = Stadiums.sID GROUP BY (Teams.Region) HAVING SUM(Stadiums.Capacity) > 0;"
    };

    String[] joesDELETEQuerys = {"DELETE FROM Teams WHERE Teams.tID = 1;",
                                "DELETE FROM Stadiums WHERE Stadiums.sID = 1;"};

    String[] dannysSELECTQuerys = {"SELECT TeamName FROM Teams WHERE tID = 1"};


    String[] dannysSqlStatments = {

        "DROP DATABASE IF EXISTS PremBusiness", "CREATE DATABASE PremBusiness;", "USE PremBusiness;",

        "CREATE TABLE Stadiums (sID INTEGER PRIMARY KEY NOT NULL, Capacity INTEGER NOT NULL CHECK (Capacity >= 0), StadiumName VARCHAR(50) NOT NULL, YearBuilt INTEGER NOT NULL CHECK (YearBuilt >= 1861), PostCode VARCHAR(10) NOT NULL, IsActive BOOLEAN NOT NULL, AverageTicketSales INT);",

        "CREATE TABLE ShirtSponsors (ssID INTEGER PRIMARY KEY NOT NULL, ShirtSponsorName VARCHAR(50), NationOfCompany VARCHAR(25), Owner VARCHAR(50), Website VARCHAR(50));",

        "CREATE TABLE Teams (tID INTEGER PRIMARY KEY NOT NULL, TeamName VARCHAR(30), sID INTEGER NOT NULL, ssID INTEGER, YearFounded INTEGER NOT NULL CHECK (YearFounded >= 1857), Website VARCHAR(60), FOREIGN KEY (sID) REFERENCES Stadiums(sID) ON DELETE RESTRICT, FOREIGN KEY (ssID) REFERENCES ShirtSponsors(ssID) ON DELETE RESTRICT);",

        "CREATE TABLE Players(pID INT PRIMARY KEY NOT NULL, FirstName VARCHAR(20), LastName VARCHAR(20), tID INT, StrongFoot VARCHAR(2), DOB DATE, WeightKG INT, ShirtNum INT, FOREIGN KEY (tID) REFERENCES Teams(tID) ON DELETE RESTRICT);",

        "CREATE TABLE Contracts (cID INTEGER PRIMARY KEY, pID INTEGER NOT NULL, tID INTEGER NOT NULL, ContractType VARCHAR(20), WeeklySalaryUSD DECIMAL, DateSigned DATE, ExpiryDate DATE, Active BOOLEAN, FOREIGN KEY (pID) REFERENCES Players(pID) ON DELETE RESTRICT);",

        "CREATE TABLE Injuries (iID INTEGER PRIMARY KEY, pID INTEGER NOT NULL, DateOfInjury DATE NOT NULL, DateOfRecovery DATE, TypeOfInjury VARCHAR(50), FOREIGN KEY (pID) REFERENCES Players(pID) ON DELETE RESTRICT);",

        "CREATE TABLE TeamMerchandise (mID INTEGER PRIMARY KEY, tID INTEGER, ProductName VARCHAR(30), PriceUSD DECIMAL, UnitsSold INTEGER, InStock BOOLEAN, DateOfNextShipment DATE, FOREIGN KEY (tID) REFERENCES Teams(tID) ON DELETE RESTRICT);"
    };

    String[] scottsSQLStatments = {"DROP DATABASE IF EXISTS footballStuff" ,"CREATE DATABASE footballStuff;", "USE footballStuff;", 

    "CREATE TABLE Players (player_id INT NOT NULL PRIMARY KEY, player_name VARCHAR(100), nationality VARCHAR(50), date_of_birth DATE, position VARCHAR(20), club_id INT NOT NULL, FOREIGN KEY (club_id) REFERENCES Clubs(club_id) ON DELETE RESTRICT);",

    "CREATE TABLE Clubs (club_id INT NOT NULL PRIMARY KEY, club_name VARCHAR(100) UNIQUE NOT NULL, country VARCHAR(50), founded_year INT);",

    "CREATE TABLE Stadiums (stadium_id INT NOT NULL PRIMARY KEY, stadium_name VARCHAR(100) NOT NULL, club_id INT NOT NULL, stadium_capacity INT, FOREIGN KEY (club_id) REFERENCES Clubs(club_id) ON DELETE RESTRICT);",

    "CREATE TABLE Matches (match_id INT NOT NULL PRIMARY KEY, match_date DATE, home_team_id INT NOT NULL, away_team_id INT NOT NULL, stadium_id INT NOT NULL, home_goals INT, away_goals INT, FOREIGN KEY (home_team_id) REFERENCES Clubs(club_id) ON DELETE RESTRICT, FOREIGN KEY (away_team_id) REFERENCES Clubs(club_id) ON DELETE RESTRICT, FOREIGN KEY (stadium_id) REFERENCES Stadiums(stadium_id) ON DELETE RESTRICT);"
    };

    String[] scottsDELETEQuerys = {"DELETE FROM Clubs WHERE club_id = 1;",
                                "DELETE FROM Stadiums WHERE stadium_id = 1;"};

    String[] scottsSELECTQuerys = {"SELECT match_id FROM Matches WHERE (home_goals > away_goals)"};

    String[] scottsGroupByQuerys = {"SELECT club_id, COUNT(*) AS num_players FROM Players GROUP BY club_id HAVING COUNT(*) > 20;",
                                "SELECT stadium_id, stadium_name, stadium_capacity FROM Stadiums GROUP BY stadium_id, stadium_name, stadium_capacity HAVING stadium_capacity > 10000;"};

    
    public static void main(String[] args) throws SQLException { // java -cp ".:/usr/share/java/mariadb-java-client.jar:" Code.java

        Code code = new Code();
        String url = "jdbc:mysql://localhost:3306/";
        Connection con = connect(url);

        if (con == null) { //was connection succesfull?
            System.out.print("Issue with connecting to mySQL server\nProgram closing...");
            System.exit(0); //close program, if server isnt connected then nothing else is going to work
        }

        String[] csvArray = {"38639416.csv", "38790475.csv", "38783681.csv"};
        for (String filenumber: csvArray) { //loop over the csv file names array

            CsvReader reader = new CsvReader(filenumber); //make new csv reader with current csv file
            String[] schema = {}; //will hold the sql statments to make current group members schema
            String[][] statements = code.getSQLStatments();

            switch (filenumber) { //whos schema are we making?
                case "38639416.csv":
                    schema = statements[0]; //make the schema array Joe's SQL statments
                    break;
                case "38790475.csv":
                    schema = statements[1]; //make the schema array Danny's SQL statments
                    break;
                case "38783681.csv":
                    schema = statements[2]; //make the schema array Scott's SQL statments
                    break;
            }

            code.constructSchema(schema, con, filenumber);

            code.constructINSERTQuerys(reader, con, filenumber);

            code.performSELECTS(con, filenumber);

            //code.performDeletions(con, filenumber);
        }
    }

    public void performDeletions(Connection con, String filename)
    {
        String[] queries = this.getDELETEQuerys(filename);
        if (!(queries == null)) {
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
    }

    public String[] getDELETEQuerys(String filename)
    {
        if (filename.equals(new String("38639416.csv"))) {
            return this.joesDELETEQuerys;
        } else {
            if (filename.equals(new String("38783681.csv"))){
                return this.scottsDELETEQuerys;
            }
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

    public void constructSchema(String[] schema, Connection con, String filename) //Builds the current DB schema
    {
        System.out.println("Making Schema of student:" + filename + "....."); 
        try {
            Statement statment = con.createStatement();

            for (String query: schema) {

                String SQLQuery = query;

                statment.executeUpdate(SQLQuery);

            }
        } catch (Exception e) {
                e.printStackTrace();
        }
        System.out.println("Schema of student: " + filename +  " has been constructed");   
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
                    
                    case "38783681.csv":
                        this.makeScottsINSERTS(tableName, valuesToInsert, con);
                        break;
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {}
    }

    public static PreparedStatement statetmentBuilder(PreparedStatement prepState, ArrayList<String> valuesToInsert, String[] types)throws SQLException{ 

        for (int x = 1; x < valuesToInsert.size(); x++) {
            Boolean defaultCaught = false;
            String temp = valuesToInsert.get(x).strip();
            if (temp.equals("NULL")){
                int typeOf = 0;
                //deal with null
                switch (types[x - 1]) {
                    case "Int":
                        typeOf = java.sql.Types.INTEGER;
                        break;

                    case "Bool":
                        typeOf = java.sql.Types.TINYINT;
                        break;
                    
                    case "Decimal":
                        typeOf = java.sql.Types.DECIMAL;
                        break;

                    case "Date":
                        typeOf = java.sql.Types.DATE;
                        break;
                    
                    case "DateTime":
                        typeOf = java.sql.Types.TIMESTAMP;
                    
                    default:
                        defaultCaught = true;
                        prepState.setString(x, temp);
                        break;
                }
                if (!defaultCaught){
                    System.out.println("Inserting: " + temp + " at position: " + x);
                    prepState.setNull(x, typeOf);
                }
            }
            else{
                System.out.println("Inserting: " + temp + " at position: " + x);
                prepState.setString(x, valuesToInsert.get(x));
            }
        }
        return prepState;
    }


    public void makeDannysINSERTS(String tableName, ArrayList<String> valuesToInsert, Connection con) throws SQLException
    {
        String sql = "";
        PreparedStatement statement = con.prepareStatement(" ");
        String[] types = {};
        switch (tableName) {
            case "Stadiums":
                types = new String[] {"Int", "Int", "String", "Int", "String", "Bool", "Int"};
                sql = "INSERT INTO  " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?)";
                break;
        
            case "ShirtSponsors":
                types = new String[] {"Int", "String", "String", "String", "String"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
                break;

            case "Teams":
                types = new String[] {"Int", "String", "Int", "Int", "Int", "String"};
                sql = "INSERT INTO " + tableName + " VALUES(? ,?, ?, ?, ?, ?)";
                break;
            
            case "Players":
                types = new String[] {"Int", "String", "String", "Int", "String", "Date", "Int", "Int"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
                break;
            
            case "Contracts":
                types = new String[] {"Int", "Int", "Int", "String", "Decimal", "Date", "Date", "Bool"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
                break;

            case "Injuries":
                types = new String[] {"Int", "Int", "Date", "Date", "String"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
                break;

            case "TeamMerchandise":
                types = new String[] {"Int", "Int", "String", "Decimal", "Int", "Bool", "Date"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?)";
                break;
        }
        statement = con.prepareStatement(sql);
        statement = statetmentBuilder(statement, valuesToInsert, types);

        Integer rowsAffected = statement.executeUpdate();

    }


    public void makeJoesINSERTS(String tableName, ArrayList<String> valuesToInsert, Connection con) throws SQLException
    {
        String sql = "";
        PreparedStatement statement = con.prepareStatement(" ");
        String[] types = {};
        switch (tableName) {
            case "Coaches":
                types = new String[] {"Int", "String", "String", "Int", "Bool"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
                break;
            case "Referees":
                types = new String[] {"Int", "String", "String", "Int", "Int"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
                break;
            case "Teams":
                types = new String[] {"Int", "Int", "String", "String", "Int", "Bool", "Int", "String"};
                sql = "INSERT INTO " + tableName + " VALUES(? ,?, ?, ?, ?, ?, ?, ?)";            
                break;
            case "Fixtures":
                //System.out.println("record: "+ valuesToInsert);
                types = new String[] {"Int", "Int", "Date", "String", "Int", "Int", "Int", "Int"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
                break;
            case "Players":
                types = new String[] {"Int", "Int", "String", "Date", "Int", "Int"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?)";
                break;
            case "Stadiums":
                types = new String[] {"Int", "String", "Int", "Int", "Int"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?)";
                break;
            case "TeamMatchups":
                types = new String[] {"Int", "Int", "Int"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?)";
                break; 
        }
        statement = con.prepareStatement(sql);
        statement = statetmentBuilder(statement, valuesToInsert, types);
        Integer rowsAffected = statement.executeUpdate();
    
    }

    public void makeScottsINSERTS(String tableName, ArrayList<String> valuesToInsert, Connection con) throws SQLException
    {
        String sql = "";
        PreparedStatement statement = con.prepareStatement(" ");
        String[] types = {};
        switch (tableName) {
            case "Players":
                types = new String[] {"Int", "String", "String", "Date", "String", "Int"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?)";
                break;
            case "Clubs":
                types = new String[] {"Int", "String", "String", "Int"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?)";
                break;
            case "Stadiums":
                types = new String[] {"Int", "String", "Int", "Int"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?)";
                break;
            case "Matches":
                types = new String[] {"Int", "Date", "Int", "Int", "Int", "Int", "Int"};
                sql = "INSERT INTO " + tableName + " VALUES(?, ?, ?, ?, ?, ?, ?)";
                break;
        }
        statement = con.prepareStatement(sql);
        statement = statetmentBuilder(statement, valuesToInsert, types);
        Integer rowsAffected = statement.executeUpdate();
    
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
