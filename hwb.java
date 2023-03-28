import java.security.SecureRandom;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class hwb {
    static String url = "jdbc:postgresql://localhost:5432/hwb";
    static String user = "postgres";
    static String password = "NikPostgr@2023";

    public static void main(String args[]){
        Random random = new Random();
        Connection connection;

        String deleteExists = "DROP TABLE IF EXISTS TestData" ;
        String createTable_sql = "CREATE TABLE TestData " +
                "(pk INTEGER PRIMARY KEY, " +
                "ht INTEGER, " +
                "tt INTEGER, " +
                "ot INTEGER, " +
                "hund INTEGER, " +
                "ten INTEGER, " +
                "filler CHAR(247))" ;
//        String secInd = "CREATE INDEX secind on benchmark (ht, tt, ot, hund, ten)" ;

        String insert_sql = "INSERT INTO TestData VALUES (?, ?, ?, ?, ?, ?, ?)";

        String query1 = "CREATE TABLE A " +
                "AS (SELECT * FROM TestData)" ;

        String query2 = "CREATE TABLE B " +
                "AS (SELECT * FROM TestData)" ;

        String query3 = "CREATE TABLE C " +
                "AS (SELECT * FROM TestData)" ;

        String query4 = "CREATE TABLE Aprime " +
                "AS (SELECT * FROM TestData)" ;

        String query5 = "CREATE TABLE Bprime " +
                "AS (SELECT * FROM TestData)" ;

        String query6 = "CREATE TABLE Cprime " +
                "AS (SELECT * FROM TestData)" ;

        String secIndA = "CREATE INDEX secindA on Aprime (ht, tt, ot, hund, ten)" ;
        String secIndB = "CREATE INDEX secindB on Bprime (ht, tt, ot, hund, ten)" ;
        String secIndC = "CREATE INDEX secindC on Cprime (ht, tt, ot, hund, ten)" ;

        int rows = 5000000;

        ArrayList<Integer> unique5mil = new ArrayList<>();
        for(int i = 0; i<rows; i++){
            unique5mil.add(i);
        }
//        Collections.shuffle(unique5mil);

        try {
            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(false);

            System.out.println("Starting now!!");

            long start = System.currentTimeMillis();

            Statement statementTable = connection.createStatement();
            statementTable.addBatch(deleteExists);
            statementTable.addBatch(createTable_sql);
//            statementTable.addBatch(secInd);
            statementTable.executeBatch();
            connection.commit();
            System.out.println("Created table in given database...");

            PreparedStatement statementInsert = connection.prepareStatement(insert_sql);

            int i = 0;

            int theKey, ht, tt, ot, hund, ten;
            String filler;

            while(i<rows) {
                theKey = unique5mil.get(i);
                ht = random.nextInt(100000);
                tt = random.nextInt(10000);
                ot = random.nextInt(1000);
                hund = random.nextInt(100);
                ten = random.nextInt(10);
                filler = random.ints(97, 122 + 1)
                        .limit(247)
                        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                        .toString();
                statementInsert.setInt(1, theKey);
                statementInsert.setInt(2, ht);
                statementInsert.setInt(3, tt);
                statementInsert.setInt(4, ot);
                statementInsert.setInt(5, hund);
                statementInsert.setInt(6, ten);
                statementInsert.setString(7, filler);

                statementInsert.addBatch();
                i++;
                if(i%50 == 0){
//                    System.out.println(i);
                    statementInsert.executeBatch();
                    statementInsert.clearBatch();
                }
            }
            statementInsert.executeBatch();
            statementInsert.clearBatch();
            connection.commit();

            Statement statementCopy = connection.createStatement();
            statementCopy.addBatch(query1);
            statementCopy.addBatch(query2);
            statementCopy.addBatch(query3);
            statementCopy.addBatch(query4);
            statementCopy.addBatch(secIndA);
            statementCopy.addBatch(query5);
            statementCopy.addBatch(secIndB);
            statementCopy.addBatch(query6);
            statementCopy.addBatch(secIndC);
            statementCopy.executeBatch();
            connection.commit();

        } catch (SQLException e) {
            System.out.println("Exception!!!!!!!");
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}
