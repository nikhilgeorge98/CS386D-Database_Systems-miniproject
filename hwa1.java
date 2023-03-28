import java.security.SecureRandom;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class hwa1 {

    static String url = "jdbc:postgresql://localhost:5432/hwa1";
    static String user = "postgres";
    static String password = "NikPostgr@2023";

    public static void main(String args[]){
        Connection connection;

        String deleteExists = "DROP TABLE IF EXISTS benchmark" ;
        String createTable_sql = "CREATE TABLE benchmark " +
                "(theKey INTEGER PRIMARY KEY, " +
                "columnA INTEGER, " +
                "columnB INTEGER, " +
                "filler CHAR(247))" ;
        String secIndOnA = "CREATE INDEX colA on benchmark (columnA)" ;
        String secIndOnB = "CREATE INDEX colB on benchmark (columnB)" ;
        String secIndOnAB = "CREATE INDEX colAB on benchmark (columnA, columnB)" ;

        String insert_sql = "INSERT INTO benchmark VALUES (?, ?, ?, ?)";

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
//            statementTable.addBatch(secIndOnA);
//            statementTable.addBatch(secIndOnB);
            statementTable.addBatch(secIndOnAB);
            statementTable.executeBatch();
            connection.commit();
            System.out.println("Created table in given database...");

            PreparedStatement statementInsert = connection.prepareStatement(insert_sql);

//            int rows = 1000;
            int i = 0;

            Random random = new Random();
            SecureRandom rand = new SecureRandom();
//            UUID theKey = UUID.randomUUID();
            int theKey, columnA, columnB;
            String filler;

            while(i<rows) {
//                theKey = rand.nextInt();
                theKey = unique5mil.get(i);
                columnA = random.nextInt(50000) + 1;
                columnB = random.nextInt(50000) + 1;
                filler = random.ints(97, 122 + 1)
                        .limit(247)
                        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                        .toString();
                statementInsert.setInt(1, theKey);
                statementInsert.setInt(2, columnA);
                statementInsert.setInt(3, columnB);
                statementInsert.setString(4, filler);

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

            long end = System.currentTimeMillis();
            System.out.println("Execution time(s): "+(end-start)/1000.0);
        } catch (SQLException e) {
            System.out.println("Exception!!!!!!!");
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}