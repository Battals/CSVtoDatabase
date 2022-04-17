import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.Buffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class CSVconsume {
    public static void main(String[] args) {
        String jdbcUrl = "jdbc:mysql://localhost:3306/imdb"; //Indtat din databases URL
        String userName = "root"; // Indtast din databases userName
        String passwoord = "x"; //Indtast dit database password
        String filePath = "/Users/Ozcan/Desktop/imdb-data (1).csv";
        int batchsize = 20;
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(jdbcUrl, userName, passwoord);
            connection.setAutoCommit(false);
            //Indtast dine unikke parametre.
            String sql = "Insert into data(year,length,title,subject,popularity,awards) values(?,?,?,?,?,?)";

            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(filePath));

            String lineText = null;
            int count = 0;

            lineReader.readLine();
            while ((lineText = lineReader.readLine()) != null) {
                //Indtast dine unikke parametre.
                String[] data = lineText.split(";");
                String year = data[0];
                String length = data[1];
                String title = data[2];
                String subject = data[3];
                String popularity = data[4];
                String awards = data[5];
                statement.setInt(1, Integer.parseInt(year));
                statement.setString(2, length);
                statement.setString(3, title);
                statement.setString(4, subject);
                statement.setInt(5, Integer.parseInt(popularity));
                statement.setString(6, awards);
                statement.addBatch();
                if (count % batchsize == 0) {
                    statement.executeBatch();
                }
            }
            lineReader.close();
            statement.executeBatch();
            connection.commit();
            connection.close();
            System.out.println("Data has been inserted succesfully");
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}