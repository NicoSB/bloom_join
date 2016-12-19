import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import com.nicosb.uni.bloom_join.EnglishNumberToWords;

public class Setup {
	public static void main(String[] args){
		String create_numbers = "CREATE TABLE numbers(" +
				"id INTEGER UNIQUE," +
				"a VARCHAR(100)," + 
				"b BIGINT," + 
				"c VARCHAR(100)," + 
				"d BIGINT);";
		String create_numbers2 = "CREATE TABLE numbers2(" +
				"id INTEGER UNIQUE," +
				"a VARCHAR(100)," + 
				"b BIGINT," + 
				"c VARCHAR(100)," + 
				"d BIGINT);";
		String create_fives = "CREATE TABLE fives(" +
				"id INTEGER UNIQUE," +
				"a VARCHAR(100)," + 
				"b BIGINT," + 
				"c VARCHAR(100)," + 
				"d BIGINT);";
		String create_thirteens = "CREATE TABLE thirteens(" +
				"id INTEGER UNIQUE," +
				"a VARCHAR(100)," + 
				"b BIGINT," + 
				"c VARCHAR(100)," + 
				"d BIGINT);";

		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost/bloom_join";
			Properties props = new Properties();
			props.setProperty("user", System.getenv("DB_USER"));
			props.setProperty("password", System.getenv("DB_PASSWORD"));
			Connection conn = DriverManager.getConnection(url, props);
		
			System.out.println("Dropping tables...");

			conn.createStatement().executeUpdate("DROP TABLE IF EXISTS numbers;");
			conn.createStatement().executeUpdate("DROP TABLE IF EXISTS numbers2;");
			conn.createStatement().executeUpdate("DROP TABLE IF EXISTS fives;");
			conn.createStatement().executeUpdate("DROP TABLE IF EXISTS thirteens;");

			System.out.println("Creating tables...");

			conn.createStatement().executeUpdate(create_numbers);
			conn.createStatement().executeUpdate(create_numbers2);
			conn.createStatement().executeUpdate(create_fives);
			conn.createStatement().executeUpdate(create_thirteens);

			System.out.println("Filling tables...");

			String query = "INSERT INTO numbers(id, a, b, c, d) VALUES (?,?,?,?,?)";
			PreparedStatement prep = conn.prepareStatement(query);
			
			final int max = 20000;
			
			for(long i = 0; i < max; i++){
				prep.setLong(1, i);
				prep.setString(2, EnglishNumberToWords.convert(i));
				prep.setLong(3, (i*3));
				prep.setString(4, EnglishNumberToWords.convert(3*i));
				prep.setLong(5, i*i);
				prep.addBatch();
				prep.clearParameters();
			}
			prep.executeBatch();
			prep.close();
			
			query = "INSERT INTO numbers2(id, a, b, c, d) VALUES (?,?,?,?,?)";
			prep = conn.prepareStatement(query);
			for(long i = 0; i < max; i++){
				prep.setLong(1, i);
				prep.setString(2, EnglishNumberToWords.convert(i));
				prep.setLong(3, (i*3));
				prep.setString(4, EnglishNumberToWords.convert(3*i));
				prep.setLong(5, i*i);
				prep.addBatch();
				prep.clearParameters();
			}
			prep.executeBatch();
			prep.close();
			
			query = "INSERT INTO fives(id, a, b, c, d) VALUES (?,?,?,?,?)";
			prep = conn.prepareStatement(query);
			for(long i = 0; i < max; i+=5){
				prep.setLong(1, i);
				prep.setString(2, EnglishNumberToWords.convert(i));
				prep.setLong(3, (i*3));
				prep.setString(4, EnglishNumberToWords.convert(3*i));
				prep.setLong(5, i*i);
				prep.addBatch();
				prep.clearParameters();
			}
			prep.executeBatch();
			prep.close();
			
			query = "INSERT INTO thirteens(id, a, b, c, d) VALUES (?,?,?,?,?)";
			prep = conn.prepareStatement(query);
			for(long i = 0; i < max; i+=13){
				prep.setLong(1, i);
				prep.setString(2, EnglishNumberToWords.convert(i));
				prep.setLong(3, (i*3));
				prep.setString(4, EnglishNumberToWords.convert(3*i));
				prep.setLong(5, i*i);
				prep.addBatch();
				prep.clearParameters();
			}
			prep.executeBatch();
			prep.close();
			conn.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		System.out.println("Setup complete");
	}
}
