package com.nicosb.uni.bloom_join;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.nicosb.uni.bloom_join.exception.InvalidQueryException;
import com.nicosb.uni.bloom_join.master.QueryValidator;

/**
 * QueryInformation stores valuable information about the query, 
 * namely the concerned tables and join attributes, as well as the maximum join size
 * 
 * @author Nico
 *
 */
public class QueryInformation {
	private String[] tables;
	private HashMap<String, String> joinAttributes;
	private int maxJoinSize;
		
	public QueryInformation(String query) throws InvalidQueryException{
		if(!QueryValidator.validate(query)){
			throw new InvalidQueryException();
		}
		else{
			tables = extractTables(query);
			joinAttributes = extractJoinAttributes(query);
		}
	}

	/**
	 * extracts the tables from a given query
	 * 
	 * @param query the query to be evaluated
	 * @return A HashMap containing the table as key and the attribute as value
	 * 
	 */
	private HashMap<String, String> extractJoinAttributes(String query) {
		HashMap<String, String> attrs = new HashMap<>();
		String attributesSubstring = query;
		int index;
		Pattern p = Pattern.compile("[\\w_]+\\.[\\w_]+");
		Matcher m;
		while((index = attributesSubstring.indexOf("on ")) != -1){
			attributesSubstring = attributesSubstring.substring(index + "on ".length());
			m = p.matcher(attributesSubstring); 
			if(m.find()){
				String attr = m.group();
				// table, attr
				attrs.put(attr.substring(0, attr.indexOf(".")).toLowerCase(), attr.substring(attr.indexOf(".")+1));
				attributesSubstring = attributesSubstring.substring(attributesSubstring.indexOf("=") + 1).trim();
				m = p.matcher(attributesSubstring);
				if(m.find()){
					attr = m.group();
					attrs.put(attr.substring(0, attr.indexOf(".")), attr.substring(attr.indexOf(".")+1));
				}
			}
		}
		return attrs;
	}

	/**
	 * 
	 * Extracts the tables from a given query
	 * @param query the query to be evaluated
	 * @return A string array containing all tables mentioned in the query
	 * @throws InvalidQueryException
	 * 
	 */
	private String[] extractTables(String query) throws InvalidQueryException {
		
		String table1 = query.substring(query.indexOf("from ") + "from ".length(), query.indexOf("join")).trim();
		String joinSubstring = query;
		ArrayList<String> join_tables = new ArrayList<>();
		join_tables.add(table1.toLowerCase());
		
		int index;
		
		// Parse join tables
		while((index = joinSubstring.indexOf("join ")) != -1){
			joinSubstring = joinSubstring.substring(index + "join ".length()).trim();
			join_tables.add(joinSubstring.substring(0, joinSubstring.indexOf(" ")));
			joinSubstring = joinSubstring.substring(joinSubstring.indexOf(" ")).trim();
		}
		try {
			calculateMaxJoinSize(join_tables);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		String[] ret = new String[join_tables.size()];
		join_tables.toArray(ret);
		return ret;
	}

	/**
	 * 
	 * Calculates the maximum join size. 
	 * Basically, it sums up the sizes for each table and returns the smalles value.
	 * 
	 * This is due to the fact, that the maximal number of values stored in the joined bloom filter
	 * is equal to the size of the smallest relation
	 * 
	 * @param join_tables an arraylist containing the names of every table used in the query.
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws InvalidQueryException
	 */
	private void calculateMaxJoinSize(ArrayList<String> join_tables) throws ClassNotFoundException, SQLException, InvalidQueryException {
		Class.forName("org.postgresql.Driver");
		String url = "jdbc:postgresql://localhost/bloom_join";
		Properties props = new Properties();
		props.setProperty("user", System.getenv("DB_USER"));
		props.setProperty("password", System.getenv("DB_PASSWORD"));
			
		Connection conn = DriverManager.getConnection(url, props);
		String check = tablesAreRegistered(conn, join_tables);
		if(check.length() > 0){
			throw new InvalidQueryException(check + " is not registered!");
		}
		
		
		PreparedStatement prep;
		String query = "SELECT min(sums.s) FROM (SELECT SUM(count) AS s FROM sitetables WHERE tablename IN (";
		for(int i = 0; i < join_tables.size(); i++){
			query += "?,";
		}
		query = query.substring(0, query.length() - 1);
		query += ") GROUP BY tablename) AS sums";
		
		prep = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		
		int i = 1;
		for(String t: join_tables){
			prep.setString(i, t);
			i++;
		}
		
		ResultSet rs = prep.executeQuery();
		rs.first();
		maxJoinSize = rs.getInt(1);
		conn.close();
	}

	/**
	 * Checks whether all tables given are registered by a site.
	 * Therefore, the method queries all tables from the relation "sitetables"
	 * which contains all table-site pairs.
	 * 
	 * @param conn The database connection
	 * @param tables ArrayList containing all tables to be checked.
	 * @return the name of the first table that is missing OR
	 * an empty string if all tables are registered.
	 * 
	 * @throws SQLException
	 */
	private String tablesAreRegistered(Connection conn, ArrayList<String> tables) throws SQLException{
		String query = "SELECT * FROM sitetables WHERE tablename = ?";
		PreparedStatement prep = conn.prepareStatement(query);
		
		for(String t: tables){
			prep.setString(1, t);
			ResultSet rs = prep.executeQuery();
			if(!rs.next()){
				return t;
			}
		}
		return "";
	}
	
	public String[] getTables() {
		return tables;
	}

	public void setTables(String[] tables) {
		this.tables = tables;
	}

	public HashMap<String, String> getJoinAttributes() {
		return joinAttributes;
	}

	public void setJoinAttributes(HashMap<String, String> joinAttributes) {
		this.joinAttributes = joinAttributes;
	}

	public int getMaxJoinSize() {
		return maxJoinSize;
	}
	
	
}
