package db;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DB {
	
	private static Connection conn = null;
	
	// méotodo que cria uma connection
	public static Connection getConnection() {
		if (conn == null) {
			try {
				Properties props = loadProperties(); 
				String url = props.getProperty("dburl");
				
				conn = DriverManager.getConnection(url, props);
			} catch (SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
		
		return conn;
	}
	
	// método que encessa uma connection
	public static void closeConnection() {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		
	}
	
	// método para carregas as propriedades do arquivo db.properties
	private static Properties loadProperties() {
		try (FileInputStream fs = new FileInputStream("db.properties")) {
			Properties props = new Properties();
			props.load(fs);
			return props;
		} catch(IOException e) {
			throw new DbException(e.getMessage());
		}
	}
	
	// método para fechar um statement
	public static void closeStatement(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch(SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
	}
	
	// método utilizado para fechar um resultset
	public static void closeResultSet(ResultSet resultSet) {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch(SQLException e) {
				throw new DbException(e.getMessage());
			}
		}
	}
}
