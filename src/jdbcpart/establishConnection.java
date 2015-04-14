package jdbcpart;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class establishConnection {
		String url = "jdbc:mysql://localhost:3306/test";
		String uname = "root";
		String pwd = "madhu";
		
		public Connection getConnection()
		{
			Connection conn=null;
			try {
				Class.forName("com.mysql.jdbc.Driver");
				 conn = DriverManager.getConnection(url, uname, pwd);
				
			} catch (Exception e) {
				
				e.printStackTrace();
			}
			return conn;
			
		}
		
		public void endConnection(Connection conn)
		{
			try {
				conn.commit();
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		

	

}
