package sportsvis.gsu.edu;
import java.sql.*;

public class DBConnector
{

       // JDBC driver name and database URL
	   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   static final String DB_URL = "jdbc:mysql://localhost:8889/SportVis";
	   
	   //Database credentials
	   static final String USER = "root";
	   static final String PASS = "root";
	   
	   
	   private Connection conn = null;
	   private PreparedStatement stmt = null;
	   private Statement smt = null;
	   private String table = "";
	   
	   
	   public void setTable(String s) 
	   {
		   table = s;
	   }
	   
	   public boolean connet() 
	   {
		   try{
		      //STEP 2: Register JDBC driver
		      Class.forName("com.mysql.jdbc.Driver");
	
		      //STEP 3: Open a connection
		      System.out.println("Connecting to database...");
		      conn = DriverManager.getConnection(DB_URL,USER,PASS);
		      /*
		      //STEP 4: Execute a query
		      System.out.println("Creating statement...");
		      
		      String sql;
		      sql = "INSERT INTO `SportVis`.`AthleticBayern` (`Time`, `TweetJson`) VALUES (CURRENT_TIMESTAMP, 'ppg is a pig and so is his wife!');";
		      stmt.executeUpdate(sql);*/
		      return true;
		      /*
		      //STEP 5: Extract data from result set
		      while(rs.next()){
		         //Retrieve by column name
		         int id  = rs.getInt("id");
		         int age = rs.getInt("age");
		         String first = rs.getString("first");
		         String last = rs.getString("last");
	
		         //Display values
		         System.out.print("ID: " + id);
		         System.out.print(", Age: " + age);
		         System.out.print(", First: " + first);
		         System.out.println(", Last: " + last);
		      } */
		      //STEP 6: Clean-up environment
		      //rs.close();
		   }catch(SQLException se){
			   
		      //Handle errors for JDBC
		      se.printStackTrace();
		   }catch(Exception e){
		      //Handle errors for Class.forName
		      e.printStackTrace();
		   }
		   return false;
	}//end main
	   
	
	public void prepare(String sql) 
	{
		try
		{
			stmt = conn.prepareStatement(sql);
		} catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setString(int field, String str)
	{
		try
		{
			stmt.setString(field, str);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public void setLong(int field, Long l) 
	{
		try
		{
			stmt.setLong(field, l);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	   
	public void updateDB()
	{
		try
		{
			//System.out.println(stmt)
			stmt.executeUpdate();
			//conn.commit();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public void close() 
	{
		try
		{
			stmt.close();
			conn.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public void setTimeStamp(int field, Timestamp t)
	{
		try
		{
			stmt.setTimestamp(field, t);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public void setInt(int field, int v)
	{
		try
		{
			stmt.setInt(field, v);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public void setBoolean(int field, boolean b)
	{
		try
		{
			stmt.setBoolean(field, b);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public ResultSet query(String queryStr)
	{
		ResultSet res = null;
		try
		{
			smt = conn.createStatement() ;
			res = smt.executeQuery(queryStr);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
		
		return res;
	}
	
	public static void main(String[] args)
	{
		DBConnector dc = new DBConnector();
		String sql = "INSERT INTO `SportVis`.`` (`Time`, `TweetJson`) VALUES (CURRENT_TIMESTAMP, 'ppg111 is a pig and so is his wife!');";
		
		if (dc.connet()) 
		{
			//dc.updateDB(sql);
		}
		dc.close();
	}
}
