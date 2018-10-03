package com.demo.SparkStreamingWordCount2;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
public class HbaseCon
{
	private static String URL = "jdbc:phoenix:10.21.20.40,10.21.20.42,10.21.20.44:12181";
	public static Connection getConnection()
	{
		Connection connection = null;
		try
		{
			connection = DriverManager.getConnection(URL);
		} catch (Exception e)
		{
			e.printStackTrace();
			// TODO: handle exception
		}
		return connection;
	}
	public static void close(Connection connection)
	{
		try
		{
			if (connection != null)
			{
				connection.close();
			}
		} catch (Exception e)
		{
			e.printStackTrace();
			// TODO: handle exception
		}
	}
	public void inserttest(String string) throws Exception
	{
		String str[] = string.split(";");
		Integer ID = Integer.parseInt(str[0]);
		String Name = str[1];
		String age = str[2];
		String sql1 = "upsert into STUDENT VALUES(" + ID + "," + "'" + Name + "'" + "," + "'" + age + "'" + ")";
		// String sql2 = "upsert into STUDENT VALUES(101,'SECOND','SUCCESS')";
		// String sql1 = "upsert into STUDENT VALUES(100,'100','100')";
		Connection connection = getConnection();
		Statement statement = null;
		try
		{
			statement = connection.createStatement();
			statement.executeUpdate(sql1);
			connection.commit();
		} catch (Exception e)
		{
			e.printStackTrace();
			// TODO: handle exception
		}
		statement.close();
		close(connection);
	}
	public void sel() throws Exception
	{
		Connection connection = getConnection();
		Statement statement;
		ResultSet rSet = null;
		statement = connection.createStatement();
		String sql1 = "select * from STUDENT";
		rSet = statement.executeQuery(sql1);
		while (rSet.next())
		{
			System.out.println(rSet.getInt("ID"));
		}
		statement.close();
		close(connection);
	}
	// public static void main(String[] args) throws Exception {
	// HbaseCon hbaseCon = new HbaseCon();
	// hbaseCon.inserttest("104;first;second");
	// hbaseCon.inserttest("101;first;second");
	// hbaseCon.inserttest("102;first;second");
	// hbaseCon.inserttest("103;first;second");
	// //hbaseCon.sel();
	// }
}