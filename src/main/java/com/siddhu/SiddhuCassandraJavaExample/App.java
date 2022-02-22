package com.siddhu.SiddhuCassandraJavaExample;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class App {
	public static void main(String[] args) {
		// Connect to the cluster and keyspace "devjavasource"
		final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
				.build();		
		final Session session = cluster.connect("siddhu");

		try
		{
			System.out.println("*********Cluster Information *************");
			System.out.println(" Cluster Name is: " + cluster.getClusterName() );
			System.out.println(" Driver Version is: " + cluster.getDriverVersion() );
			System.out.println(" Cluster Configuration is: " + cluster.getConfiguration() );
			System.out.println(" Cluster Metadata is: " + cluster.getMetadata() );
			System.out.println(" Cluster Metrics is: " + cluster.getMetrics() );		

			// Retrieve all User details from Users table
			System.out.println("*********Retrive User Data Example *************");		 
			getUsersAllDetails(session);

			// Insert new User into users table
			System.out.println("*********Insert User Data Example *************");		
			session.execute("INSERT INTO sid_emp (emp_id, emp_name, emp_houseno) VALUES (1, 'siddhu', 601);");
			getUsersAllDetails(session);

			// Update user data in users table
			System.out.println("*********Update User Data Example *************");		
			session.execute("UPDATE sid_emp SET emp_houseno=602 WHERE emp_id=1;");
			getUsersAllDetails(session);

			// Delete user from users table
			System.out.println("*********Delete User Data Example *************");		
			session.execute("DELETE FROM sid_emp WHERE emp_id=1;");
			getUsersAllDetails(session);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			// Close Cluster and Session objects
			System.out.println("nIs Cluster Closed :" + cluster.isClosed());
			System.out.println("Is Session Closed :" + session.isClosed());		
			cluster.close();
			session.close();
			System.out.println("Is Cluster Closed :" + cluster.isClosed());
			System.out.println("Is Session Closed :" + session.isClosed());
		}




	}

	private static void getUsersAllDetails(final Session inSession){		
		// Use select to get the users table data
		ResultSet results = inSession.execute("SELECT * FROM sid_emp");
		for (Row row : results) {
			System.out.println("emp name:"+row.getString("emp_name"));
			System.out.println("emp id:"+row.getInt("emp_id"));
			System.out.println("emp houseno:"+row.getVarint("emp_houseno"));

		}
	}
}