package com.example.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Resource {

	@RequestMapping("/put")
	public String put() {

		return putData();
	}

	@RequestMapping("/get")
	public Map<Integer, String> get() {

		return getData();
	}

	@RequestMapping("/getdb")
	public String getdb() {

		return getDb();
	}

	private String getDb() {

		String host = "jdbc:mysql://mysql:3306/sampledb";
		String user = "iris";
		String password = "iris";
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(host, user, password);

			System.out.println("\n\t The connection class name is : " + conn.getClass().getName());
			System.out.println("\n\n\t GOT Connection at: " + new java.util.Date());
			Statement stmt = (Statement) conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from sampledb.iris_ui");
			System.out.println("\n\t###########################################");
			System.out.println("\n\tid" + "\tname" + "\t\t\trating");
			int id = 0;
			String name = "";
			while (rs.next()) {
				id = rs.getInt("employee_id");
				name = rs.getString("employee_name");
				System.out.println("\n\t" + id + "\t" + name);
			}
			System.out.println("\n\t###########################################");
			return id + " : " + name;

		} catch (SQLException e) {
			e.printStackTrace();
		}

		finally {

			if (conn != null) {
				System.out.println("\n\tfinally{} if(con!=null) ");
				try {
					conn.close();
					System.out.println("\n\t Closing the connection !!");
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}
		String result = "Successfully Got the Connection ...Exiting the program";

		return result;
	}

	public String putData() {

		Ignite objIgnite = getInstance();
		IgniteCache<Integer, String> objIgniteCache = objIgnite.getOrCreateCache("irisCache");
		System.out.println("**************Start putting data into ignite cache**************");
		objIgniteCache.put(1, "iris-ui");
		objIgniteCache.put(2, "iris-spring");
		objIgniteCache.put(3, "iris-ignite");
		objIgniteCache.put(4, "iris-oracle");

		return "Data is stored in Ignite";
	}

	public Map<Integer, String> getData() {

		Ignite objIgnite = getInstance();
		IgniteCache<Integer, String> objIgniteCache = objIgnite.getOrCreateCache("irisCache");
		System.out.println("**************Start fetching data from ignite cache**************");
		System.out.println(objIgniteCache.get(1));
		System.out.println(objIgniteCache.get(2));
		System.out.println(objIgniteCache.get(3));
		System.out.println(objIgniteCache.get(4));

		Map<Integer, String> map = new HashMap<>();
		map.put(1, objIgniteCache.get(1));
		map.put(2, objIgniteCache.get(2));
		map.put(3, objIgniteCache.get(3));
		map.put(4, objIgniteCache.get(4));
		return map;
	}
	
	public Ignite getInstance() {
		Ignition.setClientMode(true);

		Ignite objIgnite = Ignition.start("default-config.xml");

		return objIgnite;
	}

}
