package testCase;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import jdbcpart.BigData;
import jdbcpart.establishConnection;

import org.junit.Test;

public class BigDataTest {


	

	
	
	@Test
	public void testLoadData() {
		try
		{
			BigData.createTables();
			boolean flag=false;
			
			BigData.loadData();
			
			String query = "select count(*) from mesowest1";
			int count;
			establishConnection con = new establishConnection();
			Connection conn = con.getConnection();
			
			Statement stmt = conn.createStatement();
			
			
			
			
			ResultSet rs_1 = stmt.executeQuery(query);
			rs_1.next();
			count = rs_1.getInt(1);
			//System.out.println("COUNT AFTER LOAD : "+ count);
			
			
			if(count>0)
				flag=true;
			
			assertTrue(flag);

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
				
	}
	
	@Test
	
	public void testDeleteData() {
		try{
			
			
			BigData.deleteData();
			int count;
			establishConnection con = new establishConnection();
			Connection conn = con.getConnection();
			String query = "select count(*) from mesowest1";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			rs.next();
			count = rs.getInt(1);
			assertEquals(0, count);
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

	@Test
	public void testFetchData() {
		ResultSet rs = BigData.fetchData();
		assertNotNull(rs);
	}

	@Test
	public void testSortData() {
		ResultSet rs = BigData.sortData();
		assertNotNull(rs);
	}

	@Test
	public void testFindTemp() {
		ResultSet rs = BigData.findTemp();
		assertNotNull(rs);
	}

	@Test
	public void testUpdate() {
		boolean flag = false;
		
		int count = BigData.update();
		if(count>0)
			flag=true;
		assertEquals(true, flag);
		
	}

}
