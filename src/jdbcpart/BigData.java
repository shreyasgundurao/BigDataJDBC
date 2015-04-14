package jdbcpart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;

import jdbcpart.establishConnection;

public class BigData {

	static ArrayList<String> validFiles = new ArrayList<String>();
	static String mesowest_csv;

	
	
	
	

	public static void main(String[] args) {
		createTables();
		int choice1, choice2;
		Scanner input1, input2;
		do {
			System.out
					.println("Enter \n 1 to load data \n 2 to delete data \n 3 to fetch all data \n 4 to sort \n 5 to find temp of a station \n 6 to update \n 0 to exit");
			input1 = new Scanner(System.in);
			choice1 = input1.nextInt();
			switch (choice1) {
			case 1:
				loadData();
				break;

			case 2:
				deleteData();
				break;

			case 3:
				fetchData();
				break;

			case 4:
				sortData();
				break;

			case 5:
				findTemp();
				break;

			case 6:
				update();
				break;

			case 0:
				dropTables();
				System.exit(0);
			default:
				System.out.println("Enter a valid input...!");
				break;
			}
			System.out.println("Enter any number to continue or 0 to exit :");
			input2 = new Scanner(System.in);
			choice2 = input2.nextInt();

			if (choice2 == 0)
				dropTables();

		} while (choice2 != 0);

	}

	public static void loadData() {

		deleteData();
		try {

			Scanner file_1, file_2, file_3;

			System.out.println("\nEnter the path of mesowest_csv.tbl file\n");
			file_1 = new Scanner(System.in);
			String file_1_path = file_1.nextLine();

			System.out.println("\nEnter the path of mesowest1.out file\n");
			file_2 = new Scanner(System.in);
			String file_2_path = file_2.nextLine();

			// ------------------------------------------------------------------------------
			System.out.println("\nEnter the path of mesowest2.out file\n");
			file_3 = new Scanner(System.in);
			String file_3_path = file_3.nextLine();

			// --------------------------------------------------------------------------------
			// System.out.println("File 1 : "+file_1_path);
			// System.out.println("File 2 : "+file_2_path);
			//
			int counter = 0;

			// String filePath = "src\\finalData\\mesowest_csv.tbl";

			// deleteData();
			long starttime = System.currentTimeMillis();
			File myFile = new File(file_1_path);
			String result[] = new String[18];
			establishConnection con = new establishConnection();
			Connection conn = con.getConnection();
			FileInputStream f_In = new FileInputStream(myFile);
			BufferedReader b_In = new BufferedReader(
					new InputStreamReader(f_In));
			String temp = b_In.readLine();
			temp = b_In.readLine();
			StringTokenizer str;
			PreparedStatement pstmt;
			int primary_key_id = 1;
			int i;
			String query;
			query = "insert into mesowest1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			pstmt = conn.prepareStatement(query);
			conn.setAutoCommit(false);
			while (temp != null) {

				while (temp.contains(",,")) {
					temp = temp.replace(",,", ",0,").toString();

				}

				if (temp.contains(",;"))
					temp = temp.replace(",;", ",0").toString();

				i = 0;
				str = new StringTokenizer(temp, ",");
				while (str.hasMoreTokens()) {
					result[i++] = str.nextToken();
				}

				for (int j = 0, k = 1; j < result.length; j++, k++) {
					pstmt.setString(k, result[j]);

				}
				pstmt.setInt(19, primary_key_id++);
				pstmt.addBatch();

				counter++;

				if ((counter + 1) % 1000 == 0) {
					pstmt.executeBatch(); // Execute every 1000 items.
				}

				if (counter % 100 == 0)
					System.gc();

				temp = b_In.readLine();
			}

			pstmt.executeBatch();
			System.out.println(counter + " added in mesowest table");
			// con.endConnection(conn);

			long stopttime = System.currentTimeMillis();
			System.out.println("Time taken = " + (stopttime - starttime)
					/ 1000f);

			int counter_1 = 0;
			// String filePath_1 = "src\\finalData\\mesowest.out";
			long starttime_1 = System.currentTimeMillis();
			File myFile_1 = new File(file_2_path);
			String result_1[] = new String[16];
			FileInputStream f_In_1 = new FileInputStream(myFile_1);
			BufferedReader b_In_1 = new BufferedReader(new InputStreamReader(
					f_In_1));
			String temp_1 = b_In_1.readLine();
			temp_1 = b_In_1.readLine();
			StringTokenizer str_1;
			PreparedStatement pstmt_1;
			int primary_key_id_1 = 1;
			int z;
			String query_1;
			query_1 = "insert into mesowest_station1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			pstmt_1 = conn.prepareStatement(query_1);
			conn.setAutoCommit(false);
			while (temp_1 != null) {

				try {
					z = 0;
					str_1 = new StringTokenizer(temp_1, " ");
					while (str_1.hasMoreTokens()) {
						result_1[z++] = str_1.nextToken();
					}

					pstmt_1.setString(1, result_1[0]);
					
					//------------------------------------
					
					char[] charArray = new char[2];
					int position = 6;
					for(int c=0; c<=1; c++) {
					charArray[c] = result_1[1].toString().charAt(position);
					position++;
					}

					StringBuffer strBuf = new StringBuffer().append(charArray);

					int res = Integer.parseInt(strBuf.toString());

//----------------------------------------------------------------
					pstmt_1.setString(2, result_1[1]);

					for (int j = 6, k = 3; j < result_1.length; j++, k++) {
						pstmt_1.setString(k, result_1[j]);

					}
					pstmt_1.setInt(13, primary_key_id_1++);
					pstmt_1.setInt(14, res);
					pstmt_1.addBatch();

					counter_1++;

					if ((counter_1 + 1) % 1000 == 0) {
						pstmt_1.executeBatch(); // Execute every 1000 items.
					}

					if (counter_1 % 100 == 0)
						System.gc();

					temp_1 = b_In_1.readLine();

				} catch (SQLIntegrityConstraintViolationException e) {
					continue;

				}

			}

			pstmt_1.executeBatch();
			// System.out.println(counter_1 +
			// " added in mesowest_station table");

			// ---------------------------------------------------------------------------------

			File myFile_2 = new File(file_3_path);
			String result_2[] = new String[16];
			FileInputStream f_In_2 = new FileInputStream(myFile_2);
			BufferedReader b_In_2 = new BufferedReader(new InputStreamReader(
					f_In_2));
			String temp_2 = b_In_2.readLine();
			temp_2 = b_In_2.readLine();
			StringTokenizer str_2;
			PreparedStatement pstmt_2;
			int primary_key_id_2 = primary_key_id_1;
			int a;
			String query_2;
			query_2 = "insert into mesowest_station1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			pstmt_2 = conn.prepareStatement(query_2);
			conn.setAutoCommit(false);
			while (temp_2 != null) {

				try {
					a = 0;
					str_2 = new StringTokenizer(temp_2, " ");
					while (str_2.hasMoreTokens()) {
						result_2[a++] = str_2.nextToken();
					}

					pstmt_2.setString(1, result_2[0]);
					
					char[] charArray = new char[2];
					int position = 6;
					for(int c=0; c<=1; c++) {
					charArray[c] = result_2[1].toString().charAt(position);
					position++;
					}

					StringBuffer strBuf = new StringBuffer().append(charArray);

					int res2 = Integer.parseInt(strBuf.toString());


					pstmt_2.setString(2, result_2[1]);

					for (int j = 6, k = 3; j < result_2.length; j++, k++) {
						pstmt_2.setString(k, result_2[j]);

					}
					pstmt_2.setInt(13, primary_key_id_2++);
					pstmt_2.setInt(14, res2);
					
					pstmt_2.addBatch();

					counter_1++;

					if ((counter_1 + 1) % 1000 == 0) {
						pstmt_2.executeBatch(); // Execute every 1000 items.
					}

					if (counter_1 % 100 == 0)
						System.gc();

					temp_2 = b_In_2.readLine();

				} catch (SQLIntegrityConstraintViolationException e) {
					continue;

				}

			}

			pstmt_2.executeBatch();
			System.out.println(counter_1 + " added in mesowest_station table");

			// ------------------------------------------------------------------------------------

			con.endConnection(conn);

			long stopttime_1 = System.currentTimeMillis();
			System.out.println("Time taken = " + (stopttime_1 - starttime_1)
					/ 1000f);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void deleteData() {

		try {

			long starttime = System.currentTimeMillis();

			establishConnection con = new establishConnection();
			Connection conn = con.getConnection();
			conn.setAutoCommit(false);

			String delete_query = "delete from mesowest1";
			PreparedStatement pstmt1 = conn.prepareStatement(delete_query);
			pstmt1.execute();

			// con.endConnection(conn);

			long stopttime = System.currentTimeMillis();

			System.out.println("Time taken to delete from mesowest = "
					+ (stopttime - starttime) / 1000f);

			long starttime_1 = System.currentTimeMillis();

			conn.setAutoCommit(false);

			String delete_query_1 = "delete from mesowest_station1";
			PreparedStatement pstmt = conn.prepareStatement(delete_query_1);
			pstmt.execute();

			con.endConnection(conn);

			long stopttime_1 = System.currentTimeMillis();

			System.out.println("Time taken to delete from mesowest_station  = "
					+ (stopttime_1 - starttime_1) / 1000f);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static ResultSet fetchData() {
		// loadData();
		ResultSet rs_1 = null;
		int j = 0;
		try {
			long starttime_1 = System.currentTimeMillis();
			establishConnection con = new establishConnection();
			Connection conn = con.getConnection();
			conn.setAutoCommit(false);
			String query_1 = "select * from test.mesowest1 limit 100000";
			Statement stmt_1 = conn.createStatement();
			rs_1 = stmt_1.executeQuery(query_1);

			while (rs_1.next()) {
				j++;
//				for (int i = 1; i < 19; i++) {
//					System.out.print(rs_1.getString(i) + ",");
//				}
//				System.out.println("\n");
			}

			long stopttime_1 = System.currentTimeMillis();

			System.out.println("Time taken to fetch " + j
					+ " rows from mesowest  = " + (stopttime_1 - starttime_1)
					/ 1000f);

			long starttime_2 = System.currentTimeMillis();
			conn.setAutoCommit(false);
			String query_2 = "select * from test.mesowest_station1 limit 100000";
			Statement stmt_2 = conn.createStatement();
			ResultSet rs_2 = stmt_2.executeQuery(query_2);

			int k = 0;
			while (rs_2.next()) {
				k++;
//				for (int i = 1; i < 13; i++) {
//					System.out.print(rs_2.getString(i) + ",");
//				}
//				System.out.println("\n");
			}

			con.endConnection(conn);

			long stopttime_2 = System.currentTimeMillis();

			System.out.println("Time taken to fetch " + k
					+ " rows from mesowest_station  = "
					+ (stopttime_2 - starttime_2) / 1000f);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return rs_1;

	}

	public static ResultSet sortData() {
		ResultSet rs_1 = null;
		try {
			long starttime_1 = System.currentTimeMillis();
			establishConnection con = new establishConnection();
			Connection conn = con.getConnection();
			conn.setAutoCommit(false);
			String query_1 = "select * from test.mesowest1 order by primary_id limit 100";
			Statement stmt_1 = conn.createStatement();
			rs_1 = stmt_1.executeQuery(query_1);

			int j = 0;
			while (rs_1.next()) {
				j++;
				// for(int i=1; i<19 ; i++)
				// {
				// System.out.print(rs_1.getString(i)+",");
				// }
				// System.out.println("\n");
			}

			long stopttime_1 = System.currentTimeMillis();

			System.out
					.println("Time taken to sort by primar_id in ascending order for"
							+ j
							+ " rows from mesowest  = "
							+ (stopttime_1 - starttime_1) / 1000f);

			long starttime_2 = System.currentTimeMillis();
			conn.setAutoCommit(false);
			String query_2 = "select * from test.mesowest_station1 order by stn limit 100";
			Statement stmt_2 = conn.createStatement();
			ResultSet rs_2 = stmt_2.executeQuery(query_2);

			int k = 0;
			while (rs_2.next()) {
				k++;
				// for(int i=1; i<17 ; i++)
				// {
				// System.out.print(rs_2.getString(i)+",");
				// }
				// System.out.println("\n");
			}

			con.endConnection(conn);

			long stopttime_2 = System.currentTimeMillis();

			System.out.println("Time taken to sort by station name for " + k
					+ " rows from mesowest_station in ascending order  = "
					+ (stopttime_2 - starttime_2) / 1000f);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return rs_1;

	}

	public static ResultSet findTemp() {
		ResultSet rs = null;
		try {
			long starttime = System.currentTimeMillis();
			establishConnection con = new establishConnection();
			Connection conn = con.getConnection();
			conn.setAutoCommit(false);

			String query = "SELECT mesowest1.primary_id,mesowest1.state,mesowest1.country, mesowest_station1.TMPF, mesowest_station1.datetime FROM mesowest1 INNER JOIN mesowest_station1 ON mesowest1.primary_id = mesowest_station1.STN "
					+ "where mesowest1.primary_id = 'KIYA'";

			Statement stmt = conn.createStatement();
			rs = stmt.executeQuery(query);

			int j = 0;

			System.out
					.println("STATION      STATE      COUNTRY      TEMPERTURE       TIMESTAMP");
			while (rs.next()) {
				j++;
				for (int i = 1; i < 6; i++) {
					System.out.print(rs.getString(i) + "          ");
				}
				System.out.println("\n");
			}

			con.endConnection(conn);

			long stopttime = System.currentTimeMillis();

			System.out.println("Time taken to fetch "+ j +" rows   = " + (stopttime - starttime)
					/ 1000f);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return rs;
	}

	public static int update() {
		int updated = 0;
		try {
			long starttime = System.currentTimeMillis();
			establishConnection con = new establishConnection();
			Connection conn = con.getConnection();
			conn.setAutoCommit(false);

			String query = "update mesowest_station1 SET WTHR=123 WHERE stn='KIYA' ";

			Statement stmt = conn.createStatement();
			updated = stmt.executeUpdate(query);

			if (updated == 0)
				System.out.println("NO ROWS UPDATED");
			else
				System.out.println(updated + " ROWS UPDATED SUCCESSFULLY");

			con.endConnection(conn);

			long stopttime = System.currentTimeMillis();

			System.out.println("Time taken   = " + (stopttime - starttime)
					/ 1000f);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return updated;
	}

	public static void createTables() {
		try {
			establishConnection con = new establishConnection();
			Connection conn = con.getConnection();
			conn.setAutoCommit(false);

			String query_1 = "create table mesowest1 "
					+ "( primary_id VARCHAR(150),secondary_id VARCHAR(150),station_name VARCHAR(150),state VARCHAR(150),"
					+ "country VARCHAR(150),latitude VARCHAR(150),longitude VARCHAR(150),elevation VARCHAR(150),mesowest_network_id VARCHAR(150),"
					+ "network_name VARCHAR(150),status VARCHAR(150),primary_provider_id VARCHAR(150),primary_provider VARCHAR(150),"
					+ "secondary_provider_id VARCHAR(150),secondary_provider VARCHAR(150),tertiary_provider_id VARCHAR(150),tertiary_provider VARCHAR(150),"
					+ "wims_id VARCHAR(150),primary_key VARCHAR(150) NOT NULL PRIMARY KEY)";
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(query_1);

			// con.endConnection(conn);

			System.out.println("mesowest table created");

			String query_2 = "CREATE TABLE `test`.`mesowest_station1` ( `stn` varchar(150) DEFAULT NULL, `datetime` varchar(25) DEFAULT NULL,"
					+ " `TMPF` varchar(50) DEFAULT NULL, `SKNT` varchar(50) DEFAULT NULL, `DRCT` varchar(50) DEFAULT NULL, `GUST` varchar(50) DEFAULT NULL,"
					+ " `PMSL` varchar(50) DEFAULT NULL, `ALTI` varchar(50) DEFAULT NULL, `DWPF` varchar(50) DEFAULT NULL, `RELH` varchar(50) DEFAULT NULL,"
					+ " `WTHR` varchar(50) DEFAULT NULL, `P24I` varchar(50) DEFAULT NULL, `range_id` int(11) DEFAULT NULL, `day` int(11) DEFAULT NULL) "
					+ "/*!50100 PARTITION BY RANGE (day)(PARTITION p0 VALUES LESS THAN (10) ENGINE = InnoDB,PARTITION p1 VALUES LESS THAN (20) ENGINE = InnoDB,"
					+ "PARTITION p2 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;";

			Statement stmt1 = conn.createStatement();
			stmt1.executeUpdate(query_2);

			System.out.println("mesowest_station table created");
			con.endConnection(conn);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void dropTables() {
		try {
			establishConnection con = new establishConnection();
			Connection conn = con.getConnection();
			conn.setAutoCommit(false);

			String query = "drop table mesowest1";
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(query);

			System.out.println("mesowest table deleted");

			String query_1 = "drop table mesowest_station1";
			Statement stmt1 = conn.createStatement();
			stmt1.executeUpdate(query_1);

			con.endConnection(conn);

			System.out.println("mesowest_station table deleted");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
