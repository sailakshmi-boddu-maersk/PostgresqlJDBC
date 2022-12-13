package com.jdbc.com;
import java.util.*;
import java.sql.*;
import java.sql.SQLException;
import java.sql.DriverManager;
public class ConnectDB {
	
	public static Connection connection=null;
	public static Scanner sc=new Scanner(System.in);
	public static void main(String[] args) {
		
		ConnectDB c=new ConnectDB();
		try {
			
			Class.forName("org.postgresql.Driver");
			connection=DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres","2323");
			if(connection!=null) {
				System.out.println("connection established succesfully");	
			}
			else {
				System.out.println("Failed  to connect");
			}
			c.printMenu();
			int option=Integer.parseInt(sc.nextLine());
			switch(option) {
			case 1:
				c.insertRecord();
				break;
			case 2:
				c.select();
				break;
			case 3:
				c.selectAllRecords();
				break;
			case 4:
				c.updateRecord();
				break;
			case 5:
				c.deleteRecord();
				break;
			case 6:
				c.transaction();
				break;
			case 7:
				c.batchProcessing();
				break;
			default:
				break;
			}
			
           
	/*
		CallableStatement callableStatement=connection.prepareCall("{CALL GET_ALL()}");
		ResultSet res=callableStatement.executeQuery();
		while(res.next()) {
			System.out.print(res.getInt(1)+" ");
			System.out.print(res.getString(2)+" ");
			System.out.println(res.getString(3));
			}
	*/
	}	
	catch(Exception e) {
			System.out.println(e);
	}
		
		

}
	private void selectAllRecords() throws SQLException {
		CallableStatement callableStatement=connection.prepareCall("{CALL GET_ALL()}");
		ResultSet res=callableStatement.executeQuery();
		while(res.next()) {
			System.out.print(res.getInt(1)+" ");
			System.out.print(res.getString(2)+" ");
			System.out.println(res.getString(3));
			}
	}
	private void insertRecord() throws SQLException{
		 String query="insert into employee1(emp_id,emp_name,emp_address) values(?,?,?)";
         PreparedStatement preparedStatement=connection.prepareStatement(query);
      /*
         preparedStatement.setInt(1,6);
		 preparedStatement.setString(2,"Ashish");
		 preparedStatement.setString(3,"Delhi");
		 preparedStatement.executeUpdate();
	 */
         System.out.println("enter emp id");
		 preparedStatement.setInt(1,Integer.parseInt(sc.nextLine()));
			
		 System.out.println("enter emp name");
		 preparedStatement.setString(2,sc.nextLine());
			
		 System.out.println("enter emp address");
		 preparedStatement.setString(3,sc.nextLine());
			
		 preparedStatement.executeUpdate();
			
		 System.out.println("value inserted succesfully");	
		
	}
  private void select() throws SQLException{
	    String s="select * from employee1";
		Statement statement=connection.createStatement();
		ResultSet r=statement.executeQuery(s);
		
		while(r.next()) {
		System.out.print(r.getString(1)+" ");
		System.out.print(r.getString(2)+" ");
		System.out.println(r.getString(3));
		} 
	  
  }
  private void updateRecord() throws SQLException{
	  PreparedStatement preparedStatement;
	  int r3;
	  System.out.println("enter the id");
	   int id=Integer.parseInt(sc.nextLine());
	   
	   String s="select * from employee1 where emp_id= "+id;
		Statement statement=connection.createStatement();
		ResultSet r=statement.executeQuery(s);
		
		if(r.next()) {
		System.out.print(r.getString(1)+" ");
		System.out.print(r.getString(2)+" ");
		System.out.println(r.getString(3));
		
		String str="update employee1 set ";
		System.out.println("select what you want to update");
		System.out.println("1.name to be updated");
		System.out.println("2.address to be updated");
		int ch=Integer.parseInt(sc.nextLine());
		switch(ch) {
		case 1:
			
			str=str+"emp_name =? where emp_id = "+id;
			preparedStatement=connection.prepareStatement(str);
			System.out.println("enter name");
			preparedStatement.setString(1, sc.nextLine());
			r3=preparedStatement.executeUpdate();
//		    preparedStatement.executeQuery();
		    if(r3>0) {
		    	System.out.println("name updated successfully!!");
		    }
		    else {
		    	System.out.println("something wrong!!");
		    }
		   break;
		case 2:
			str=str+"emp_address =? where emp_id = "+id;
			preparedStatement=connection.prepareStatement(str);
			System.out.println("enter address");
			preparedStatement.setString(1, sc.nextLine());
			r3=preparedStatement.executeUpdate();
//		    preparedStatement.executeQuery();
		    if(r3>0) {
		    	System.out.println("address updated successfully!!");
		    }
		    else {
		    	System.out.println("something wrong!!");
		    }
		    break;
		 default:
			 break;
		}
		}
		
		
		else {
			System.out.println("Record not Found!!");
		}
  
	
}
  private void deleteRecord() throws SQLException {
	  System.out.println("enter emp id to delete");
	  int id=Integer.parseInt(sc.nextLine());
	  String sql="delete from employee1 where emp_id= "+id;
	  Statement statement=connection.createStatement();
      statement.executeQuery(sql);
      
	  
	  
  }
  private void transaction() throws SQLException {
	  connection.setAutoCommit(false);
	  
	  String sql1="insert into employee1(emp_id,emp_name,emp_address) values(8,'akash','delhi')";
	  String sql2="insert into employee1(emp_id,emp_name,emp_address) values(9,'Raghu','Bangalore')";
	  
	  PreparedStatement preparedStatement=connection.prepareStatement(sql1);
      int row1=preparedStatement.executeUpdate();
      
      
      preparedStatement=connection.prepareStatement(sql2);
      int row2=preparedStatement.executeUpdate();
      
      if(row1>1 && row2>1) {
      connection.commit();
      System.out.println("inserted successfully");
      }
     
  }
  private void batchProcessing() throws SQLException {
      connection.setAutoCommit(false);
	  
	  String sql1="insert into employee1(emp_id,emp_name,emp_address) values(8,'akash','delhi')";
	  String sql2="insert into employee1(emp_id,emp_name,emp_address) values(9,'Raghu','Bangalore')";
	  String sql3="insert into employee1(emp_id,emp_name,emp_address) values(10,'vijay','Bangalore')";
	  Statement statement=connection.createStatement();
	  statement.addBatch(sql1);
	  statement.addBatch(sql2);
	  statement.addBatch(sql3);
	  int[] rows=statement.executeBatch();
	  
	  for(int i:rows) {
		  if(i>0) {
			  continue;
		  }else {
			  connection.rollback();
		  }
	  }     
	  connection.commit();
  }
  private void printMenu() {
	  System.out.println("select an option:");
	  System.out.println("1. Insert row");
	  System.out.println("2. Select All");
	  System.out.println("3. Select All Records with procedure");
	  System.out.println("4. Update Record");
	  System.out.println("5. Delete Record");
	  System.out.println("6. Trascations demo");
	  System.out.println("7. Batch Processing demo");
  }
}
