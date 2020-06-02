import java.sql.*;  
import java.io.*;
import java.util.*;
 
public class test {   
   public static void main(String[] args) throws FileNotFoundException {
      try { 
         Connection conn = DriverManager.getConnection(
               "jdbc:mysql://localhost:3306", "root", "ramathatha@R1");    
         Statement stmt = conn.createStatement();
         
         stmt.executeUpdate("create database if not exists homework4db");
         stmt.executeUpdate("use homework4db"); 
         
         stmt.executeUpdate("drop table if exists employee");
 		 stmt.executeUpdate("create table employee (eid int not null," + "name varchar(20), salary int," + "primary key (eid))");
 		 
 		 stmt.executeUpdate("drop table if exists supervisor");
 		 stmt.executeUpdate("create table supervisor (eid int references employee(eid)," + "sid int references employee(eid)," + "primary key (eid))");
         
 		 Integer e_eid[] = new Integer[] {10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120};
 		 String e_names[] = new String[] {"Martina", "Bary", "Iris", "Catline", "Harrison", "Mathew", "Steve", "Radcliff", "Emma", "Patrick", "Jack", "Christina"};
 		 Integer e_salary[] = new Integer[] {100000, 200000, 150000, 500000, 400000, 250000, 300000, 450000, 590000, 480000, 250000, 190000};
 		 PreparedStatement ps = conn.prepareStatement("insert into employee values (?,?,?)");
 		 for (int i = 0; i < 12; ++i) {
			ps.setInt(1, e_eid[i]);
			ps.setString(2, e_names[i]);
			ps.setInt(3, e_salary[i]);
			ps.executeUpdate();
 		 }
 		 
 		Integer s_eid[] = new Integer[] {20, 50, 30, 40, 60, 10, 70, 80, 90, 100, 110};
		Integer s_sid[] = new Integer[] {10, 90, 100, 30, 100, 120, 40, 70, 110, 120, 20};
		PreparedStatement ps1 = conn.prepareStatement("insert into supervisor values (?,?)");
		 for (int i = 0; i < 11; ++i) {
			ps1.setInt(1, s_eid[i]);
			ps1.setInt(2, s_sid[i]);
			ps1.executeUpdate();
		 }
 		 BufferedReader reader;
      try {
        	 reader = new BufferedReader(new FileReader("C:\\Users\\Canarabank\\Desktop\\Fall 2019\\DB\\HW4\\transfile.txt"));
        	 String line = reader.readLine();
        	 while(line!=null) {
        		 String[] str = line.split(" ");
        		 int trans_code= Integer.parseInt(str[0]);
        		 if (trans_code == 1) {
    	            Integer del_eid=Integer.parseInt(str[1]);
    	            PreparedStatement preparedStatement;
    	            preparedStatement = conn.prepareStatement("SELECT eid FROM employee WHERE eid =?");
    	            preparedStatement.setInt(1, del_eid);
    	            ResultSet rset=preparedStatement.executeQuery();
    	            if(rset.next()) {
    	            	if(rset!=null) {
    	            		preparedStatement = conn.prepareStatement("DELETE FROM employee WHERE eid=?");
    	            		preparedStatement.setInt(1, del_eid);  
    	            		Integer b=preparedStatement.executeUpdate();
    	    	            preparedStatement = conn.prepareStatement("UPDATE supervisor SET sid=? WHERE sid=?");
    	    	            preparedStatement.setString(1, null);  
    	    	            preparedStatement.setInt(2, del_eid);  
    	    	            preparedStatement.executeUpdate();
    	    	            if(b!=0) {
    	            			System.out.println("done deleteing an employee\n"); 
        	                	        }
    	    	            	    }
    	    	            	}
    	            else {
    	            		System.out.println("error deleteing an employee\n");
    	            	}
	    		 }
    	         else if (trans_code == 2) {
    	        	 Integer eid=Integer.parseInt(str[1]);
    	        	 String name=str[2];
    	        	 Integer salary=Integer.parseInt(str[3]);
    	        	 PreparedStatement preparedStatement;
      	             preparedStatement = conn.prepareStatement("INSERT into employee values (?,?,?)");
      	             preparedStatement.setInt(1, eid);
      	             preparedStatement.setString(2, name);
      	             preparedStatement.setInt(3, salary);
      	             preparedStatement.executeUpdate();
      	             
      	             Integer sid=Integer.parseInt(str[4]);
      	             preparedStatement = conn.prepareStatement("SELECT eid FROM employee WHERE eid =?");
  	                 preparedStatement.setInt(1, sid);
  	                 ResultSet rset=preparedStatement.executeQuery();
  	                 if(rset.next()) {
  	                	 
  	                		 preparedStatement = conn.prepareStatement("INSERT INTO supervisor values (?,?)");
  	                		 preparedStatement.setInt(1, eid);
  	                		 preparedStatement.setInt(2, sid);
   	            		     Integer i=preparedStatement.executeUpdate();  
   	            		     if(i!=0) {
   	            		    	 System.out.println("done inserting an employee\n"); 
      	                	        }
    	            
  	                }
  	                else {
  	                	System.out.print("Error inserting an employee\n");
  	                	PreparedStatement preparedStatement1;
  	                	preparedStatement1 = conn.prepareStatement("INSERT INTO supervisor values (?, ?)");
	                	preparedStatement1.setInt(1, eid);
	                	preparedStatement1.setString(2, null);
	            		preparedStatement1.executeUpdate();	 
  	                 }
    	         }
    	         else if (trans_code == 3) {
    	        	int eid=Integer.parseInt(str[1]);
    	        	if (str.length >2) {
    	        		int sid=Integer.parseInt(str[2]);
    	        		PreparedStatement preparedStatement;
         	            preparedStatement = conn.prepareStatement("SELECT eid FROM supervisor where eid=?");
         	            preparedStatement.setInt(1, eid);
         	            ResultSet rset=preparedStatement.executeQuery();
         	            if(rset.next()) {
         	            	if(rset!=null) {
         	            		preparedStatement = conn.prepareStatement("UPDATE supervisor set sid=? where eid=?");
                  	            preparedStatement.setInt(1, sid);
                  	            preparedStatement.setInt(2, eid);
                  	            preparedStatement.executeUpdate();
         	            	}
         	            }
         	            else {
         	            	PreparedStatement preparedStatement1;
         	            	preparedStatement1 = conn.prepareStatement("INSERT into supervisor values (?, ?)");
              	            preparedStatement1.setInt(1, sid);
              	            preparedStatement1.setInt(2, eid);
              	            preparedStatement1.executeUpdate();
         	            }
         	        }
    	        	else {
    	        		PreparedStatement preparedStatement;
    	        		preparedStatement = conn.prepareStatement("UPDATE supervisor set sid=? where eid=?");
          	            preparedStatement.setNull(1, java.sql.Types.INTEGER);
          	            preparedStatement.setInt(2, eid);
          	            preparedStatement.executeUpdate();
    	        	}
    	         }
    	        	
    	         else if (trans_code == 4) { 
     	           String strSelect = "select AVG(salary) from employee";
     	           ResultSet rset = stmt.executeQuery(strSelect);
     	           while(rset.next()) {   
     	        	   Double sal = rset.getDouble(1);
     	        	   System.out.println("\nAverage salary is: "+sal+"\n");
     	               }
     	            }
    	         else if (trans_code == 5) {
    	        	int sid=Integer.parseInt(str[1]);
    	        	PreparedStatement preparedStatement;
 	        		preparedStatement = conn.prepareStatement("select eid from supervisor where sid=?");
       	            preparedStatement.setInt(1, sid);
       	            ResultSet rset = preparedStatement.executeQuery();
       	            List<Integer> sid_list = new ArrayList<Integer>();
	            	while(rset.next()) {
       	            	    Integer sid1[] = new Integer[] {rset.getInt(1)};
       	            	    for (int i = 0; i < sid1.length; ++i) {
       	            	    	sid_list.add(sid1[i]);
       	            	    	}
	            	}
       	           if(sid_list!=null) {
	            	preparedStatement = conn.prepareStatement("select eid from supervisor where sid=?");            		
	            	for (int i = 0; i <sid_list.size(); ++i) {
	            		preparedStatement.setInt(1, sid_list.get(i));
	            		ResultSet rset1 =preparedStatement.executeQuery();
		       	        while(rset1.next()) {
		       	        	Integer sid2=rset1.getInt(1);
		       	        	if(!sid_list.contains(sid2)) {
		       	        		sid_list.add(sid2);
		       	        	}
		       	        }
		       	     }
       	           }
       	        preparedStatement = conn.prepareStatement("select name from employee where eid=?");            		
       	        System.out.print("Names of employees supervised by the supervisor "+sid+" are:"+"\n");
       	        for (int i = 0; i <sid_list.size(); ++i) {
            		preparedStatement.setInt(1, sid_list.get(i));
            		ResultSet rset1 =preparedStatement.executeQuery();
	       	        while(rset1.next()) {
	       	        	String name=rset1.getString(1); 
	       	        	System.out.print(name+"\n");  	 
	       	     }
       	       }
            }
    	         else if (trans_code == 6) {
    	        	 int sid=Integer.parseInt(str[1]);
     	        	PreparedStatement preparedStatement;
  	        		preparedStatement = conn.prepareStatement("select eid from supervisor where sid=?");
        	            preparedStatement.setInt(1, sid);
        	            ResultSet rset = preparedStatement.executeQuery();
        	            List<Integer> sid_list = new ArrayList<Integer>();
 	            	while(rset.next()) {
        	            	    Integer sid1[] = new Integer[] {rset.getInt(1)};
        	            	    for (int i = 0; i < sid1.length; ++i) {
        	            	    	sid_list.add(sid1[i]);
        	            	    	}
 	            	}
        	           if(sid_list!=null) {
 	            	preparedStatement = conn.prepareStatement("select eid from supervisor where sid=?");            		
 	            	for (int i = 0; i <sid_list.size(); ++i) {
 	            		preparedStatement.setInt(1, sid_list.get(i));
 	            		ResultSet rset1 =preparedStatement.executeQuery();
 		       	        while(rset1.next()) {
 		       	        	Integer sid2=rset1.getInt(1);
 		       	        	if(!sid_list.contains(sid2)) {
 		       	        		sid_list.add(sid2);
            	            	
 		       	        	}
 		       	        }
 		       	     }
        	       }
        	        preparedStatement = conn.prepareStatement("select salary from employee where eid in (?)");            		
        	        int sum_sal=0;
    	        	
        	        for (int i = 0; i <sid_list.size(); ++i) {
        	        	preparedStatement.setInt(1, sid_list.get(i));
        	        	ResultSet rset1 =preparedStatement.executeQuery();
        	        	
        	        	while(rset1.next()) {
        	        		
            	        	sum_sal=sum_sal+rset1.getInt(1);
            	        	
            	        	
            	       }
        	        	
        	        }
        	        
        	        int avg_sal = sum_sal / sid_list.size();
        	        System.out.print("\nThe average salary of the employees supervised by "+sid+" is "+avg_sal);
 	            }
        		  line = reader.readLine();
             	 }
        	 reader.close();
        	 stmt.executeUpdate("drop table if exists employee");
        	 stmt.executeUpdate("drop table if exists supervisor");
        	 stmt.close();
        	 conn.close();
         } catch(Exception e) {
        	 System.out.println("Error occured\n");
        	 e.printStackTrace();	 
         }
        
         } catch(Exception e) {
        	 System.out.println("Error occured\n");
             e.printStackTrace();
       }
   }
}