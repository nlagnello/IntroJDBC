/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bookSample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 *
 * @author Nick
 */
public class MySqlDb{
    
    private Connection conn;
    
    
    public void openConnection(String driverClass, String url, String userName, String password) throws Exception{
        Class.forName (driverClass);
	conn = DriverManager.getConnection(url, userName, password);
    }
    public void closeConnection() throws SQLException{
        conn.close();
    }
    
    public List<Map<String,Object>> findAllRecords(String tableName) throws SQLException {
        
        List<Map<String,Object>> records = new ArrayList<>();
        
        String sql = "SELECT * FROM " + tableName;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        while(rs.next()){
            Map<String, Object> record = new HashMap<>();
            for(int i=1; i<= columnCount; i++){
                record.put(metaData.getColumnName(i), rs.getObject(i));
            }
            records.add(record); 
        }
        
        return records;
    }
    
    public boolean deleteOneRecord(String tableName, String key, String value) throws SQLException {
        
        List<Map<String,Object>> records = new ArrayList<>();
        records = findAllRecords(tableName);
        Statement stmt = conn.createStatement();
        String sql = "DELETE FROM " + tableName + " WHERE " + key + " = " + value;
        stmt.executeUpdate(sql);
        
        return true;
    }
    
    public boolean createOneRecord(String tableName, ArrayList newRecord) throws SQLException {
        
        
        Object[] recordValueArray = newRecord.toArray();
        String recordValueString = recordValueArray[0].toString();
        for(int i = 1; i < recordValueArray.length; i++){
            recordValueString += ", '" + recordValueArray[i].toString() + "'"; 
                    
        }
        
        Statement stmt = conn.createStatement();
        String sql = "INSERT INTO " + tableName + " VALUES (" + recordValueString + ")";
        
        stmt.executeUpdate(sql);
        
        return true;
    }
    
    public boolean updateOneRecord(String tableName, Map<String,Object> currentRecord,Map<String,Object> updateRecord) throws SQLException {
        List<Map<String,Object>> records = new ArrayList<>();
        Map<String, Object> record = updateRecord;
        records = findAllRecords(tableName);
        Object[] recordKeyArray = record.keySet().toArray();
        Object[] recordObjectArray = record.values().toArray();
        String key = currentRecord.keySet().toArray()[0].toString();
        String value = currentRecord.values().toArray()[0].toString();
        String recordString = recordKeyArray[0].toString() + "= '" + recordObjectArray[0].toString() + "'";
        for(int i = 1; i < recordKeyArray.length-1; i++){
            recordString += "," + recordKeyArray[i].toString() + "= '" + recordObjectArray[i].toString() + "'";
        }
        Statement stmt = conn.createStatement();
        String sql = "UPDATE " + tableName + " SET "  + recordString + " WHERE " + key + " = '" + value + "'";
        stmt.executeUpdate(sql);
        
        return true;
    }
    
    public static void main(String[] args) throws Exception {
        MySqlDb db = new MySqlDb();
        db.openConnection("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/book", "root", "admin");
        System.out.println("R - Read: \n");
        List<Map<String, Object>> records = db.findAllRecords("author");
        
        for(Map record:records){
            System.out.println(record);
        }
        
        System.out.println("D - Delete:");
        
//        db.deleteOneRecord("author", "author_name", "'John Doe'");
//        
//        records = db.findAllRecords("author");
//        for(Map record:records){
//            System.out.println(record);
//        }
        System.out.println("Delete = Working");
        System.out.println("C - Create:");
        
//        ArrayList newRecord = new ArrayList();
//        newRecord.add("NULL");
//        newRecord.add("Jared Kowalske");
//        newRecord.add("1991-08-27");
//        db.createOneRecord("author", newRecord);
//        
//        records = db.findAllRecords("author");
//        for(Map record:records){
//            System.out.println(record);
//        }
        System.out.println("Create = Working");
        System.out.println("U - Update:");
//        Map<String,Object> updateRecord = new HashMap();
//        Map<String,Object> currentRecord = new HashMap();
//        currentRecord.put("author_id", "3");
//        updateRecord.put("author_name", "John Smith");
//        db.updateOneRecord("author", currentRecord,updateRecord);
//        
//        records = db.findAllRecords("author");
//        for(Map record:records){
//            System.out.println(record);
//        }
        
        System.out.println("Update = Working");
        
        db.closeConnection();
    }
    
    
}
