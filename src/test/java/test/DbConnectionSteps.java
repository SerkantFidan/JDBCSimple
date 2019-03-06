package test;

import org.junit.*;
import java.sql.*;

public class DbConnectionSteps {

    @Test
    public void jdbcTest1() throws SQLException {

        String url = "jdbc:postgresql://serpostgresql.c3wgvtooxxut.us-east-2.rds.amazonaws.com:5432/postgres";
        String username = "sdet";
        String password = "sdet12345";

        String sql="select first_name from employees order by employee_id";



        Connection connection = DriverManager.getConnection(    url, username, password);

        Statement statement = connection.createStatement(      ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        ResultSet resultSet = statement.executeQuery(sql);

        // iteration to next rows
        resultSet.next();
        System.out.println("--- Row Number ");
        System.out.println(resultSet.getRow());
        resultSet.next();
        System.out.println("--- Row Number ");
        System.out.println(resultSet.getRow());


        //lest get first row number
        resultSet.first();
        System.out.println("---First Row Number ");
        System.out.println(resultSet.getRow());

        //lets get first_name column first rows value
        resultSet.first();
        System.out.println("---First Row  first_name Column Value ");
        System.out.println(resultSet.getRow() + " :: " + resultSet.getObject("first_name"));


        //lets get first_name column all rows values
        // we need to go before first to reach first value

        System.out.println(" ");
        resultSet.beforeFirst();
        System.out.println("---First Row  first_name Column Value ");

        while(resultSet.next()) {
            System.out.println(resultSet.getRow()+" :: "+resultSet.getObject("first_name"));
        }
        //lets get last column first_name column value
        System.out.println(" ");
        resultSet.last();
        System.out.println("---Last Row  first_name Column Value ");
        System.out.println(resultSet.getRow() + " :: " + resultSet.getObject("first_name"));


        //lets get determined ,absolute, column first_name column value
        System.out.println(" ");
        resultSet.absolute(106);
        System.out.println("---Absolute Row  first_name Column Value ");
        System.out.println(resultSet.getRow() + " :: " + resultSet.getObject("first_name"));



        //Metadata about data base
        System.out.println(" ");
        System.out.println("--MetaData of DataBase ");
        DatabaseMetaData dbMetaData = connection.getMetaData();
        System.out.println("Username: " + dbMetaData.getUserName());
        String expcetedDBType = "PostgreSQL";
        String actualDBType = dbMetaData.getDatabaseProductName();
        Assert.assertEquals(expcetedDBType, actualDBType);
        System.out.println("Data Base Type  ;"+actualDBType+" : : "  + dbMetaData.getDatabaseProductVersion());


        //Metadata about resultset, means metadata about or query
        System.out.println(" ");
        System.out.println("--MetaData of ResultSet ");
        resultSet = statement.executeQuery(sql);
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        System.out.println(resultSetMetaData.getColumnCount());
        System.out.println(resultSetMetaData.getColumnName(1));


        System.out.println(" ");
        sql="select * from employees order by employee_id";
        resultSet = statement.executeQuery(sql);
        resultSetMetaData = resultSet.getMetaData();
        int numOfComun = resultSetMetaData.getColumnCount();
        for(int i = 1 ; i <= numOfComun ; i ++) {
            System.out.println("Name of a specific column with index : "+i +"-"+resultSetMetaData.getColumnName(i));
        }


        connection.close();
    }
}