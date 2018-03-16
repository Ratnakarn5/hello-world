package edu.knoldus

import com.datastax.driver.core.Session
import edu.knoldus.util.CassandraProvider
import scala.collection.JavaConverters._


object CassandraOperation extends  CassandraProvider {

  def main(args: Array[String]): Unit = {


    // Retrieve all User details from Users table

    println("\n\n*********Retrieve User Data Example *************")
    getUsersAllDetails(cassandraSession)

    // Insert new User into users table
    println("\n\n*********Insert User Data Example *************")

    insertRow(cassandraSession)
    getUsersAllDetails(cassandraSession)
    updateRecord(cassandraSession)

//    insertRow(cassandraSession)
    getUsersAllDetails(cassandraSession)
//    getDataById(cassandraSession)

//    getdataSalary(cassandraSession)
    getdataCity(cassandraSession)
//    deleteRecord(cassandraSession)
    getUsersAllDetails(cassandraSession)
    // Close Cluster and Session objects
    println("\n\nIs Cluster Closed :" + cassandraSession.isClosed)
    println("\n\nIs Session Closed :" + cassandraSession.isClosed)
    cassandraSession.close()
    println("\n\nIs Cluster Closed :" + cassandraSession.isClosed)
    println("\n\nIs Session Closed :" + cassandraSession.isClosed)

     def getUsersAllDetails(inSession: Session): Unit = {
         inSession.execute(s"CREATE TABLE IF NOT EXISTS employee (emp_id int, emp_name text,emp_city text,emp_salary varint, emp_phone varint,  primary key(emp_id, emp_salary, emp_city)) ")

      // Use select to get the users table data
      val results = inSession.execute("SELECT * FROM employee").asScala.toList
      for (row <- results) {
        println("data: -" + row)
      }
    }

    def insertRow(session: Session): Unit ={
      session.execute(
        "INSERT INTO employee (emp_id, emp_name, emp_city, emp_salary, emp_phone)   " +
          "VALUES (101,'priyanka', 'delhi', 100000, 9911676745)")


      session.execute("INSERT INTO employee (emp_id, emp_name, emp_city, emp_salary, emp_phone)   " +
        "VALUES (102,'neha', 'delhi', 100000, 9911679856)");


      session.execute("INSERT INTO employee (emp_id, emp_name, emp_city, emp_salary, emp_phone)   " +
        "VALUES (103,'himanshu', 'delhi', 100000, 991123256)");

      session.execute("INSERT INTO employee (emp_id, emp_name, emp_city, emp_salary, emp_phone) "+
        "VALUES (104,'kritika', 'delhi', 100000, 891123256)");
      println("\n\n*********Retrieve User Data Example *************")
    }

    def getDataById(session: Session): Unit = {
      val results = session.execute("SELECT * FROM employee where emp_id = 102 and emp_city='delhi'").asScala.toList
      for (row <- results) {
        println("data: -" + row)
      }
    }

    def updateRecord(session: Session): Unit = {
      session.execute("DELETE FROM employee WHERE emp_id=104 and emp_salary = 100000 and emp_city='delhi' IF EXISTS;")
      session.execute("INSERT INTO employee (emp_id, emp_name, emp_city, emp_salary, emp_phone) "+
        "VALUES (104,'kritika', 'chandigarh', 54000, 891123256)");
    }
    def setIndex(session: Session): Unit = {
      session.execute("CREATE INDEX IF NOT EXISTS " +
        "ON employee(emp_city)");
    }
    def getdataSalary(session: Session): Unit = {
      val results = session.execute("SELECT * FROM employee where emp_id = 104 and emp_salary > 3000").asScala.toList
      for (row <- results) {
        println("data: -" + row)
      }
    }

    def getdataCity(session: Session): Unit = {
      val results = session.execute("SELECT * FROM employee where emp_city ='chandigarh' ").asScala.toList
      for (row <- results) {
        println("data: -" + row)
      }
    }
    def deleteRecord(session: Session): Unit = {
      session.execute("DELETE FROM employee WHERE emp_id=104 and emp_salary = 54000 and emp_city ='chandigarh' IF EXISTS;")
    }
  }
}
