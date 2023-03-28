package org.example.eventsourcing.db;

import com.rabbitmq.client.*;
import org.example.eventsourcing.DbUtil;
import org.example.eventsourcing.RabbitMQUtil;


import javax.sql.DataSource;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

public class DbApp {
  private static final String QUEUE_NAME = "person-queue";

  public static void main(String[] args) throws Exception {
    DataSource dataSource = initDb();
    ConnectionFactory connectionFactory = initMQ();

    // тут пишем создание и запуск приложения работы с БД
    connectionFactory.setHost("localhost");

      try (Connection connection = connectionFactory.newConnection();
           Channel channel = connection.createChannel()) {

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        while (true) {
          DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");

            String[] split = message.split(";");
            String action = split[0];
            Long id = Long.parseLong(split[1]);
            String firstName = split[2];
            String lastName = split[3];
            String middleName = split[4];

            if (action.equals("save")) {
              savePerson(id, firstName, lastName, middleName, dataSource);
            }
            else if (action.equals("delete")) {
              deletePersonById(id, dataSource);
            }
          };

          channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
          });
        }

      } catch (IOException | TimeoutException e) {
        System.out.println("Error setting up RabbitMQ: " + e.getMessage());
      }
    }

  public static void savePerson(Long id, String firstName, String lastName, String middleName, DataSource dataSource) {
    String checkPersonSql = "SELECT * FROM person WHERE person_id = ?";

    try (java.sql.Connection dbConnection = dataSource.getConnection();
         PreparedStatement checkStatement = dbConnection.prepareStatement(checkPersonSql)) {

      checkStatement.setLong(1, id);
      ResultSet resultSet = checkStatement.executeQuery();

      if (resultSet.next()) {
        // пользователь с таким id уже существует - выполнить обновление данных
        updatePerson(id, firstName, lastName, middleName, dbConnection);
      } else {
        // пользователь не существует - добавить в базу нового
        addPerson(id, firstName, lastName, middleName, dbConnection);
      }
    } catch (SQLException ex) {
      System.out.println("Error inserting/updating person into database: " + ex.getMessage());
    }
  }

  public static void updatePerson(Long id, String firstName, String lastName, String middleName, java.sql.Connection dbConnection) throws SQLException {
    String updatePersonSql = "UPDATE person SET first_name = ?, last_name = ?, middle_name = ? WHERE person_id = ?";

    try (PreparedStatement updateStatement = dbConnection.prepareStatement(updatePersonSql)) {
      updateStatement.setString(1, firstName);
      updateStatement.setString(2, lastName);
      updateStatement.setString(3, middleName);
      updateStatement.setLong(4, id);
      updateStatement.executeUpdate();
    }

  }

  public static void addPerson(Long id, String firstName, String lastName, String middleName, java.sql.Connection dbConnection) throws SQLException {
    String insertPersonSql = "INSERT INTO person(person_id, first_name, last_name, middle_name) " +
            "VALUES (?, ?, ?, ?)";
    try (PreparedStatement insertStatement = dbConnection.prepareStatement(insertPersonSql)) {
      insertStatement.setLong(1, id);
      insertStatement.setString(2, firstName);
      insertStatement.setString(3, lastName);
      insertStatement.setString(4, middleName);
      insertStatement.executeUpdate();
    }
  }

  public static void deletePersonById(Long id, DataSource dataSource) {
    String deletePersonSql = "DELETE FROM person WHERE person_id = ?";
    try (java.sql.Connection dbConnection = dataSource.getConnection();
         PreparedStatement statement = dbConnection.prepareStatement(deletePersonSql)) {
      statement.setLong(1, id);
      int rowsAffected = statement.executeUpdate();
      if (rowsAffected == 0) {
        System.out.println("Person with id " + id + " does not exist");
      } else {
        System.out.println("Person with id " + id + " has been deleted");
      }
    } catch (SQLException ex) {
      System.out.println("Error deleting person from database");
    }
  }





  private static ConnectionFactory initMQ() throws Exception {
    return RabbitMQUtil.buildConnectionFactory();
  }
  
  private static DataSource initDb() throws SQLException {
    String ddl = ""
                     + "drop table if exists person;"
                     + "create table if not exists person (\n"
                     + "person_id bigint primary key,\n"
                     + "first_name varchar,\n"
                     + "last_name varchar,\n"
                     + "middle_name varchar\n"
                     + ")";
    DataSource dataSource = DbUtil.buildDataSource();
    DbUtil.applyDdl(ddl, dataSource);
    return dataSource;
  }
}
