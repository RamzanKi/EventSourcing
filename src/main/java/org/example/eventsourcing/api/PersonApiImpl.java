package org.example.eventsourcing.api;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import org.example.eventsourcing.DbUtil;
import org.example.eventsourcing.Person;


import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;


public class PersonApiImpl implements PersonApi {

  private final DataSource dataSource = DbUtil.buildDataSource();
  private ConnectionFactory connectionFactory;

  public PersonApiImpl(ConnectionFactory connectionFactory) throws SQLException {
    this.connectionFactory = connectionFactory;
  }

  @Override
  public void deletePerson(Long personId) {
    try (com.rabbitmq.client.Connection connection = connectionFactory.newConnection()) {

      Channel channel = connection.createChannel();
      String queueName = "person-queue";

      channel.queueDeclare(queueName, false, false, false, null);

      String message = "delete;" + personId + ";" + " " + ";" + " " + ";" + " ";
      channel.basicPublish("", queueName, null, message.getBytes());

      channel.close();
    } catch (IOException | TimeoutException e) {
      System.out.println("Error in deletePerson method: " + e.getMessage());
    }
  }

  @Override
  public void savePerson(Long personId, String firstName, String lastName, String middleName) {
    try (com.rabbitmq.client.Connection connection = connectionFactory.newConnection()) {

      Channel channel = connection.createChannel();
      String queueName = "person-queue";

      channel.queueDeclare(queueName, false, false, false, null);
      if (firstName.isEmpty()){
        firstName = " ";
      }
      if (lastName.isEmpty()){
        lastName = " ";
      }
      if (middleName.isEmpty()){
        middleName = " ";
      }
      String message = "save;" + personId + ";" + firstName + ";" + lastName + ";" + middleName;
              channel.basicPublish("", queueName, null, message.getBytes());

      channel.close();
    } catch (IOException | TimeoutException e) {
      System.out.println("Error in savePerson method: " + e.getMessage());
    }
  }

  @Override
  public Person findPerson(Long personId) {
    try (Connection connection = dataSource.getConnection()) {
      PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM person WHERE person_id = ?");
      preparedStatement.setLong(1, personId);
      ResultSet resultSet = preparedStatement.executeQuery();

      if (resultSet.next()) {
        Person person = new Person(resultSet.getLong("person_id"),
                resultSet.getString("first_name"),
                resultSet.getString("last_name"),
                resultSet.getString("middle_name"));

        return person;
      }
      preparedStatement.close();
      resultSet.close();
    } catch (SQLException e) {
      System.out.println("Error in findPerson method: " + e.getMessage());
    }
    return null;
  }

  @Override
  public List<Person> findAll() {
    List<Person> personList = new ArrayList<>();
    try (Connection connection = dataSource.getConnection()) {
      PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM person");
      ResultSet resultSet = preparedStatement.executeQuery();

      while (resultSet.next()) {
        personList.add(new Person(resultSet.getLong("person_id"),
                resultSet.getString("first_name"),
                resultSet.getString("last_name"),
                resultSet.getString("middle_name")));
      }
      preparedStatement.close();
      resultSet.close();
    } catch (SQLException e) {
      System.out.println("Error in findAll method: " + e.getMessage());
    }
    return personList;
  }
}

