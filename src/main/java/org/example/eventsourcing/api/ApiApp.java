package org.example.eventsourcing.api;

import com.rabbitmq.client.ConnectionFactory;
import org.example.eventsourcing.Person;
import org.example.eventsourcing.RabbitMQUtil;


import java.util.List;
import java.util.Scanner;

public class ApiApp {
  public static void main(String[] args) throws Exception {
    ConnectionFactory connectionFactory = initMQ();
    // Тут пишем создание PersonApi, запуск и демонстрацию работы

    Scanner scanner = new Scanner(System.in);
    PersonApi personApi = new PersonApiImpl(connectionFactory);

    while (true) {
      System.out.println("Enter command (find, findAll, save, delete): ");
      String command = scanner.nextLine().trim();

      if (command.equals("find")) {
        System.out.println("Enter person ID: ");
        Long id = Long.parseLong(scanner.nextLine().trim());
        Person person = personApi.findPerson(id);
        if (person != null) {
          System.out.println("Person found: " + person);
        } else {
          System.out.println("Person not found");
        }

      } else if (command.equals("findAll")) {
        List<Person> persons = personApi.findAll();
        System.out.println("All persons: " + persons.toString());

      } else if (command.equals("save")) {
        Person person = new Person();
        System.out.println("Enter person ID: ");
        Long id = Long.parseLong(scanner.nextLine().trim());
        person.setId(id);
        System.out.println("Enter person first name: ");
        person.setName(scanner.nextLine());
        System.out.println("Enter person last name: ");
        person.setLastName(scanner.nextLine());
        System.out.println("Enter person middle name: ");
        person.setMiddleName(scanner.nextLine());

        personApi.savePerson(person.getId(), person.getName(), person.getLastName(), person.getMiddleName());
      } else if (command.equals("delete")) {
        System.out.println("Enter person ID: ");
        Long id = Long.parseLong(scanner.nextLine().trim());
        personApi.deletePerson(id);
      }
    }
  }

  private static ConnectionFactory initMQ() throws Exception {
    return RabbitMQUtil.buildConnectionFactory();
  }
}
