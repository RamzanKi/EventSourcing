# EventSourcing
Написано 2 приложения, реализующие функционал асинхронной записи данных в БД.

Приложение DataProcessor
Принимает из RabbitMQ сообщения о добавлении/удалении данных, затем выполняет
в БД соответствующие запросы.


реализовано с использованием JDBC, RabbitMQ, PostgreSQL