Требования к системе
====================

- Несколько гигабайт места на диске
- ОС - Linux или WSL
- Установленный docker, docker-compose

Установка
=========

`./create_db` - запустить докер, загрузить схему бд и сгенерировать тестовые данные и выполнить тесты
`./test_db` - выполнить тесты

Исследование данных
===================

Выполнять примеры запросы из файла [select_queries.sql](select_queries.sql)

Прочие файлы
============

[schema.sql](schema.sql) - описания таблиц, функций, денормализации
[data_generator.sql](data_generator.sql) - генератор тестовых данных
[tests.sql](tests.sql) - автотесты на денормализацию