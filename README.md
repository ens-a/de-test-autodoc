#### Для запуска проекта необходимо выполнить шаги:
 
  1. Билдим Clickhouse и airflow из docker_compose:
     ```shell
     docker-compose build
     ```
  2. Инициализируем Airflof scheduler, БД и другие конфиги:
     ```shell
     docker-compose up airflow-init
     ```
  3. Запускаем сервисы:
     ```shell
     docker-compose up
     ```

  2. Делаем инсерт днных в Clickhouse:
     ```shell
     docker exec -i my-clickhouse-server clickhouse-client --query "INSERT INTO stocks FORMAT CSVWithNames" < ./clickhouse/data/stocks.csv
     docker exec -i my-clickhouse-server clickhouse-client --query "INSERT INTO sales FORMAT CSVWithNames" < ./clickhouse/data/sales.csv
     ```