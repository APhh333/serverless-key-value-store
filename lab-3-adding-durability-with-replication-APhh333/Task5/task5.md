# 5. Benchmark your solution with YCSB https://github.com/brianfrankcooper/YCSB/wiki/Adding-a-Database https://benchant.com/blog/ycsb-custom-workloads

## Скачали і всстановили Java + Maven

![5.1.png](./5.1.png)

## Налаштування Java-проекту (Maven) 

![5.2.png](./5.2.png)

## Збірка "з'єднувача" (Binding)
![5.3.png](./5.3.png)

## Створений клієнт
![5.4.png](./5.4.png)

## Завантаження та Налаштування YCSB
![5.5.png](./5.5.png)

# Запуск Бенчмарку
![5.6.png](./5.6.png)

## YCSB-тест проходить у 2 фази:
## LOAD (Завантаження даних)
## ./bin/ycsb load basic -threads 10 -P workloads/workloada -p db.class=com.lab.ycsb.FastApiDbClient -p coordinator.url=http://localhost:8000 -p operationcount=1000 -s
![5.7.png](./5.7.png)

## RUN (Виконання тесту)
## ./bin/ycsb run basic -threads 10 -P workloads/workloada -p db.class=com.lab.ycsb.FastApiDbClient -p coordinator.url=http://localhost:8000 -p operationcount=5000 -s
![5.8.png](./5.8.png)
