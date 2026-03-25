# Now Sharding: 
## Shard writes. Coordinator calculates hash and based on it selects one of
## downstream shards. Shards are registered in the coordinator upon start.
## (API needs to be added). Use consistent hashing or similar algorithms 
## for dynamic updates. (3 points)
## Shard reads (2 points)

# Приклад запуску контейнерів
![3.1.png](./3.1.png)

## API Coordinator With Sharding
![3.2.png](./3.2.png)

## Методи керування шардами 
![3.3.png](./3.3.png)

## Create record
![3.4.png](./3.4.png)

## Shards з доданим методом Update
![3.5.png](./3.5.png)

![3.6.png](./3.6.png)