# 3. N8N Agentic Integration - Choose one or more options (max 5 points from this section):

# b. Operational Automation (2 points) Implement automated operational tasks:
## Generate daily health reports with key metrics
## Крок 1. Додаємо N8N у docker-compose.yml
![3.1.png](./3.1.png)
## Додали сервіс n8n

![3.2.png](./3.2.png)
## Налаштовано воркфлов в N8N

![3.3.png](./3.3.png)

## Було налаштовано відправку повідомлення у телеграм
## Реалізація Health Check Workflow
## Було створено workflow, який складається з таких етапів:
## Schedule Trigger: Запускає процес за розкладом (інтервал 1 хвилина для демо).
## HTTP Request: Виконує GET-запит до http://coordinator:8000/health.
## If (Condition): Перевіряє статус healthy.
## Generate Report: Формує JSON-звіт та надсилає його (або зберігає).

# (2 points) Automated incident response. Create n8n workflow that:
# Listens to your alerting system

## Налаштовуємо пересилання пакетів через NGROK
![3.4.png](./3.4.png)

![3.5.png](./3.5.png)
## Workflow для обробки вебхуків та надсилання в телеграм



 



