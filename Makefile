install:
	@pip install -r requirements.txt
	@pre-commit install
	@npm install -g aws-cdk-local aws-cdk

up:
	@docker-compose up -d

down:
	@docker compose down

stop:
	@docker compose down

ps:
	@docker compose ps

build:
	@cdklocal bootstrap
	@cdklocal deploy AGHADynamoDBStack

loaddata:
	@python test_data/insert_data.py

