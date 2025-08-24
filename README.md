# Weather ETL with Airflow

This project uses **Airflow** to extract weather data from OpenWeather API and load into Postgres.  
It supports multiple cities configured in `config.json`.

## Setup

1. Clone the repo:

git clone https://github.com/dmishra2311/weather-etl-airflow.git
cd weather-etl-airflow

2. Create .env with your OpenWeather API key:

OPENWEATHER_API_KEY=your_api_key_here


3. Start Airflow + Postgres:

docker-compose up -d


4. Access Airflow UI: http://localhost:8080

Username: admin
Password: admin

5. Trigger DAG weather_etl and check tables in Postgres.

Notes

Raw JSON data stored in raw_weather table.

Transformed data stored in weather table.

Cities are configurable in config.json.


Then commit and push:

```bash
git add README.md .gitignore
git commit -m "Add README and gitignore"
git push
