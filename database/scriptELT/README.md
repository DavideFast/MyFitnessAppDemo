# Script per effettuare l'ELT dei dati ricevuti dai client

This script is designed to perform the ELT (Extract, Load, Transform) of data received from clients and stored in the PostgreSQL database to the Clickhouse database intended for massive data analysis.

The script is executed in two steps:

1. **Data extraction and loading**: data is extracted from the PostgreSQL database and loaded into the Clickhouse database.
2. **Data transformation**: data is transformed in the Clickhouse database to optimize queries and data analysis.

Data is extracted from Clickhouse and not from Postgresql because Clickhouse is optimized for real-time data analysis and allows complex queries to be executed efficiently. Otherwise, the Python script itself would have to extract data from Postgresql and load it into Clickhouse, but this would result in a higher load on the server and longer execution times.

# Build Docker Image

To build the Docker image for the script, follow these steps:

```bash
cd MyFitnessAppDemo/database/scriptELT
docker build -t elt-script:latest .
```
