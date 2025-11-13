FROM jupyter/pyspark-notebook:latest

# Instalar PostgreSQL JDBC driver
USER root
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -o /usr/local/spark/jars/postgresql-42.6.0.jar

# Copiar configuración optimizada de Spark
COPY spark-config/ /opt/spark/conf/

USER jovyan

# Instalar dependencias Python adicionales
RUN pip install psycopg2-binary requests python-dotenv