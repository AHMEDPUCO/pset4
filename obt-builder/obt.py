#!/usr/bin/env python3
"""
build_obt_optimized.py - One Big Table Builder OPTIMIZADO para NYC TLC Taxi Trips

Modos de ejecución:
- FULL: Reconstruye la OBT completa (2015-2025)
- BY-PARTITION: Procesa solo particiones específicas (por año/mes)

Uso:
    python build_obt_optimized.py --mode full --run-id obt_20250103
    python build_obt_optimized.py --mode by-partition --year-start 2023 --year-end 2024 --months 1,2 --run-id obt_partial
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime
from typing import List
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import io


class OptimizedOBTBuilder:
    """Constructor OPTIMIZADO de One Big Table para NYC TLC Taxi Trips."""
    
    def __init__(self):
        """Inicializa el builder con configuración desde variables de ambiente."""
        self.pg_host = os.getenv('PG_HOST', 'postgres')
        self.pg_port = os.getenv('PG_PORT', '5432')
        self.pg_db = os.getenv('PG_DB', 'nyc_tlc')
        self.pg_user = os.getenv('PG_USER', 'postgres')
        self.pg_password = os.getenv('PG_PASSWORD', 'postgres')
        self.schema_raw = os.getenv('PG_SCHEMA_RAW', 'raw')
        self.schema_analytics = os.getenv('PG_SCHEMA_ANALYTICS', 'analytics')
        self.table_name = 'obt_trips'
        
        # Configuración de performance
        self.batch_size = int(os.getenv('OBT_BATCH_SIZE', '500000'))
        self.parallel_workers = int(os.getenv('OBT_PARALLEL_WORKERS', '8'))
        
        self.conn = None
        self.cursor = None
        
        # Configurar logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('obt_builder.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        """Establece conexión a PostgreSQL con algunas optimizaciones de performance."""
        try:
            self.conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                database=self.pg_db,
                user=self.pg_user,
                password=self.pg_password,
                application_name="OBT_Builder_Optimized"
            )
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.conn.cursor()
            
            # Ajustes razonables de performance
            self.cursor.execute("SET work_mem = '512MB';")
            self.cursor.execute("SET maintenance_work_mem = '512MB';")
            self.cursor.execute("SET effective_cache_size = '4GB';")
            self.cursor.execute(f"SET max_parallel_workers_per_gather = {self.parallel_workers};")
            self.cursor.execute("SET max_parallel_workers = 16;")
            self.cursor.execute("SET synchronous_commit = OFF;")
            
            self.logger.info(f"✓ Conectado a {self.pg_user}@{self.pg_host}:{self.pg_port}/{self.pg_db}")
        except Exception as e:
            self.logger.error(f"✗ Error de conexión: {e}")
            sys.exit(1)
    
    def close(self):
        """Cierra la conexión a PostgreSQL."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.logger.info("✓ Conexión cerrada")
    
    def create_analytics_schema(self):
        """Crea el esquema analytics si no existe."""
        query = f"CREATE SCHEMA IF NOT EXISTS {self.schema_analytics};"
        self.cursor.execute(query)
        self.logger.info(f"✓ Esquema '{self.schema_analytics}' verificado/creado")
    
    def drop_obt_table(self):
        """Elimina la tabla OBT si existe (para full rebuild)."""
        views = ['obt_trips_with_metrics', 'obt_trips_denormalized']
        for view in views:
            try:
                self.cursor.execute(
                    f"DROP VIEW IF EXISTS {self.schema_analytics}.{view} CASCADE;"
                )
            except Exception:
                pass
        
        query = f"DROP TABLE IF EXISTS {self.schema_analytics}.obt_trips CASCADE;"
        self.cursor.execute(query)
        self.logger.info(f"✓ Tabla {self.schema_analytics}.obt_trips eliminada")
    
    def create_obt_table(self):
        """
        Crea la estructura de la tabla OBT.
        SOLO campos base - métricas derivadas en columnas calculadas.
        """
        query = f"""
        CREATE UNLOGGED TABLE IF NOT EXISTS {self.schema_analytics}.obt_trips (
            -- Identificador único del viaje
            trip_id BIGSERIAL PRIMARY KEY,
            
            -- Tiempo (campos base)
            pickup_datetime TIMESTAMP NOT NULL,
            dropoff_datetime TIMESTAMP NOT NULL,
            pickup_hour INT,
            pickup_dow INT,
            pickup_month INT,
            pickup_year INT,
            
            -- Ubicación pickup
            pu_location_id INT,
            pu_zone VARCHAR(255),
            pu_borough VARCHAR(100),
            pu_service_zone VARCHAR(100),
            
            -- Ubicación dropoff
            do_location_id INT,
            do_zone VARCHAR(255),
            do_borough VARCHAR(100),
            do_service_zone VARCHAR(100),
            
            -- Servicio y códigos
            service_type VARCHAR(10),
            vendor_id INT,
            vendor_name VARCHAR(50),
            rate_code_id INT,
            rate_code_desc VARCHAR(50),
            payment_type INT,
            payment_type_desc VARCHAR(50),
            trip_type INT,
            store_and_fwd_flag VARCHAR(1),
            
            -- Viaje y montos (CAMPOS BASE para cálculos)
            passenger_count INT,
            trip_distance NUMERIC(10, 2),
            fare_amount NUMERIC(10, 2),
            extra NUMERIC(10, 2),
            mta_tax NUMERIC(10, 2),
            tip_amount NUMERIC(10, 2),
            tolls_amount NUMERIC(10, 2),
            improvement_surcharge NUMERIC(10, 2),
            congestion_surcharge NUMERIC(10, 2),
            airport_fee NUMERIC(10, 2),
            total_amount NUMERIC(10, 2),
            
            -- Metadatos
            run_id VARCHAR(100),
            source_year INT,
            source_month INT,
            ingested_at_utc TIMESTAMP,
            obt_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (
            fillfactor = 95,
            autovacuum_vacuum_scale_factor = 0.02,
            autovacuum_analyze_scale_factor = 0.01,
            autovacuum_vacuum_cost_delay = 10
        );
        """
        
        self.cursor.execute(query)
        self.logger.info("✓ Tabla OBT base creada (UNLOGGED)")
    
    def create_metrics_view(self):
        """
        VERSIÓN IDEMPOTENTE:
        Crea columnas calculadas en la tabla OBT si no existen.
        """
        try:
            check_columns_query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s 
            AND column_name IN ('trip_duration_min', 'trip_speed_mph', 'is_weekend', 'hour_of_day')
            """
            
            logger = self.logger
            self.cursor.execute(check_columns_query, (self.schema_analytics, self.table_name))
            existing_columns = [row[0] for row in self.cursor.fetchall()]
            
            logger.info(
                f"📊 Columnas existentes en {self.schema_analytics}.{self.table_name}: "
                f"{existing_columns}"
            )
            
            columns_to_add = []
            
            if 'trip_duration_min' not in existing_columns:
                columns_to_add.append("""
                trip_duration_min NUMERIC(10,2) GENERATED ALWAYS AS (
                    CASE 
                        WHEN pickup_datetime IS NOT NULL AND dropoff_datetime IS NOT NULL 
                        THEN EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60 
                        ELSE NULL 
                    END
                ) STORED
                """)
            
            if 'trip_speed_mph' not in existing_columns:
                columns_to_add.append("""
                trip_speed_mph NUMERIC(10,2) GENERATED ALWAYS AS (
                    CASE 
                        WHEN trip_duration_min > 0 AND trip_distance > 0 
                        THEN (trip_distance / (trip_duration_min / 60))
                        ELSE NULL 
                    END
                ) STORED
                """)
            
            if 'is_weekend' not in existing_columns:
                columns_to_add.append("""
                is_weekend BOOLEAN GENERATED ALWAYS AS (
                    EXTRACT(DOW FROM pickup_datetime) IN (0, 6)
                ) STORED
                """)
            
            if 'hour_of_day' not in existing_columns:
                columns_to_add.append("""
                hour_of_day INTEGER GENERATED ALWAYS AS (
                    EXTRACT(HOUR FROM pickup_datetime)
                ) STORED
                """)
            
            if columns_to_add:
                alter_query = f"""
                ALTER TABLE {self.schema_analytics}.{self.table_name}
                ADD COLUMN {', ADD COLUMN '.join(columns_to_add)};
                """
                
                logger.info(f"🔧 Agregando {len(columns_to_add)} columnas calculadas...")
                self.cursor.execute(alter_query)
                logger.info("✅ Columnas calculadas agregadas exitosamente")
            else:
                logger.info("✅ Todas las columnas calculadas ya existen")
            
            # Índices adicionales sobre columnas calculadas
            self.create_optimized_indexes()
            
        except Exception as e:
            self.logger.error(f"❌ Error en create_metrics_view: {e}")
            raise
    
    def create_optimized_indexes(self):
        """Crear índices optimizados para consultas comunes sobre columnas calculadas."""
        indexes = [
            f"idx_{self.table_name}_pickup_date",
            f"idx_{self.table_name}_service_type",
            f"idx_{self.table_name}_location_combo",
            f"idx_{self.table_name}_trip_duration"
        ]
        
        for index_name in indexes:
            try:
                check_index_query = """
                SELECT 1 FROM pg_indexes 
                WHERE schemaname = %s AND tablename = %s AND indexname = %s
                """
                self.cursor.execute(
                    check_index_query,
                    (self.schema_analytics, self.table_name, index_name)
                )
                
                if not self.cursor.fetchone():
                    if index_name == f"idx_{self.table_name}_pickup_date":
                        self.cursor.execute(f"""
                        CREATE INDEX {index_name} 
                        ON {self.schema_analytics}.{self.table_name} (date(pickup_datetime))
                        """)
                    elif index_name == f"idx_{self.table_name}_service_type":
                        self.cursor.execute(f"""
                        CREATE INDEX {index_name} 
                        ON {self.schema_analytics}.{self.table_name} (service_type)
                        """)
                    elif index_name == f"idx_{self.table_name}_location_combo":
                        self.cursor.execute(f"""
                        CREATE INDEX {index_name} 
                        ON {self.schema_analytics}.{self.table_name} (pu_location_id, do_location_id)
                        """)
                    elif index_name == f"idx_{self.table_name}_trip_duration":
                        self.cursor.execute(f"""
                        CREATE INDEX {index_name} 
                        ON {self.schema_analytics}.{self.table_name} (trip_duration_min)
                        WHERE trip_duration_min IS NOT NULL
                        """)
                    
                    self.logger.info(f"✅ Índice creado: {index_name}")
                else:
                    self.logger.debug(f"✅ Índice ya existe: {index_name}")
                    
            except Exception as e:
                self.logger.warning(f"⚠️  No se pudo crear índice {index_name}: {e}")
    
    def create_indexes(self):
        """Crea índices adicionales para acelerar consultas sobre analytics.obt_trips."""
        self.logger.info("Creando índices en analytics.obt_trips...")
        
        indexes = [
            # Índices para filtros temporales
            f"CREATE INDEX IF NOT EXISTS idx_obt_pickup_datetime "
            f"ON {self.schema_analytics}.obt_trips(pickup_datetime)",
            
            f"CREATE INDEX IF NOT EXISTS idx_obt_year_month "
            f"ON {self.schema_analytics}.obt_trips(pickup_year, pickup_month)",
            
            # Índices para filtros de negocio
            f"CREATE INDEX IF NOT EXISTS idx_obt_service_type "
            f"ON {self.schema_analytics}.obt_trips(service_type)",
            
            f"CREATE INDEX IF NOT EXISTS idx_obt_pu_borough "
            f"ON {self.schema_analytics}.obt_trips(pu_borough)",
            
            f"CREATE INDEX IF NOT EXISTS idx_obt_payment_type "
            f"ON {self.schema_analytics}.obt_trips(payment_type)",
            
            # Índices compuestos para queries analíticas
            f"CREATE INDEX IF NOT EXISTS idx_obt_service_year_borough "
            f"ON {self.schema_analytics}.obt_trips(service_type, pickup_year, pu_borough)",
            
            f"CREATE INDEX IF NOT EXISTS idx_obt_datetime_service "
            f"ON {self.schema_analytics}.obt_trips(pickup_datetime, service_type)",
            
            # Índices para joins
            f"CREATE INDEX IF NOT EXISTS idx_obt_pu_location "
            f"ON {self.schema_analytics}.obt_trips(pu_location_id)",
            
            f"CREATE INDEX IF NOT EXISTS idx_obt_do_location "
            f"ON {self.schema_analytics}.obt_trips(do_location_id)"
        ]
        
        for idx_query in indexes:
            try:
                t0 = time.time()
                self.cursor.execute(idx_query)
                dt = time.time() - t0
                self.logger.info(f"  ✓ Índice creado/ya existente en {dt:.1f}s")
            except Exception as e:
                self.logger.warning(f"  ⚠ Error creando índice: {e}")
        
        self.logger.info("✓ Creación de índices completada")
    
    def build_obt_query(self, service: str, year_start: int = None, 
                        year_end: int = None, months: List[int] = None) -> str:
        """Construye la query SQL para insertar en OBT desde raw."""
        
        if service == 'yellow':
            pickup_col = '"tpep_pickup_datetime"'
            dropoff_col = '"tpep_dropoff_datetime"'
            airport_fee_col = 'COALESCE(t."airport_fee", 0)'
        else:  # green
            pickup_col = '"lpep_pickup_datetime"'
            dropoff_col = '"lpep_dropoff_datetime"'
            airport_fee_col = '0'
        
        query = f"""
        SELECT
            -- Tiempo (base)
            t.{pickup_col} as pickup_datetime,
            t.{dropoff_col} as dropoff_datetime,
            EXTRACT(HOUR FROM t.{pickup_col})::INT as pickup_hour,
            EXTRACT(DOW FROM t.{pickup_col})::INT as pickup_dow,
            EXTRACT(MONTH FROM t.{pickup_col})::INT as pickup_month,
            EXTRACT(YEAR FROM t.{pickup_col})::INT as pickup_year,
            
            -- Ubicación pickup
            t."PULocationID" as pu_location_id,
            pu.zone as pu_zone,
            pu.borough as pu_borough,
            pu.service_zone as pu_service_zone,
            
            -- Ubicación dropoff
            t."DOLocationID" as do_location_id,
            "do".zone as do_zone,
            "do".borough as do_borough,
            "do".service_zone as do_service_zone,
            
            -- Servicio y códigos
            '{service}' as service_type,
            t."VendorID" as vendor_id,
            CASE t."VendorID"
                WHEN 1 THEN 'Creative Mobile Technologies'
                WHEN 2 THEN 'VeriFone Inc.'
                WHEN 4 THEN 'Other'
                ELSE 'Unknown'
            END as vendor_name,
            t."RatecodeID" as rate_code_id,
            CASE t."RatecodeID"
                WHEN 1 THEN 'Standard rate'
                WHEN 2 THEN 'JFK'
                WHEN 3 THEN 'Newark'
                WHEN 4 THEN 'Nassau or Westchester'
                WHEN 5 THEN 'Negotiated fare'
                WHEN 6 THEN 'Group ride'
                ELSE 'Unknown'
            END as rate_code_desc,
            t."payment_type",
            CASE t."payment_type"
                WHEN 1 THEN 'Credit card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided trip'
                ELSE 'Unknown'
            END as payment_type_desc,
            { 't."trip_type"' if service == 'green' else 'NULL::INT' } as trip_type,
            t."store_and_fwd_flag",
            
            -- Viaje y montos (base, con límites para calidad)
            LEAST(GREATEST(t."passenger_count", 1), 6) as passenger_count,
            LEAST(t."trip_distance", 100) as trip_distance,
            LEAST(t."fare_amount", 500) as fare_amount,
            LEAST(GREATEST(t."extra", 0), 50) as extra,
            LEAST(GREATEST(t."mta_tax", 0), 10) as mta_tax,
            LEAST(GREATEST(t."tip_amount", 0), 200) as tip_amount,
            LEAST(GREATEST(t."tolls_amount", 0), 100) as tolls_amount,
            LEAST(GREATEST(t."improvement_surcharge", 0), 10) as improvement_surcharge,
            LEAST(GREATEST(COALESCE(t."congestion_surcharge", 0), 0), 50) as congestion_surcharge,
            LEAST(GREATEST({airport_fee_col}, 0), 50) as airport_fee,
            LEAST(t."total_amount", 1000) as total_amount,
            
            -- Metadatos
            t."run_id",
            t."source_year",
            t."source_month",
            t."ingested_at_utc"::TIMESTAMP as ingested_at_utc
            
        FROM {self.schema_raw}.{service}_taxi_trip t
        LEFT JOIN {self.schema_raw}.taxi_zone_lookup pu 
            ON t."PULocationID" = pu.locationid
        LEFT JOIN {self.schema_raw}.taxi_zone_lookup "do" 
            ON t."DOLocationID" = "do".locationid
        WHERE 1=1
            AND t.{pickup_col} < t.{dropoff_col}
            AND EXTRACT(EPOCH FROM (t.{dropoff_col} - t.{pickup_col})) BETWEEN 60 AND 86400
            AND t."trip_distance" > 0 AND t."trip_distance" < 500
            AND t."fare_amount" > 0 AND t."fare_amount" < 1000
            AND t."total_amount" > 0 AND t."total_amount" < 2000
            AND t."passenger_count" BETWEEN 1 AND 9
            AND (t."trip_distance" / NULLIF(
                    EXTRACT(EPOCH FROM (t.{dropoff_col} - t.{pickup_col})) / 3600.0, 0
                )) < 200
            AND t."tip_amount" >= 0
        """
        
        if year_start and year_end:
            query += f' AND t."source_year" BETWEEN {year_start} AND {year_end}'
        
        if months:
            months_str = ','.join(map(str, months))
            query += f' AND t."source_month" IN ({months_str})'
        
        return query
    
    def bulk_insert_partition(self, service: str, year: int, month: int, 
                              target_table: str = None) -> int:
        """
        INSERCIÓN usando COPY. Retorna el número de filas insertadas.
        """
        if target_table is None:
            target_table = f"{self.schema_analytics}.obt_trips"
        
        source_query = self.build_obt_query(service, year, year, [month])
        
        copy_query = f"COPY ({source_query}) TO STDOUT WITH CSV;"
        
        insert_query = f"""
        COPY {target_table} (
            pickup_datetime, dropoff_datetime, pickup_hour, pickup_dow, 
            pickup_month, pickup_year,
            pu_location_id, pu_zone, pu_borough, pu_service_zone,
            do_location_id, do_zone, do_borough, do_service_zone,
            service_type, vendor_id, vendor_name, rate_code_id, rate_code_desc,
            payment_type, payment_type_desc, trip_type, store_and_fwd_flag,
            passenger_count, trip_distance, fare_amount, extra, mta_tax,
            tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge,
            airport_fee, total_amount,
            run_id, source_year, source_month, ingested_at_utc
        ) FROM STDIN WITH CSV;
        """
        
        try:
            t0 = time.time()
            
            with io.StringIO() as csv_buffer:
                self.cursor.copy_expert(copy_query, csv_buffer)
                csv_buffer.seek(0)
                self.cursor.copy_expert(insert_query, csv_buffer)
            
            dt = time.time() - t0
            
            count_query = f"""
            SELECT COUNT(*) FROM {target_table} 
            WHERE service_type = %s AND source_year = %s AND source_month = %s;
            """
            self.cursor.execute(count_query, (service, year, month))
            row_count = self.cursor.fetchone()[0]
            
            self.logger.info(
                f"  ✓ {service} {year}-{month:02d}: {row_count:,} filas en {dt:.1f}s "
                f"({row_count/dt:.0f} filas/seg)"
            )
            
            return row_count
        
        except Exception as e:
            self.logger.error(f"  ✗ Error en bulk insert {service} {year}-{month:02d}: {e}")
            return 0
    
    def check_partition_exists(self, service: str, year: int, month: int) -> bool:
        """Verifica si una partición ya existe en OBT."""
        query = f"""
        SELECT COUNT(*) 
        FROM {self.schema_analytics}.obt_trips
        WHERE service_type = %s 
          AND source_year = %s 
          AND source_month = %s;
        """
        self.cursor.execute(query, (service, year, month))
        count = self.cursor.fetchone()[0]
        return count > 0
    
    def delete_partition(self, service: str, year: int, month: int):
        """Elimina una partición específica de OBT."""
        query = f"""
        DELETE FROM {self.schema_analytics}.obt_trips
        WHERE service_type = %s 
          AND source_year = %s 
          AND source_month = %s;
        """
        self.cursor.execute(query, (service, year, month))
        self.logger.info(f"  ✓ Partición {service} {year}-{month:02d} eliminada")
    
    def get_partition_count(self, service: str, year: int, month: int) -> int:
        """Obtiene el conteo de filas de una partición en OBT."""
        query = f"""
        SELECT COUNT(*) 
        FROM {self.schema_analytics}.obt_trips
        WHERE service_type = %s 
          AND source_year = %s 
          AND source_month = %s;
        """
        self.cursor.execute(query, (service, year, month))
        return self.cursor.fetchone()[0]
    
    def get_source_partition_count(self, service: str, year: int, month: int) -> int:
        """Obtiene el conteo de filas de una partición en RAW."""
        query = f"""
        SELECT COUNT(*) 
        FROM {self.schema_raw}.{service}_taxi_trip
        WHERE "source_year" = %s 
          AND "source_month" = %s;
        """
        self.cursor.execute(query, (year, month))
        return self.cursor.fetchone()[0]
    
    def build_full_optimized(self, services: List[str], run_id: str):
        """
        Modo FULL: Reconstruye la OBT completa escribiendo directamente sobre analytics.obt_trips.
        Sin tabla temporal ni swap atómico.
        """
        self.logger.info("\n" + "="*80)
        self.logger.info("MODO: FULL REBUILD")
        self.logger.info("="*80)
        self.logger.info(f"Servicios: {', '.join(services)}")
        self.logger.info(f"RUN_ID: {run_id}")
        self.logger.info("="*80 + "\n")
        
        start_time = time.time()
        
        self.create_analytics_schema()
        self.drop_obt_table()
        self.create_obt_table()
        target_table = f"{self.schema_analytics}.obt_trips"
        
        total_rows = 0
        processed_partitions = 0
        
        for service in services:
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"Procesando servicio: {service.upper()}")
            self.logger.info(f"{'='*80}\n")
            
            service_start = time.time()
            service_rows = 0
            
            for year in range(2015, 2025):
                for month in range(1, 13):
                    source_count = self.get_source_partition_count(service, year, month)
                    if source_count == 0:
                        self.logger.info(f"  [SKIP] {service} {year}-{month:02d}: No hay datos")
                        continue
                    
                    partition_rows = self.bulk_insert_partition(
                        service, year, month, target_table
                    )
                    
                    service_rows += partition_rows
                    total_rows += partition_rows
                    processed_partitions += 1
            
            service_time = time.time() - service_start
            self.logger.info(f"✓ {service.upper()}: {service_rows:,} filas en {service_time:.1f}s")
        
        self.create_metrics_view()
        self.create_indexes()
        
        total_time = time.time() - start_time
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info("RESUMEN FULL REBUILD")
        self.logger.info(f"{'='*80}")
        self.logger.info(f"Particiones procesadas: {processed_partitions}")
        self.logger.info(f"Total filas insertadas: {total_rows:,}")
        self.logger.info(f"Tiempo total: {total_time:.1f}s")
        if total_rows > 0:
            self.logger.info(f"Velocidad: {total_rows / total_time:.0f} filas/seg")
        self.logger.info(f"{'='*80}\n")
        
        return total_rows
    
    def build_by_partition_optimized(self, services: List[str], year_start: int, year_end: int,
                                     months: List[int], run_id: str, overwrite: bool = False):
        """
        Modo BY-PARTITION: Procesa solo particiones específicas sin swap.
        """
        self.logger.info("\n" + "="*80)
        self.logger.info("MODO: BY-PARTITION")
        self.logger.info("="*80)
        self.logger.info(f"Servicios: {', '.join(services)}")
        self.logger.info(f"Años: {year_start}-{year_end}")
        self.logger.info(f"Meses: {months if months else 'Todos'}")
        self.logger.info(f"RUN_ID: {run_id}")
        self.logger.info(f"Overwrite: {overwrite}")
        self.logger.info("="*80 + "\n")
        
        start_time = time.time()
        
        self.create_analytics_schema()
        self.create_obt_table()
        
        if not months:
            months = list(range(1, 13))
        
        total_rows = 0
        processed_partitions = 0
        skipped_partitions = 0
        
        for service in services:
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"Servicio: {service.upper()}")
            self.logger.info(f"{'='*80}\n")
            
            for year in range(year_start, year_end + 1):
                for month in months:
                    partition_key = f"{service} {year}-{month:02d}"
                    
                    source_count = self.get_source_partition_count(service, year, month)
                    if source_count == 0:
                        self.logger.info(f"  [SKIP] {partition_key}: No hay datos en RAW")
                        skipped_partitions += 1
                        continue
                    
                    exists = self.check_partition_exists(service, year, month)
                    
                    if exists and not overwrite:
                        existing_count = self.get_partition_count(service, year, month)
                        self.logger.info(
                            f"  [SKIP] {partition_key}: Ya existe ({existing_count:,} filas)"
                        )
                        skipped_partitions += 1
                        continue
                    
                    if exists and overwrite:
                        self.delete_partition(service, year, month)
                    
                    partition_rows = self.bulk_insert_partition(service, year, month)
                    
                    total_rows += partition_rows
                    processed_partitions += 1
        
        if processed_partitions > 0:
            self.create_metrics_view()
            self.create_indexes()
        
        total_time = time.time() - start_time
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info("RESUMEN BY-PARTITION")
        self.logger.info(f"{'='*80}")
        self.logger.info(f"Particiones procesadas: {processed_partitions}")
        self.logger.info(f"Particiones omitidas: {skipped_partitions}")
        self.logger.info(f"Total filas insertadas: {total_rows:,}")
        self.logger.info(f"Tiempo total: {total_time:.1f}s")
        if total_rows > 0:
            self.logger.info(f"Velocidad: {total_rows / total_time:.0f} filas/seg")
        self.logger.info(f"{'='*80}\n")
        
        return total_rows
    
    def get_obt_summary(self):
        """Muestra un resumen completo de la tabla OBT."""
        self.logger.info("\n" + "="*80)
        self.logger.info("RESUMEN OBT COMPLETO")
        self.logger.info("="*80 + "\n")
        
        try:
            query = f"SELECT COUNT(*) FROM {self.schema_analytics}.obt_trips;"
            self.cursor.execute(query)
            total = self.cursor.fetchone()[0]
            self.logger.info(f"Total filas en OBT: {total:,}\n")
            
            query = f"""
            SELECT service_type, COUNT(*) as count
            FROM {self.schema_analytics}.obt_trips
            GROUP BY service_type
            ORDER BY service_type;
            """
            self.cursor.execute(query)
            self.logger.info("Por servicio:")
            for row in self.cursor.fetchall():
                self.logger.info(f"  {row[0]}: {row[1]:,}")
            
            query = f"""
            SELECT pickup_year, COUNT(*) as count
            FROM {self.schema_analytics}.obt_trips
            GROUP BY pickup_year
            ORDER BY pickup_year;
            """
            self.cursor.execute(query)
            self.logger.info("\nPor año:")
            for row in self.cursor.fetchall():
                self.logger.info(f"  {row[0]}: {row[1]:,}")
            
            query = f"""
            SELECT 
                pg_size_pretty(pg_total_relation_size('{self.schema_analytics}.obt_trips')) as total_size,
                pg_size_pretty(pg_relation_size('{self.schema_analytics}.obt_trips')) as table_size,
                pg_size_pretty(
                    pg_total_relation_size('{self.schema_analytics}.obt_trips') - 
                    pg_relation_size('{self.schema_analytics}.obt_trips')
                ) as index_size;
            """
            self.cursor.execute(query)
            sizes = self.cursor.fetchone()
            self.logger.info("\nTamaños:")
            self.logger.info(f"  Tabla: {sizes[1]}")
            self.logger.info(f"  Índices: {sizes[2]}")
            self.logger.info(f"  Total: {sizes[0]}")
            
            self.logger.info("\nColumnas calculadas disponibles:")
            self.logger.info("  • trip_duration_min - Duración del viaje en minutos")
            self.logger.info("  • trip_speed_mph     - Velocidad promedio en MPH")
            self.logger.info("  • is_weekend         - Indicador de fin de semana")
            self.logger.info("  • hour_of_day        - Hora del día")
            
        except Exception as e:
            self.logger.error(f"Error obteniendo resumen: {e}")
        
        self.logger.info("\n" + "="*80 + "\n")


def parse_arguments():
    """Parsea argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description='NYC TLC One Big Table Builder - OPTIMIZADO',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Full rebuild
  python build_obt_optimized.py --mode full --run-id obt_20250103

  # By-partition optimizado
  python build_obt_optimized.py --mode by-partition --year-start 2023 --year-end 2024 --months 1,2 --run-id obt_partial

  # Solo green taxi, full rebuild
  python build_obt_optimized.py --mode full --services green --run-id obt_green_only
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['full', 'by-partition'],
        required=True,
        help='Modo de ejecución: full (rebuild completo) o by-partition (incremental)'
    )
    
    parser.add_argument(
        '--year-start',
        type=int,
        default=2015,
        help='Año inicial (default: 2015)'
    )
    
    parser.add_argument(
        '--year-end',
        type=int,
        default=2024,
        help='Año final (default: 2024)'
    )
    
    parser.add_argument(
        '--months',
        type=str,
        default=None,
        help='Meses a procesar, separados por coma (ej: 1,2,3 para enero-marzo)'
    )
    
    parser.add_argument(
        '--services',
        type=str,
        default='yellow,green',
        help='Servicios a procesar, separados por coma (default: yellow,green)'
    )
    
    parser.add_argument(
        '--run-id',
        type=str,
        default=f"obt_optimized_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        help='Identificador único de la ejecución'
    )
    
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Sobrescribe particiones existentes (solo para by-partition)'
    )
    
    return parser.parse_args()


def main():
    """Función principal."""
    args = parse_arguments()
    
    services = [s.strip() for s in args.services.split(',')]
    
    months = None
    if args.months:
        months = [int(m.strip()) for m in args.months.split(',')]
    
    builder = OptimizedOBTBuilder()
    
    try:
        builder.connect()
        
        if args.mode == 'full':
            builder.build_full_optimized(
                services=services,
                run_id=args.run_id
            )
        else:
            builder.build_by_partition_optimized(
                services=services,
                year_start=args.year_start,
                year_end=args.year_end,
                months=months,
                run_id=args.run_id,
                overwrite=args.overwrite
            )
        
        builder.get_obt_summary()
        builder.logger.info("✓ OBT Builder OPTIMIZADO completado exitosamente\n")
    
    except KeyboardInterrupt:
        builder.logger.info("\n\n✗ Proceso interrumpido por el usuario")
        sys.exit(1)
    except Exception as e:
        builder.logger.error(f"\n✗ Error fatal: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        builder.close()


if __name__ == '__main__':
    main()
