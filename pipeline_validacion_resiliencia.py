"""
Módulo de Validación, Resiliencia y Base de Datos para Pipeline ETL
==================================================================
Autor: Ingeniero de Datos
Descripción:
    Este módulo implementa el contrato de datos, segregación de registros válidos
    vs. inválidos (cuarentena), control transaccional estricto y tolerancia a fallos
    para la ingestión de datos en PostgreSQL usando psycopg2 y Pydantic.
"""

import os
import json
import logging
import pandas as pd
from datetime import date
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import Json
from pydantic import BaseModel, field_validator, ValidationError

# =============================================================================
# 1. CONFIGURACIÓN DE LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("PipelineETL.ValidacionDB")


# =============================================================================
# 2. CONFIGURACIÓN PARAMETRIZADA DE BASE DE DATOS POSTGRESQL
# =============================================================================
@dataclass
class DBConfig:
    """Configuración de conexión a PostgreSQL parametrizada mediante variables de entorno."""
    host: str = os.getenv("PG_HOST", "localhost")
    port: int = int(os.getenv("PG_PORT", 5432))
    database: str = os.getenv("PG_DATABASE", "postgres")
    user: str = os.getenv("PG_USER", "postgres")
    password: str = os.getenv("PG_PASSWORD", "ChairmanElit1")

    def get_connection_dict(self) -> Dict[str, Any]:
        """Retorna los parámetros en formato diccionario para psycopg2."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
            "connect_timeout": 5
        }


def obtener_conexion(config: DBConfig):
    """Establece y retorna una conexión a PostgreSQL utilizando psycopg2."""
    try:
        conexion = psycopg2.connect(**config.get_connection_dict())
        logger.info(f"Conexión exitosa a PostgreSQL en {config.host}:{config.port}/{config.database}")
        return conexion
    except psycopg2.Error as err:
        logger.error(f"Error al conectar con PostgreSQL: {err}")
        raise err


# =============================================================================
# 3. MODELADO DE DATOS Y VALIDACIONES CON PYDANTIC
# =============================================================================
class Transaccion(BaseModel):
    """
    Contrato de datos para los registros del pipeline ETL.
    Valida tipos y reglas de negocio obligatorias.
    """
    id_orden: str
    fecha: date
    id_producto: str
    cantidad: int
    precio_unitario_mxn: float
    monto_total_mxn: float
    email: str
    segmento_importe: str
    monto_escalado: float

    @field_validator("cantidad")
    @classmethod
    def validar_cantidad_positiva(cls, valor: int) -> int:
        """Asegura que la cantidad sea estrictamente mayor a 0."""
        if valor <= 0:
            raise ValueError(f"La cantidad ({valor}) debe ser mayor a 0.")
        return valor


# =============================================================================
# 4. CONFIGURACIÓN Y DDL DE TABLAS EN POSTGRESQL
# =============================================================================
def configurar_tablas(conexion) -> None:
    """
    Crea las tablas 'transacciones_limpias' y 'cuarentena_errores' en PostgreSQL
    si aún no existen en el esquema.
    """
    ddl_transacciones_limpias = """
    CREATE TABLE IF NOT EXISTS transacciones_limpias (
        id_orden VARCHAR(100) NOT NULL,
        fecha DATE NOT NULL,
        id_producto VARCHAR(100) NOT NULL,
        cantidad INTEGER NOT NULL,
        precio_unitario_mxn NUMERIC(14, 4) NOT NULL,
        monto_total_mxn NUMERIC(16, 4) NOT NULL,
        email VARCHAR(255) NOT NULL,
        segmento_importe VARCHAR(50) NOT NULL,
        monto_escalado NUMERIC(16, 6) NOT NULL,
        fecha_procesamiento TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """

    ddl_cuarentena_errores = """
    CREATE TABLE IF NOT EXISTS cuarentena_errores (
        id_registro SERIAL PRIMARY KEY,
        datos_crudos JSONB NOT NULL,
        motivo_rechazo_cuarentena TEXT NOT NULL,
        fecha_rechazo TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """

    try:
        with conexion.cursor() as cursor:
            cursor.execute(ddl_transacciones_limpias)
            cursor.execute(ddl_cuarentena_errores)
            conexion.commit()
            logger.info("Tablas 'transacciones_limpias' y 'cuarentena_errores' verificadas/creadas correctamente.")
    except psycopg2.Error as err:
        conexion.rollback()
        logger.error(f"Error al configurar tablas DDL en PostgreSQL: {err}")
        raise err


# =============================================================================
# 5. MECANISMO DE SEGREGACIÓN, RESILIENCIA Y CARGA TRANSACCIONAL
# =============================================================================
def cargar_lote_resiliente(conexion, lote_datos: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Procesa e ingesta un lote de datos de manera resiliente.
    
    Reglas de negocio y aislamiento transaccional:
    - Registro válido -> Inserta en 'transacciones_limpias' y ejecuta commit().
    - ValidationError -> Ejecuta rollback(), extrae el error y envía a 'cuarentena_errores' con commit().
    - El pipeline NUNCA colapsa ante registros corruptos o excepciones individuales.
    
    Retorna un diccionario con métricas del lote procesado.
    """
    metricas = {
        "total_procesados": len(lote_datos),
        "insertados_limpias": 0,
        "enviados_cuarentena": 0,
        "errores_criticos": 0
    }

    insert_limpias_query = """
        INSERT INTO transacciones_limpias (
            id_orden,
            fecha,
            id_producto,
            cantidad,
            precio_unitario_mxn,
            monto_total_mxn,
            email,
            segmento_importe,
            monto_escalado
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    insert_cuarentena_query = """
        INSERT INTO cuarentena_errores (
            datos_crudos,
            motivo_rechazo_cuarentena
        ) VALUES (%s, %s);
    """

    with conexion.cursor() as cursor:
        for indice, registro in enumerate(lote_datos, start=1):
            id_referencia = registro.get("id_orden", f"FILA_{indice}")

            try:
                # -------------------------------------------------------------
                # PASO 1: Validación contra el contrato de datos (Pydantic)
                # -------------------------------------------------------------
                transaccion = Transaccion(**registro)

                # -------------------------------------------------------------
                # PASO 2: Inserción en transacciones_limpias
                # -------------------------------------------------------------
                cursor.execute(insert_limpias_query, (
                    transaccion.id_orden,
                    transaccion.fecha,
                    transaccion.id_producto,
                    transaccion.cantidad,
                    transaccion.precio_unitario_mxn,
                    transaccion.monto_total_mxn,
                    transaccion.email,
                    transaccion.segmento_importe,
                    transaccion.monto_escalado
                ))

                # Transacción exitosa -> Confirmar cambios
                conexion.commit()
                metricas["insertados_limpias"] += 1
                logger.info(f"[{indice}/{len(lote_datos)}] [LIMPIAS] Registro {id_referencia} guardado exitosamente.")

            except ValidationError as val_err:
                # -------------------------------------------------------------
                # PASO 3: Aislamiento de error de validación (Pydantic)
                # -------------------------------------------------------------
                conexion.rollback()

                # Extraer detalle legible del fallo de validación
                errores_formateados = []
                for error in val_err.errors():
                    campo = " -> ".join(str(loc) for loc in error.get("loc", []))
                    mensaje = error.get("msg", "")
                    errores_formateados.append(f"[{campo}]: {mensaje}")
                motivo_rechazo = "; ".join(errores_formateados)

                logger.warning(
                    f"[{indice}/{len(lote_datos)}] [CUARENTENA] Registro {id_referencia} inválido. "
                    f"Motivo: {motivo_rechazo}"
                )

                try:
                    # Inserción en tabla de cuarentena de errores
                    cursor.execute(insert_cuarentena_query, (
                        Json(registro),
                        motivo_rechazo
                    ))
                    conexion.commit()
                    metricas["enviados_cuarentena"] += 1

                except Exception as err_cuarentena:
                    conexion.rollback()
                    logger.error(
                        f"[{indice}/{len(lote_datos)}] [FALLO CRÍTICO] No se pudo enviar a cuarentena {id_referencia}: {err_cuarentena}"
                    )
                    metricas["errores_criticos"] += 1

            except Exception as exc_inesperada:
                # -------------------------------------------------------------
                # PASO 4: Resiliencia ante cualquier error no anticipado
                # (Ejemplo: errores de base de datos o tipos SQL no mapeados)
                # -------------------------------------------------------------
                conexion.rollback()
                motivo_error_inesperado = f"Error inesperado de ejecución: {str(exc_inesperada)}"
                logger.error(f"[{indice}/{len(lote_datos)}] [CUARENTENA INESPERADA] Registro {id_referencia}: {motivo_error_inesperado}")

                try:
                    cursor.execute(insert_cuarentena_query, (
                        Json(registro),
                        motivo_error_inesperado
                    ))
                    conexion.commit()
                    metricas["enviados_cuarentena"] += 1
                except Exception as err_segundo_nivel:
                    conexion.rollback()
                    logger.critical(
                        f"[{indice}/{len(lote_datos)}] [FATAL] Imposible registrar en cuarentena: {err_segundo_nivel}"
                    )
                    metricas["errores_criticos"] += 1

    logger.info("=" * 60)
    logger.info("RESUMEN DE EJECUCIÓN DEL LOTE:")
    logger.info(f"  • Total registros procesados : {metricas['total_procesados']}")
    logger.info(f"  • Insertados en limpias      : {metricas['insertados_limpias']}")
    logger.info(f"  • Enviados a cuarentena      : {metricas['enviados_cuarentena']}")
    logger.info(f"  • Errores críticos           : {metricas['errores_criticos']}")
    logger.info("=" * 60)

    return metricas


# =============================================================================
# 6. DEMOSTRACIÓN Y EJECUCIÓN (CON DATOS SINTÉTICOS DE PRUEBA)
# =============================================================================


if __name__ == "__main__":
    print("\n--- INICIANDO PIPELINE DE VALIDACIÓN Y CARGA A POSTGRESQL ---\n")
    
    # 1. Leer el CSV final que soltó el Ingeniero 2
    df_final = pd.read_csv("ventas_integradas.csv")
    
    # 2. Convertirlo al formato de lista de diccionarios que Pydantic espera
    lote_real = df_final.to_dict(orient='records')
    
    # 3. Conectar y cargar
    config_db = DBConfig()
    try:
        conexion_pg = obtener_conexion(config_db)
        try:
            configurar_tablas(conexion_pg)
            # Pasamos los 730 registros reales a tu función
            cargar_lote_resiliente(conexion_pg, lote_real)
        finally:
            conexion_pg.close()
    except Exception as e:
        print("Error de conexión a PostgreSQL:", e)