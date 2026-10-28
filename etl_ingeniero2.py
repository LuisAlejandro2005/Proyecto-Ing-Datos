"""Pipeline ETL de integración multifuente para el proyecto ATD-2602.

Uso:
    python etl_ingeniero2.py
    python etl_ingeniero2.py --ventas-csv ventas_limpias.csv \
        --catalogo-csv catalogo_productos_limpio.csv --output-dir resultados
    python etl_ingeniero2.py --online-json ventas_online.json

Requiere Python 3.10+, pandas 2.0+ y openpyxl. La tasa USD/MXN es un supuesto
fijo del ejercicio (19.50), no una cotización financiera actual.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


TIPO_CAMBIO_USD_MXN = 19.50
TOLERANCIA_TOTAL_MXN = 0.01
TOLERANCIA_PRECIO_CATALOGO = 0.01
COLUMNAS_VENTA_MINIMAS = (
    "id_orden",
    "fecha",
    "id_producto",
    "cantidad",
    "precio_unitario_mxn",
)
COLUMNAS_VENTA_FINAL = [
    "id_orden",
    "fecha",
    "id_producto",
    "cantidad",
    "precio_unitario_mxn",
    "monto_total_mxn",
    "email",
    "segmento_importe",
    "monto_escalado",
]
COLUMNAS_CUARENTENA = [
    "id_orden",
    "fecha",
    "id_producto",
    "cantidad",
    "precio_unitario_mxn",
    "monto_total_mxn",
    "email",
    "motivo_cuarentena",
    "fase",
]
COLUMNAS_DISCREPANCIAS = [
    "id_orden",
    "fuente",
    "monto_original_mxn",
    "monto_recalculado_mxn",
    "diferencia_absoluta_mxn",
]
ALIASES = {
    "id_orden": ("id_orden", "orden_id", "order_id", "id_venta", "venta_id"),
    "fecha": ("fecha", "date", "order_date", "fecha_venta"),
    "id_producto": ("id_producto", "product_id", "producto_id", "sku"),
    "cantidad": ("cantidad", "quantity", "qty"),
    "precio_unitario_mxn": (
        "precio_unitario_mxn",
        "unit_price_mxn",
        "precio_mxn",
    ),
    "_precio_unitario_sin_moneda": (
        "precio_unitario",
        "unit_price",
        "price",
    ),
    "precio_unitario_usd": ("precio_unitario_usd", "unit_price_usd", "price_usd"),
    "monto_total_mxn": (
        "monto_total_mxn",
        "total_mxn",
    ),
    "_monto_total_sin_moneda": ("monto_total", "total", "amount"),
    "monto_total_usd": ("monto_total_usd", "total_usd", "amount_usd"),
    "email": ("email", "correo", "customer_email", "cliente_email"),
    "moneda": ("moneda", "currency", "divisa"),
}


def configurar_logging(ruta: Path) -> logging.Logger:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("etl_ingeniero2")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formato = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    archivo = logging.FileHandler(ruta, encoding="utf-8")
    consola = logging.StreamHandler()
    archivo.setFormatter(formato)
    consola.setFormatter(formato)
    logger.addHandler(archivo)
    logger.addHandler(consola)
    return logger


def _limpiar_nombre(valor: Any) -> str:
    return str(valor).strip().lower()


def _mapear_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """Adapta alias comunes sin descartar columnas originales desconocidas."""
    nombres = {_limpiar_nombre(col): col for col in df.columns}
    renombres: dict[Any, str] = {}
    for destino, aliases in ALIASES.items():
        for alias in aliases:
            if alias in nombres:
                renombres[nombres[alias]] = destino
                break
    resultado = df.rename(columns=renombres).copy()
    if resultado.columns.duplicated().any():
        duplicadas = resultado.columns[resultado.columns.duplicated()].tolist()
        raise ValueError(f"Columnas duplicadas tras normalizar alias: {duplicadas}")
    return resultado


def _columna_texto(df: pd.DataFrame, nombre: str, default: str = "") -> pd.Series:
    if nombre not in df.columns:
        return pd.Series(default, index=df.index, dtype="object")
    return df[nombre].astype("object")


def cargar_ventas_csv(
    ruta: Path, fuente: str, logger: logging.Logger, metricas: dict[str, Any]
) -> pd.DataFrame:
    metricas["fuentes"][fuente] = {
        "ruta": str(ruta),
        "existente": ruta.is_file(),
        "filas_leidas": 0,
        "duplicados_exactos": 0,
        "columnas_detectadas": [],
        "estado": "ausente",
    }
    if not ruta.is_file():
        logger.warning("Fuente %s ausente: %s", fuente, ruta)
        return pd.DataFrame()
    try:
        df = pd.read_csv(ruta, encoding="utf-8-sig", dtype="object")
        df = _mapear_columnas(df)
    except (OSError, UnicodeError, pd.errors.ParserError, ValueError) as error:
        metricas["fuentes"][fuente]["estado"] = "error de lectura/estructura"
        metricas["fuentes"][fuente]["error"] = str(error)
        logger.error("No se pudo leer %s (%s): %s", ruta, fuente, error)
        return pd.DataFrame()

    info = metricas["fuentes"][fuente]
    info["filas_leidas"] = len(df)
    info["columnas_detectadas"] = [str(col) for col in df.columns]
    faltantes = [col for col in COLUMNAS_VENTA_MINIMAS if col not in df.columns]
    if faltantes:
        info["estado"] = "esquema incompatible"
        info["error"] = f"Faltan columnas mínimas: {faltantes}"
        logger.error("Esquema incompatible en %s; faltan %s", ruta, faltantes)
        return pd.DataFrame()

    antes = len(df)
    df = df.drop_duplicates(keep="first").copy()
    info["duplicados_exactos"] = antes - len(df)
    info["estado"] = "cargado"
    df["_fuente"] = fuente
    logger.info(
        "Fuente %s: %d filas leídas; %d duplicados exactos removidos",
        fuente,
        antes,
        antes - len(df),
    )
    return df


def cargar_ventas_online(
    ruta: Path | None, logger: logging.Logger, metricas: dict[str, Any]
) -> pd.DataFrame:
    nombre = "online"
    metricas["fuentes"][nombre] = {
        "ruta": str(ruta) if ruta else None,
        "existente": bool(ruta and ruta.is_file()),
        "filas_leidas": 0,
        "duplicados_exactos": 0,
        "columnas_detectadas": [],
        "estado": "no solicitada",
    }
    if ruta is None:
        logger.info("No se proporcionó --online-json; no se realizó integración digital.")
        return pd.DataFrame()
    if not ruta.is_file():
        metricas["fuentes"][nombre]["estado"] = "ausente"
        logger.warning("JSON online inexistente; se continúa con fuentes válidas: %s", ruta)
        return pd.DataFrame()
    try:
        with ruta.open("r", encoding="utf-8-sig") as archivo:
            payload = json.load(archivo)
        if isinstance(payload, list):
            registros = payload
        elif isinstance(payload, dict):
            registros = None
            for clave in ("data", "ventas", "orders", "records", "results"):
                if isinstance(payload.get(clave), list):
                    registros = payload[clave]
                    break
            if registros is None and payload and all(
                isinstance(valor, list) for valor in payload.values()
            ):
                df = pd.DataFrame(payload)
                registros = None
            elif registros is None:
                raise ValueError(
                    "Se esperaba una lista de objetos, una lista en data/ventas/"
                    "orders/records/results o un objeto columnar."
                )
            else:
                df = pd.DataFrame(registros)
        else:
            raise ValueError("La raíz JSON debe ser una lista o un objeto.")
        if registros is not None:
            if any(not isinstance(registro, dict) for registro in registros):
                raise ValueError("Todos los elementos JSON deben ser objetos.")
            df = pd.DataFrame(registros)
        df = _mapear_columnas(df)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError, TypeError) as error:
        metricas["fuentes"][nombre]["estado"] = "JSON incompatible"
        metricas["fuentes"][nombre]["error"] = str(error)
        logger.error("No se incorporó la fuente online %s: %s", ruta, error)
        return pd.DataFrame()

    info = metricas["fuentes"][nombre]
    info["filas_leidas"] = len(df)
    info["columnas_detectadas"] = [str(col) for col in df.columns]
    if "id_orden" not in df.columns:
        logger.warning(
            "El JSON online no contiene identificador de orden; los registros "
            "serán evaluados y enviados a cuarentena si no se puede identificar la venta."
        )
    if not any(
        columna in df.columns
        for columna in (
            "precio_unitario_mxn",
            "precio_unitario_usd",
            "_precio_unitario_sin_moneda",
        )
    ):
        info["estado"] = "esquema incompatible"
        info["error"] = "No se identificó precio en MXN ni precio_unitario_usd."
        logger.error("El JSON online carece de un campo de precio identificable.")
        return pd.DataFrame()

    antes = len(df)
    df = df.drop_duplicates(keep="first").copy()
    info["duplicados_exactos"] = antes - len(df)
    info["estado"] = "cargado"
    df["_fuente"] = nombre
    logger.info(
        "Fuente online: %d filas leídas; %d duplicados exactos removidos",
        antes,
        antes - len(df),
    )
    return df


def cargar_catalogo(ruta: Path, logger: logging.Logger) -> pd.DataFrame:
    if not ruta.is_file():
        raise FileNotFoundError(f"El catálogo obligatorio no existe: {ruta}")
    catalogo = pd.read_csv(ruta, encoding="utf-8-sig", dtype="object")
    catalogo = _mapear_columnas(catalogo)
    requeridas = ("id_producto", "precio_base_mxn")
    faltantes = [col for col in requeridas if col not in catalogo.columns]
    if faltantes:
        raise ValueError(f"El catálogo carece de columnas obligatorias: {faltantes}")
    catalogo["id_producto"] = catalogo["id_producto"].astype("string").str.strip()
    if (
        catalogo.empty
        or catalogo["id_producto"].isna().any()
        or catalogo["id_producto"].str.strip().eq("").any()
    ):
        raise ValueError("El catálogo está vacío o contiene identificadores nulos.")
    duplicados = catalogo.loc[
        catalogo["id_producto"].duplicated(keep=False), "id_producto"
    ].tolist()
    if duplicados:
        raise ValueError(
            "El catálogo contiene identificadores duplicados que impiden un cruce "
            f"inequívoco: {sorted(set(duplicados))}"
        )
    logger.info("Catálogo validado: %d productos únicos en %s", len(catalogo), ruta)
    return catalogo


def inspeccionar_cuarentena_heredada(
    ruta: Path, logger: logging.Logger, metricas: dict[str, Any]
) -> None:
    """Registra datos de auditoría previos sin reincorporarlos al flujo de ventas."""
    auditoria: dict[str, Any] = {
        "ruta": str(ruta),
        "existente": ruta.is_file(),
        "filas": 0,
        "columnas": [],
        "motivos": {},
        "estado": "ausente",
    }
    metricas["cuarentena_heredada"] = auditoria
    if not ruta.is_file():
        logger.info("Cuarentena heredada ausente; no se procesa.")
        return
    try:
        previa = pd.read_csv(ruta, encoding="utf-8-sig", dtype="object")
    except (OSError, UnicodeError, pd.errors.ParserError) as error:
        auditoria["estado"] = "error de lectura"
        auditoria["error"] = str(error)
        logger.warning("No se pudo inspeccionar cuarentena heredada %s: %s", ruta, error)
        return
    auditoria["filas"] = len(previa)
    auditoria["columnas"] = [str(columna) for columna in previa.columns]
    auditoria["estado"] = "solo auditoría; excluida del flujo de ventas"
    if "motivo_cuarentena" in previa.columns:
        auditoria["motivos"] = {
            str(motivo): int(cantidad)
            for motivo, cantidad in previa["motivo_cuarentena"]
            .fillna("SIN MOTIVO")
            .value_counts()
            .items()
        }
    logger.info(
        "Cuarentena heredada inspeccionada solo para auditoría: %d filas; "
        "motivos=%s; no se incorporó al flujo de ventas.",
        len(previa),
        auditoria["motivos"],
    )


def eliminar_duplicados(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Audita coincidencias interfuente, sin asumir que sean ventas duplicadas."""
    if df.empty:
        return df.copy()
    # Las filas de cada fuente ya se deduplicaron. Una coincidencia exacta
    # entre fuentes se conserva: podría representar solapamiento real, y su
    # procedencia permite decidirlo posteriormente con evidencia de negocio.
    columnas = [col for col in df.columns if col != "_fuente"]
    duplicados_entre_fuentes = int(
        df.assign(_huella=df[columnas].astype("string").fillna("<NA>").agg("|".join, axis=1))
        .groupby("_huella")["_fuente"]
        .nunique()
        .gt(1)
        .sum()
    )
    logger.info(
        "Coincidencias exactas interfuente identificadas: %d; se conservan por "
        "procedencia. IDs compartidos no se consideran suficientes para eliminar registros.",
        duplicados_entre_fuentes,
    )
    return df.copy()


def _fecha_iso(valor: Any) -> tuple[str | None, str | None]:
    if valor is None or pd.isna(valor) or not str(valor).strip():
        return None, "fecha inválida"
    texto = str(valor).strip()
    formatos = ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S")
    for formato in formatos:
        try:
            return datetime.strptime(texto, formato).date().isoformat(), None
        except ValueError:
            continue
    for formato in ("%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y"):
        try:
            parsed = datetime.strptime(texto, formato).date()
            # DD/MM and MM/DD are both valid but mean different dates when
            # both components are <= 12; do not guess between them.
            if re.fullmatch(r"\d{1,2}[/-]\d{1,2}[/-]\d{4}", texto):
                partes = re.split(r"[/-]", texto)
                if int(partes[0]) <= 12 and int(partes[1]) <= 12 and partes[0] != partes[1]:
                    return None, "fecha ambigua"
            return parsed.isoformat(), None
        except ValueError:
            continue
    return None, "fecha inválida"


def normalizar_fechas(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    resultado = df.copy()
    fechas: list[str | None] = []
    motivos: list[str | None] = []
    reformateadas = invalidas = ambiguas = 0
    originales = resultado["fecha"].tolist()
    for valor in originales:
        iso, motivo = _fecha_iso(valor)
        fechas.append(iso)
        motivos.append(motivo)
        if motivo == "fecha inválida":
            invalidas += 1
        elif motivo == "fecha ambigua":
            ambiguas += 1
        elif str(valor).strip() != iso:
            reformateadas += 1
    resultado["_fecha_original"] = originales
    resultado["fecha"] = fechas
    resultado["_motivo_fecha"] = motivos
    logger.info(
        "Fechas examinadas: %d; reformateadas: %d; inválidas: %d; ambiguas: %d",
        len(resultado),
        reformateadas,
        invalidas,
        ambiguas,
    )
    return resultado


def _numero(valor: Any) -> float | None:
    if valor is None or pd.isna(valor) or not str(valor).strip():
        return None
    texto = str(valor).strip().replace(",", "")
    try:
        numero = float(texto)
    except (TypeError, ValueError):
        return None
    return numero if math.isfinite(numero) else None


def normalizar_monedas(
    df: pd.DataFrame, logger: logging.Logger
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    resultado = df.copy()
    precio_final: list[float | None] = []
    total_original_mxn: list[float | None] = []
    motivos: list[str | None] = []
    conversiones = 0

    for _, fila in resultado.iterrows():
        fuente = fila.get("_fuente", "")
        precio_mxn = _numero(fila.get("precio_unitario_mxn"))
        precio_usd = _numero(fila.get("precio_unitario_usd"))
        precio_generico = _numero(fila.get("_precio_unitario_sin_moneda"))
        valor_moneda = fila.get("moneda", "")
        moneda = str(valor_moneda).strip().upper() if _no_vacio(valor_moneda) else ""
        motivo = None

        if precio_generico is not None:
            if moneda in ("USD", "US$"):
                precio_usd = precio_generico
            elif moneda in ("MXN", "MX$", "PESO", "PESOS"):
                precio_mxn = precio_generico
            elif precio_mxn is None and precio_usd is None:
                motivo = "moneda no identificada"

        if precio_usd is not None:
            if moneda and moneda not in ("USD", "US$", "MXN", "MX$", "PESO", "PESOS"):
                motivo = "moneda no identificada"
                precio = None
            elif precio_mxn is not None:
                if moneda in ("USD", "US$"):
                    calculado = round(precio_usd * TIPO_CAMBIO_USD_MXN, 2)
                    precio = calculado
                    if abs(precio_mxn - calculado) > TOLERANCIA_TOTAL_MXN:
                        logger.warning(
                            "Discrepancia entre precio USD convertido y MXN declarado "
                            "en orden %s (%s). Se conserva el MXN explícito.",
                            fila.get("id_orden", ""),
                            fuente,
                        )
                        precio = round(precio_mxn, 2)
                elif moneda in ("MXN", "MX$", "PESO", "PESOS"):
                    precio = round(precio_mxn, 2)
                else:
                    # Si existen ambos campos sin moneda, la denominación explícita
                    # del campo MXN es evidencia suficiente para usarlo sin conversión.
                    precio = round(precio_mxn, 2)
            elif moneda in ("USD", "US$") or (
                precio_usd is not None and not moneda
            ):
                precio = round(precio_usd * TIPO_CAMBIO_USD_MXN, 2)
                conversiones += 1
            else:
                precio = None
                motivo = "moneda no identificada"
        elif precio_mxn is not None:
            precio = round(precio_mxn, 2)
        else:
            precio = None
            if precio_generico is not None:
                motivo = "moneda no identificada"

        total_mxn = _numero(fila.get("monto_total_mxn"))
        total_usd = _numero(fila.get("monto_total_usd"))
        total_generico = _numero(fila.get("_monto_total_sin_moneda"))
        if total_usd is not None:
            if moneda in ("USD", "US$"):
                total_mxn = round(total_usd * TIPO_CAMBIO_USD_MXN, 2)
            elif not moneda and total_mxn is None:
                motivo = "moneda no identificada"
        elif total_generico is not None:
            if moneda in ("USD", "US$"):
                total_mxn = round(total_generico * TIPO_CAMBIO_USD_MXN, 2)
            elif moneda in ("MXN", "MX$", "PESO", "PESOS"):
                total_mxn = round(total_generico, 2)

        precio_final.append(precio)
        total_original_mxn.append(total_mxn)
        motivos.append(motivo)

    precio_original = _columna_texto(resultado, "precio_unitario_mxn")
    for columna in ("_precio_unitario_sin_moneda", "precio_unitario_usd"):
        alternativa = _columna_texto(resultado, columna)
        precio_original = precio_original.where(
            precio_original.map(_no_vacio), alternativa
        )
    total_original = _columna_texto(resultado, "monto_total_mxn")
    for columna in ("_monto_total_sin_moneda", "monto_total_usd"):
        alternativa = _columna_texto(resultado, columna)
        total_original = total_original.where(total_original.map(_no_vacio), alternativa)
    resultado["_precio_original"] = precio_original
    resultado["_total_original"] = total_original
    resultado["_total_original_mxn"] = total_original_mxn
    resultado["precio_unitario_mxn"] = precio_final
    resultado["_motivo_moneda"] = motivos
    logger.info(
        "Conversiones USD a MXN efectuadas: %d (tipo fijo del ejercicio: %.2f)",
        conversiones,
        TIPO_CAMBIO_USD_MXN,
    )
    return resultado, []


def validar_reglas_negocio(df: pd.DataFrame, logger: logging.Logger) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    filas_validas: list[dict[str, Any]] = []
    rechazadas: list[dict[str, Any]] = []
    discrepancias: list[dict[str, Any]] = []

    for _, fila in df.iterrows():
        motivos: list[str] = []
        id_orden = fila.get("id_orden")
        id_producto = fila.get("id_producto")
        if id_orden is None or pd.isna(id_orden) or not str(id_orden).strip():
            motivos.append("datos obligatorios faltantes")
        if _no_vacio(fila.get("_motivo_fecha")):
            motivos.append(str(fila["_motivo_fecha"]))
        if _no_vacio(fila.get("_motivo_moneda")):
            motivos.append(str(fila["_motivo_moneda"]))
        if id_producto is None or pd.isna(id_producto) or not str(id_producto).strip():
            motivos.append("datos obligatorios faltantes")

        cantidad = _numero(fila.get("cantidad"))
        if cantidad is None:
            motivos.append("cantidad inválida")
        elif not cantidad.is_integer():
            motivos.append("cantidad inválida")
        elif cantidad <= 0:
            motivos.append("cantidad <= 0")

        precio = _numero(fila.get("precio_unitario_mxn"))
        if precio is None:
            motivos.append("precio inválido")
        elif precio < 0:
            motivos.append("monto negativo")

        if not motivos:
            total = round(int(cantidad) * precio, 2)
            original = fila.get("_total_original_mxn")
            if original is not None and abs(float(original) - total) > TOLERANCIA_TOTAL_MXN:
                discrepancias.append(
                    {
                        "id_orden": id_orden,
                        "fuente": fila.get("_fuente", ""),
                        "monto_original_mxn": round(float(original), 2),
                        "monto_recalculado_mxn": total,
                        "diferencia_absoluta_mxn": round(abs(float(original) - total), 2),
                    }
                )
            registro = fila.to_dict()
            registro["cantidad"] = int(cantidad)
            registro["precio_unitario_mxn"] = precio
            registro["monto_total_mxn"] = total
            registro["_motivos"] = []
            filas_validas.append(registro)
        else:
            registro = fila.to_dict()
            registro["_motivos"] = list(dict.fromkeys(motivos))
            rechazadas.append(registro)

    logger.info(
        "Validación inicial: %d candidatas; %d rechazadas; %d discrepancias de total.",
        len(filas_validas) + len(rechazadas),
        len(rechazadas),
        len(discrepancias),
    )
    columnas_ventas = list(
        dict.fromkeys([*df.columns, "monto_total_mxn", "_motivos"])
    )
    return (
        pd.DataFrame(filas_validas, columns=columnas_ventas),
        pd.DataFrame(rechazadas, columns=columnas_ventas),
        pd.DataFrame(discrepancias, columns=COLUMNAS_DISCREPANCIAS),
    )


def integrar_catalogo(
    ventas: pd.DataFrame, catalogo: pd.DataFrame, logger: logging.Logger
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    catalogo_reducido = catalogo[["id_producto", "precio_base_mxn"]].copy()
    catalogo_reducido["precio_base_mxn"] = catalogo_reducido["precio_base_mxn"].map(_numero)
    antes = len(ventas)
    fusion = ventas.merge(
        catalogo_reducido,
        on="id_producto",
        how="left",
        indicator=True,
        validate="many_to_one",
    )
    huerfanas = fusion.loc[fusion["_merge"] == "left_only"].copy()
    candidatas = fusion.loc[fusion["_merge"] == "both"].copy()
    if not huerfanas.empty:
        logger.warning("%d ventas con producto fuera de catálogo enviadas a cuarentena.", len(huerfanas))

    advertencias = 0
    for _, fila in candidatas.iterrows():
        precio_base = fila["precio_base_mxn"]
        precio_venta = _numero(fila["precio_unitario_mxn"])
        if precio_base is None or precio_base < 0:
            advertencias += 1
            logger.warning(
                "Precio base inválido para producto %s; se omite comparación.",
                fila["id_producto"],
            )
        elif precio_base == 0:
            if precio_venta != 0:
                advertencias += 1
                logger.warning(
                    "Precio base cero para producto %s; diferencia relativa no calculable.",
                    fila["id_producto"],
                )
        else:
            diferencia = abs(precio_venta - precio_base) / precio_base
            if diferencia > TOLERANCIA_PRECIO_CATALOGO:
                advertencias += 1
                logger.warning(
                    "Precio de venta fuera de tolerancia (>1%%): orden=%s producto=%s "
                    "base=%.2f venta=%.2f diferencia=%.2f%%. Se conserva la venta.",
                    fila["id_orden"],
                    fila["id_producto"],
                    precio_base,
                    precio_venta,
                    diferencia * 100,
                )

    logger.info(
        "Cruce catálogo: %d ventas consideradas; %d productos huérfanos; "
        "%d advertencias de precio.",
        antes,
        len(huerfanas),
        advertencias,
    )
    return candidatas, huerfanas, advertencias


def calcular_variables_derivadas(
    ventas: pd.DataFrame, logger: logging.Logger
) -> tuple[pd.DataFrame, dict[str, Any]]:
    resultado = ventas.copy()
    detalle: dict[str, Any] = {"minimo_mxn": None, "maximo_mxn": None, "segmentacion": ""}
    if resultado.empty:
        detalle["segmentacion"] = "Sin ventas válidas; no se calculó Min-Max ni qcut."
        logger.warning(detalle["segmentacion"])
        resultado["monto_escalado"] = pd.Series(dtype="float64")
        resultado["segmento_importe"] = pd.Series(dtype="object")
        return resultado, detalle

    importes = resultado["monto_total_mxn"].astype(float)
    minimo = float(importes.min())
    maximo = float(importes.max())
    detalle["minimo_mxn"] = round(minimo, 2)
    detalle["maximo_mxn"] = round(maximo, 2)
    if maximo == minimo:
        resultado["monto_escalado"] = 0.0
        logger.warning(
            "Todos los montos válidos son iguales; se asigna Min-Max 0.0 a todos."
        )
    else:
        resultado["monto_escalado"] = ((importes - minimo) / (maximo - minimo)).round(6)
    if not resultado["monto_escalado"].map(math.isfinite).all() or not resultado[
        "monto_escalado"
    ].between(0, 1).all():
        raise ValueError("El resultado Min-Max no es finito o está fuera de [0, 1].")

    distintos = importes.nunique()
    if distintos >= 3 and len(resultado) >= 3:
        codigos, bordes = pd.qcut(
            importes, q=[0, 1 / 3, 2 / 3, 1], labels=False, retbins=True, duplicates="drop"
        )
        intervalos = len(bordes) - 1
        etiquetas = ["Bajo", "Medio", "Alto"][:intervalos]
        resultado["segmento_importe"] = codigos.map(
            {indice: etiqueta for indice, etiqueta in enumerate(etiquetas)}
        )
        detalle["segmentacion"] = (
            f"Cuantiles qcut; intervalos obtenidos={intervalos}; "
            f"límites={ [round(float(x), 2) for x in bordes] }."
        )
        if intervalos < 3:
            logger.warning(
                "Los cuantiles repetidos produjeron %d segmentos; se usan etiquetas "
                "ordenadas disponibles, sin fabricar un tercer intervalo.",
                intervalos,
            )
    else:
        # Con menos de tres importes distintos no se pueden justificar tres cortes.
        # qcut de dos valores da dos intervalos naturales; un único importe se
        # etiqueta como Bajo sin afirmar que existan cuantiles separadores.
        if distintos == 2 and len(resultado) >= 2:
            codigos = pd.qcut(importes, q=2, labels=False, duplicates="drop")
            resultado["segmento_importe"] = codigos.map({0: "Bajo", 1: "Alto"})
            detalle["segmentacion"] = (
                "Limitación: solo dos importes distintos; se usan dos cuantiles "
                "naturales (Bajo/Alto), no se fabrica Medio."
            )
        else:
            resultado["segmento_importe"] = "Bajo"
            detalle["segmentacion"] = (
                "Limitación: un solo importe distinto; todos quedan en Bajo y no "
                "se afirman límites cuantílicos entre segmentos."
            )
        logger.warning(detalle["segmentacion"])
    logger.info(
        "Min-Max sobre ventas válidas: mínimo=%.2f, máximo=%.2f; rango escalado=%.6f..%.6f",
        minimo,
        maximo,
        resultado["monto_escalado"].min(),
        resultado["monto_escalado"].max(),
    )
    return resultado, detalle


def generar_cuarentena(rechazadas: pd.DataFrame, huerfanas: pd.DataFrame) -> pd.DataFrame:
    registros: list[dict[str, Any]] = []
    for _, fila in rechazadas.iterrows():
        motivos = fila.get("_motivos", [])
        registros.append(_fila_cuarentena(fila, motivos))
    for _, fila in huerfanas.iterrows():
        motivos = list(fila.get("_motivos", []))
        motivos.append("producto fuera de catálogo")
        registros.append(_fila_cuarentena(fila, motivos))
    return pd.DataFrame(registros, columns=COLUMNAS_CUARENTENA)


def _fila_cuarentena(fila: pd.Series, motivos: list[str]) -> dict[str, Any]:
    return {
        "id_orden": fila.get("id_orden"),
        "fecha": fila.get("_fecha_original", fila.get("fecha")),
        "id_producto": fila.get("id_producto"),
        "cantidad": fila.get("cantidad"),
        "precio_unitario_mxn": fila.get("_precio_original", fila.get("precio_unitario_mxn")),
        "monto_total_mxn": fila.get("_total_original", fila.get("monto_total_mxn")),
        "email": fila.get("email") if _no_vacio(fila.get("email")) else "ANONIMO",
        "motivo_cuarentena": "; ".join(dict.fromkeys(motivos)),
        "fase": "F2/F4",
    }


def _no_vacio(valor: Any) -> bool:
    return valor is not None and not pd.isna(valor) and str(valor).strip() != ""


def validar_esquema_final(ventas: pd.DataFrame) -> pd.DataFrame:
    if ventas.columns.tolist() != COLUMNAS_VENTA_FINAL:
        raise ValueError(
            "El esquema final no coincide con el requerido: "
            f"{ventas.columns.tolist()}"
        )
    if ventas.empty:
        return ventas
    if ventas["id_orden"].isna().any() or ventas["id_producto"].isna().any():
        raise ValueError("Hay identificadores de orden o producto vacíos.")
    fechas_ok = ventas["fecha"].astype(str).map(
        lambda valor: bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", valor))
    )
    if not fechas_ok.all():
        raise ValueError("Hay fechas que no cumplen YYYY-MM-DD.")
    cantidades = pd.to_numeric(ventas["cantidad"], errors="coerce")
    if cantidades.isna().any() or (cantidades <= 0).any() or not (cantidades % 1 == 0).all():
        raise ValueError("Hay cantidades inválidas en el resultado final.")
    for columna in ("precio_unitario_mxn", "monto_total_mxn", "monto_escalado"):
        valores = pd.to_numeric(ventas[columna], errors="coerce")
        if valores.isna().any() or not valores.map(math.isfinite).all():
            raise ValueError(f"Hay valores no numéricos o no finitos en {columna}.")
    coherencia = (
        pd.to_numeric(ventas["cantidad"]) * pd.to_numeric(ventas["precio_unitario_mxn"])
    ).round(2)
    if not (coherencia == pd.to_numeric(ventas["monto_total_mxn"]).round(2)).all():
        raise ValueError("Hay totales que no coinciden con cantidad por precio.")
    if not pd.to_numeric(ventas["monto_escalado"]).between(0, 1).all():
        raise ValueError("Hay valores Min-Max fuera del rango [0, 1].")
    if not ventas["segmento_importe"].isin(["Bajo", "Medio", "Alto"]).all():
        raise ValueError("Hay segmentos de importe no permitidos.")
    if not ventas["email"].map(_no_vacio).all():
        raise ValueError("Hay correos vacíos sin normalizar a ANONIMO.")
    return ventas


def _preparar_salida_final(ventas: pd.DataFrame) -> pd.DataFrame:
    salida = ventas.copy()
    salida["email"] = _columna_texto(salida, "email", "ANONIMO")
    salida["email"] = salida["email"].map(
        lambda valor: valor if _no_vacio(valor) else "ANONIMO"
    )
    salida["id_orden"] = salida["id_orden"].astype("string")
    salida["id_producto"] = salida["id_producto"].astype("string")
    salida["cantidad"] = salida["cantidad"].astype("int64")
    salida["precio_unitario_mxn"] = salida["precio_unitario_mxn"].astype(float).round(2)
    salida["monto_total_mxn"] = salida["monto_total_mxn"].astype(float).round(2)
    salida["monto_escalado"] = salida["monto_escalado"].astype(float).round(6)
    return salida[COLUMNAS_VENTA_FINAL]


def _crear_reporte(
    ruta: Path,
    metricas: dict[str, Any],
    ventas: pd.DataFrame,
    cuarentena: pd.DataFrame,
    discrepancias: pd.DataFrame,
    detalle_derivadas: dict[str, Any],
    advertencias_precio: int,
) -> None:
    conteos = ventas["segmento_importe"].value_counts().to_dict() if not ventas.empty else {}
    total = len(ventas)
    distribucion = {
        etiqueta: {
            "cantidad": int(conteos.get(etiqueta, 0)),
            "porcentaje": round(100 * int(conteos.get(etiqueta, 0)) / total, 2) if total else 0,
        }
        for etiqueta in ("Bajo", "Medio", "Alto")
    }
    lineas = [
        "# Reporte de integración ETL — Ingeniero 2",
        "",
        "## Resumen de ejecución",
        f"- Registros finales integrados: **{total}**",
        f"- Registros en cuarentena de esta ejecución: **{len(cuarentena)}**",
        f"- Discrepancias entre total original y recalculado: **{len(discrepancias)}**",
        f"- Advertencias de precio contra catálogo (>1% o base no comparable): **{advertencias_precio}**",
        "- `ventas_cuarentena.csv` heredado se excluyó completamente del flujo de ventas; no se reincorporaron registros.",
        f"- Registros en cuarentena heredada (solo auditoría): **{metricas['cuarentena_heredada']['filas']}**; estado={metricas['cuarentena_heredada']['estado']}.",
        f"- Fuente online: **{metricas['fuentes']['online']['estado']}**.",
        "",
        "## Extracción",
    ]
    for fuente, datos in metricas["fuentes"].items():
        lineas.append(
            f"- **{fuente}** ({datos['ruta']}): estado={datos['estado']}; "
            f"filas leídas={datos['filas_leidas']}; "
            f"duplicados exactos eliminados={datos['duplicados_exactos']}."
        )
    lineas.extend(
        [
            "",
            "## Transformaciones",
            "- Los importes MXN se redondearon a dos decimales y `monto_total_mxn` se recalculó como cantidad × precio unitario.",
            f"- Tipo de cambio configurable del ejercicio: **1 USD = {TIPO_CAMBIO_USD_MXN:.2f} MXN**; solo se convierte si existe evidencia de USD.",
            f"- Min-Max: mínimo **{detalle_derivadas['minimo_mxn']} MXN**, máximo **{detalle_derivadas['maximo_mxn']} MXN**. Se calcula solo con ventas integradas.",
            f"- Segmentación: {detalle_derivadas['segmentacion']}",
            "- Min-Max depende del conjunto válido de esta ejecución; sus resultados pueden diferir de los campos derivados del CSV de entrada.",
            "- Los precios fuera de tolerancia generan advertencia, no rechazo, porque pueden reflejar descuentos o promociones.",
            "",
            "## Distribución de segmentos",
            "| Segmento | Registros | Porcentaje |",
            "|---|---:|---:|",
        ]
    )
    for etiqueta, dato in distribucion.items():
        lineas.append(f"| {etiqueta} | {dato['cantidad']} | {dato['porcentaje']:.2f}% |")
    lineas.extend(
        [
            "",
            "## Limitaciones y decisiones",
            "- La cuarentena heredada se considera únicamente auditoría y no una fuente recuperable automáticamente.",
            "- Fechas con interpretación ambigua se desvían a cuarentena; se aceptan ISO, año/mes/día y formatos día/mes o mes/día solo si no son ambiguos.",
            "- Si la fuente online no está, se procesa la fuente física disponible. Un JSON mal formado o incompatible se registra y excluye sin simular datos.",
            "- Los cuantiles dependen de los valores observados; valores repetidos pueden reducir el número de segmentos.",
            "- No se encontró `GUIA_LINEAMIENTOS_PROYECTO_ALUMNOS.pdf` entre los archivos del proyecto; las reglas aplicadas corresponden al prompt proporcionado.",
            "",
            "## Artefactos",
            "- `ventas_integradas.csv`: esquema final de nueve columnas.",
            "- `ventas_integradas.xlsx`: libro de Excel con valores numéricos y formato explícito para montos y escala Min-Max.",
            "- `cuarentena_integracion.csv`: rechazos de esta ejecución, con motivo y fase F2/F4.",
            "- `reporte_discrepancias.csv`: diferencias superiores a $0.01 MXN respecto al total recalculado.",
            "- `etl_ingeniero2.log`: detalle cronológico de la ejecución.",
            "",
            "## Ejecución",
            'Instalar dependencia: `python -m pip install "pandas>=2.0"`.',
            "Ejecutar desde la carpeta del proyecto: `python etl_ingeniero2.py`.",
            "Opciones: `--ventas-csv`, `--catalogo-csv`, `--output-dir` y `--online-json`.",
            "",
            "Ejecución completada y salidas validadas antes de su publicación.",
        ]
    )
    ruta.write_text("\n".join(lineas) + "\n", encoding="utf-8")


def exportar_resultados(
    salida_dir: Path,
    ventas: pd.DataFrame,
    cuarentena: pd.DataFrame,
    discrepancias: pd.DataFrame,
    reporte_writer: Any,
) -> None:
    salida_dir.mkdir(parents=True, exist_ok=True)
    destino_archivos = {
        "ventas_integradas.csv": ventas,
        "cuarentena_integracion.csv": cuarentena,
        "reporte_discrepancias.csv": discrepancias,
    }
    with tempfile.TemporaryDirectory(prefix=".etl_ingeniero2_", dir=salida_dir) as tmp:
        temporal = Path(tmp)
        for nombre, tabla in destino_archivos.items():
            exportacion = tabla.copy()
            for columna in ("precio_unitario_mxn", "monto_total_mxn"):
                if columna in exportacion.columns:
                    exportacion[columna] = exportacion[columna].map(
                        lambda valor: f"{float(valor):.2f}" if _no_vacio(valor) else ""
                    )
            if "monto_escalado" in exportacion.columns:
                exportacion["monto_escalado"] = exportacion["monto_escalado"].map(
                    lambda valor: f"{float(valor):.6f}" if _no_vacio(valor) else ""
                )
            exportacion.to_csv(temporal / nombre, index=False, encoding="utf-8")

        ruta_excel = temporal / "ventas_integradas.xlsx"
        with pd.ExcelWriter(ruta_excel, engine="openpyxl") as escritor:
            ventas.to_excel(escritor, sheet_name="Ventas integradas", index=False)
            hoja = escritor.sheets["Ventas integradas"]
            hoja.freeze_panes = "A2"
            hoja.auto_filter.ref = hoja.dimensions
            formatos = {
                "precio_unitario_mxn": "0.00",
                "monto_total_mxn": "0.00",
                "monto_escalado": "0.000000",
            }
            for indice, columna in enumerate(ventas.columns, start=1):
                letra = hoja.cell(row=1, column=indice).column_letter
                hoja.column_dimensions[letra].width = min(max(len(str(columna)) + 2, 14), 30)
                formato = formatos.get(columna)
                if formato:
                    for fila in range(2, len(ventas) + 2):
                        hoja.cell(row=fila, column=indice).number_format = formato

        reporte_writer(temporal / "REPORTE_INTEGRACION.md")
        # Publica únicamente después de producir y validar todas las tablas.
        for archivo in temporal.iterdir():
            os.replace(archivo, salida_dir / archivo.name)


def ejecutar(args: argparse.Namespace) -> int:
    salida_dir = args.output_dir.resolve()
    salida_dir.mkdir(parents=True, exist_ok=True)
    logger = configurar_logging(salida_dir / "etl_ingeniero2.log")
    logger.info("Inicio del pipeline ETL Ingeniero 2.")
    logger.info("Directorio de salida: %s", salida_dir)
    metricas: dict[str, Any] = {"fuentes": {}}
    try:
        inspeccionar_cuarentena_heredada(
            args.cuarentena_csv, logger, metricas
        )
        catalogo = cargar_catalogo(args.catalogo_csv, logger)
        ventas_csv = cargar_ventas_csv(args.ventas_csv, "pos", logger, metricas)
        online = cargar_ventas_online(args.online_json, logger, metricas)
        fuentes = [fuente for fuente in (ventas_csv, online) if not fuente.empty]
        if not fuentes:
            raise ValueError(
                "No hay fuentes de ventas válidas. Verifique el CSV POS y, si aplica, el JSON online."
            )
        ventas = eliminar_duplicados(pd.concat(fuentes, ignore_index=True, sort=False), logger)
        # Mantener identificadores como texto conserva ceros iniciales y su semántica.
        for columna in ("id_orden", "id_producto", "email"):
            if columna in ventas.columns:
                ventas[columna] = ventas[columna].astype("string")
        ventas["id_orden"] = _columna_texto(ventas, "id_orden")
        ventas["id_producto"] = _columna_texto(ventas, "id_producto")
        ventas["email"] = _columna_texto(ventas, "email")
        ventas = normalizar_fechas(ventas, logger)
        ventas, _ = normalizar_monedas(ventas, logger)
        candidatas, rechazadas, discrepancias = validar_reglas_negocio(ventas, logger)
        integradas, huerfanas, advertencias_precio = integrar_catalogo(
            candidatas, catalogo, logger
        )
        cuarentena = generar_cuarentena(rechazadas, huerfanas)
        derivadas, detalle_derivadas = calcular_variables_derivadas(integradas, logger)
        salida_ventas = _preparar_salida_final(derivadas)
        validar_esquema_final(salida_ventas)
        if not salida_ventas["id_producto"].isin(catalogo["id_producto"]).all():
            raise ValueError("Validación final detectó productos huérfanos.")

        # El archivo vacío conserva su cabecera para facilitar consumo downstream.
        if discrepancias.empty:
            discrepancias = pd.DataFrame(columns=COLUMNAS_DISCREPANCIAS)
        if cuarentena.empty:
            cuarentena = pd.DataFrame(columns=COLUMNAS_CUARENTENA)
        conteos_segmento = salida_ventas["segmento_importe"].value_counts().to_dict()
        for segmento in ("Bajo", "Medio", "Alto"):
            logger.info(
                "Segmento %s: %d registros (%.2f%%)",
                segmento,
                conteos_segmento.get(segmento, 0),
                100 * conteos_segmento.get(segmento, 0) / len(salida_ventas)
                if len(salida_ventas)
                else 0,
            )

        def escribir_reporte(ruta: Path) -> None:
            _crear_reporte(
                ruta,
                metricas,
                salida_ventas,
                cuarentena,
                discrepancias,
                detalle_derivadas,
                advertencias_precio,
            )

        exportar_resultados(
            salida_dir, salida_ventas, cuarentena, discrepancias, escribir_reporte
        )
        for nombre in (
            "ventas_integradas.csv",
            "ventas_integradas.xlsx",
            "cuarentena_integracion.csv",
            "reporte_discrepancias.csv",
            "REPORTE_INTEGRACION.md",
            "etl_ingeniero2.log",
        ):
            logger.info("Salida generada: %s", salida_dir / nombre)
        logger.info(
            "Pipeline completado. Integradas=%d; cuarentena=%d; discrepancias=%d.",
            len(salida_ventas),
            len(cuarentena),
            len(discrepancias),
        )
        logger.info("Fin del pipeline ETL Ingeniero 2.")
        print(
            f"ETL completado: {len(salida_ventas)} ventas integradas, "
            f"{len(cuarentena)} en cuarentena, {len(discrepancias)} discrepancias."
        )
        return 0
    except (OSError, ValueError, pd.errors.ParserError, pd.errors.EmptyDataError) as error:
        logger.exception("El pipeline terminó con error crítico: %s", error)
        print(f"ETL fallido: {error}", file=sys.stderr)
        return 1


def _argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Integra ventas POS y opcionalmente online, valida el catálogo y genera salidas auditables."
    )
    parser.add_argument(
        "--ventas-csv",
        type=Path,
        default=Path("ventas_limpias.csv"),
        help="CSV de ventas físicas/POS (por defecto: ventas_limpias.csv).",
    )
    parser.add_argument(
        "--catalogo-csv",
        type=Path,
        default=Path("catalogo_productos_limpio.csv"),
        help="Catálogo maestro (por defecto: catalogo_productos_limpio.csv).",
    )
    parser.add_argument(
        "--cuarentena-csv",
        type=Path,
        default=Path("ventas_cuarentena.csv"),
        help="Cuarentena heredada, solo para métricas de auditoría.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("."),
        help="Directorio para los resultados (por defecto: directorio actual).",
    )
    parser.add_argument(
        "--online-json",
        type=Path,
        default=None,
        help="Fuente JSON online opcional.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(ejecutar(_argumentos()))
