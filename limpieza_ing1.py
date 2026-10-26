"""
limpieza_ing1.py
=========================
Módulo Unificado de Calidad, Nulos y Outliers (CSV + JSON)
Integra perfilado estadístico, capping IQR y homologación para ETL.
"""

import argparse
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("calidad")

# ============================================================================
# 0. CONFIGURACIÓN
# ============================================================================
@dataclass
class ConfigCalidad:
    col_id: str = ""
    col_fecha: str = ""
    col_cantidad: str = ""
    col_precio: str = ""
    col_total: str = ""
    col_cliente: str = ""
    col_producto: str = ""
    col_pago: str = ""
    col_sucursal: str = ""

    cols_numericas: list[str] = field(default_factory=list)
    cols_categoricas: list[str] = field(default_factory=list)
    etiquetas_faltante: dict[str, str] = field(default_factory=dict)
    
    factor_iqr: float = 1.5
    cols_outliers: list[str] = field(default_factory=list)
    tolerancia_total: float = 0.05
    formatos_fecha: tuple[str, ...] = ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y")
    alpha_chi2: float = 0.05

TOKENS_NULOS = {"", "nan", "none", "null", "na", "n/a", "-", "--", "sin dato", "?"}

# ============================================================================
# 1. UTILIDADES Y LECTURA
# ============================================================================
def leer_crudo(ruta: Path) -> pd.DataFrame:
    if not ruta.exists():
        raise FileNotFoundError(f"No existe el archivo: {ruta}")
    
    if ruta.suffix.lower() == '.json':
        df = pd.read_json(ruta)
        df = df.astype(str) # Forzar texto para limpieza
        log.info("Leído %s (%d filas, %d columnas, JSON)", ruta.name, *df.shape)
    else:
        for enc in ("utf-8", "utf-8-sig", "latin-1"):
            try:
                df = pd.read_csv(ruta, dtype=str, keep_default_na=False, encoding=enc)
                log.info("Leído %s (%d filas, %d columnas, CSV)", ruta.name, *df.shape)
                break
            except UnicodeDecodeError:
                continue
            
    df.columns = [c.strip().lower() for c in df.columns]
    return df.apply(lambda s: s.map(lambda v: np.nan if str(v).strip().lower() in TOKENS_NULOS else str(v).strip()))

def parsear_fechas(serie: pd.Series, formatos: tuple[str, ...]) -> pd.Series:
    resultado = pd.Series(pd.NaT, index=serie.index, dtype="datetime64[ns]")
    
    # Manejar timestamps Unix (comunes en JSON online)
    es_numerico = serie.str.isnumeric().fillna(False)
    if es_numerico.any():
        resultado[es_numerico] = pd.to_datetime(serie[es_numerico].astype(int), unit='s', errors='coerce')
        
    for fmt in formatos:
        pendientes = resultado.isna() & serie.notna()
        if not pendientes.any(): break
        resultado[pendientes] = pd.to_datetime(serie[pendientes], format=fmt, errors="coerce")
    return resultado

def _normalizar_texto(v):
    if pd.isna(v): return v
    return re.sub(r"\s+", " ", str(v)).strip()

def estandarizar_tipos(df: pd.DataFrame, cfg: ConfigCalidad) -> pd.DataFrame:
    out = df.copy()
    for c in cfg.cols_numericas:
        if c in out:
            limpio = out[c].astype("string").str.replace(r"[$,\s]", "", regex=True)
            out[c] = pd.to_numeric(limpio, errors="coerce")
    if cfg.col_fecha in out:
        out[cfg.col_fecha] = parsear_fechas(out[cfg.col_fecha], cfg.formatos_fecha)
    for c in cfg.cols_categoricas:
        if c in out:
            out[c] = out[c].map(_normalizar_texto)
    if cfg.col_producto in out:
        out[cfg.col_producto] = out[cfg.col_producto].str.upper()
    return out

# ============================================================================
# 2. CUARENTENA Y REGLAS DE IMPOSIBILIDAD
# ============================================================================
class Cuarentena:
    def __init__(self):
        self._partes = []

    def enviar(self, df: pd.DataFrame, mascara: pd.Series, motivo: str, fase: str) -> pd.DataFrame:
        mascara = mascara.fillna(False).astype(bool)
        if mascara.any():
            rechazadas = df.loc[mascara].copy()
            rechazadas["motivo_cuarentena"] = motivo
            rechazadas["fase"] = fase
            self._partes.append(rechazadas)
            log.info("Cuarentena [%s] %-40s -> %d filas", fase, motivo, int(mascara.sum()))
        return df.loc[~mascara].copy()

    @property
    def df(self) -> pd.DataFrame:
        return pd.concat(self._partes, ignore_index=True) if self._partes else pd.DataFrame()

def es_correo_invalido(email):
    email = str(email).strip()
    if email == 'ANONIMO' or email == 'nan': return False
    if '@@' in email or '..' in email or '@' not in email: return True
    return False

def filtrar_imposibles(df: pd.DataFrame, cfg: ConfigCalidad, cuarentena: Cuarentena) -> pd.DataFrame:
    out = cuarentena.enviar(df, df[cfg.col_cantidad] <= 0, "cantidad <= 0 (imposible)", "F3")
    out = cuarentena.enviar(out, out[cfg.col_precio] <= 0, "precio <= 0 (imposible)", "F3")
    out = cuarentena.enviar(out, out[cfg.col_cantidad].isna() | out[cfg.col_precio].isna(), "cantidad/precio irrecuperable", "F2")
    out = cuarentena.enviar(out, out[cfg.col_fecha].isna(), "fecha no interpretable", "F1")
    
    # Filtro específico para correos basura en online
    if "customer_email" in out.columns:
        mask_correo = out["customer_email"].apply(es_correo_invalido)
        out = cuarentena.enviar(out, mask_correo, "Correo mal formado", "F1")
        
    return out

# ============================================================================
# 3. OUTLIERS (IQR CAPPING)
# ============================================================================
def limites_iqr(serie: pd.Series, k: float = 1.5) -> dict:
    s = pd.to_numeric(serie, errors="coerce").dropna()
    if s.empty:
        return {"lim_inf": np.nan, "lim_sup": np.nan}
    q1, q3 = s.quantile([0.25, 0.75])
    iqr = q3 - q1
    return {"lim_inf": q1 - k * iqr, "lim_sup": q3 + k * iqr}

def capping_iqr(df: pd.DataFrame, cols: list[str], k: float = 1.5, piso_fisico: dict = None) -> pd.DataFrame:
    out = df.copy()
    piso_fisico = piso_fisico or {}
    for c in cols:
        if c not in out: continue
        L = limites_iqr(out[c], k)
        inf = max(L["lim_inf"], piso_fisico.get(c, -np.inf))
        sup = L["lim_sup"]
        
        # 1. Convertir temporalmente a float para aceptar límites con decimales
        out[c] = out[c].astype(float)
        
        bajo, alto = out[c] < inf, out[c] > sup
        out[c] = out[c].clip(lower=inf, upper=sup)
        
        # 2. Si es columna de piezas/unidades, redondear y regresar a formato entero
        if c in ['cantidad', 'units']:
            out[c] = out[c].round().astype('Int64')
            
        log.info("Capping %s -> [%.2f, %.2f] recortados=%d", c, inf, sup, int((bajo | alto).sum()))
    return out

# ============================================================================
# 4. ORQUESTADOR PRINCIPAL
# ============================================================================
def ejecutar_pipeline(ruta_archivo: str, cfg: ConfigCalidad) -> dict:
    crudo = leer_crudo(Path(ruta_archivo))
    df = estandarizar_tipos(crudo, cfg)
    cuarentena = Cuarentena()

    # Duplicados
    df = cuarentena.enviar(df, df.duplicated(keep="first"), "duplicado exacto", "F1")

    # Imputación Categórica
    for col, etiqueta in cfg.etiquetas_faltante.items():
        if col in df: df[col] = df[col].fillna(etiqueta)

    # Imposibles y Cuarentena
    df = filtrar_imposibles(df, cfg, cuarentena)

    # Capping IQR
    df = capping_iqr(df, cfg.cols_outliers, cfg.factor_iqr, piso_fisico={cfg.col_cantidad: 1})
    
    # Recalcular total post-capping
    if cfg.col_total in df.columns:
        df[cfg.col_total] = (df[cfg.col_cantidad] * df[cfg.col_precio]).round(2)

    return {"limpio": df, "cuarentena": cuarentena.df}

# ============================================================================
# 5. EJECUCIÓN
# ============================================================================
def main():
    salida = Path("salida")
    salida.mkdir(exist_ok=True)

    # --- CONFIGURACIÓN TIENDA FÍSICA ---
    cfg_fisica = ConfigCalidad(
        col_id="id_ticket", col_fecha="fecha_venta", col_cantidad="cantidad",
        col_precio="precio_unitario", col_total="monto_total_mxn", col_producto="id_producto",
        cols_numericas=["cantidad", "precio_unitario", "monto_total_mxn"],
        cols_categoricas=["id_producto", "id_cliente", "metodo_pago", "sucursal"],
        cols_outliers=["cantidad", "precio_unitario", "monto_total_mxn"],
        etiquetas_faltante={"id_cliente": "ANONIMO", "metodo_pago": "NO_ESPECIFICADO", "sucursal": "SIN_SUCURSAL"}
    )
    
    print("\n--- PROCESANDO TIENDA FÍSICA ---")
    try:
        res_fis = ejecutar_pipeline("ventas_tienda_fisica.csv", cfg_fisica)
        df_fis = res_fis["limpio"].rename(columns={
            'id_ticket': 'id_orden', 'fecha_venta': 'fecha', 'precio_unitario': 'precio_unitario_mxn'
        })
        df_fis.to_csv(salida / "ventas_fisica_limpias.csv", index=False, encoding='utf-8')
        res_fis["cuarentena"].to_csv(salida / "cuarentena_fisica.csv", index=False, encoding='utf-8')
        print(f"✅ Física completada -> Limpios: {len(df_fis)} | Cuarentena: {len(res_fis['cuarentena'])}")
    except FileNotFoundError:
        print("❌ Archivo ventas_tienda_fisica.csv no encontrado.")

    # --- CONFIGURACIÓN TIENDA ONLINE ---
    cfg_online = ConfigCalidad(
        col_id="order_id", col_fecha="order_timestamp", col_cantidad="units",
        col_precio="unit_price", col_total="total_amount", col_producto="product_id",
        cols_numericas=["units", "unit_price", "total_amount"],
        cols_categoricas=["product_id", "customer_email", "currency", "order_status"],
        cols_outliers=["units", "unit_price", "total_amount"],
        etiquetas_faltante={"customer_email": "ANONIMO", "shipping_address": "NO PROPORCIONADA"}
    )
    
    print("\n--- PROCESANDO TIENDA ONLINE ---")
    try:
        res_on = ejecutar_pipeline("ventas_online.json", cfg_online)
        df_on = res_on["limpio"].rename(columns={
            'order_id': 'id_orden', 'order_timestamp': 'fecha', 'product_id': 'id_producto',
            'units': 'cantidad', 'unit_price': 'precio_unitario', 'total_amount': 'monto_total',
            'customer_email': 'email', 'currency': 'moneda'
        })
        df_on.to_json(salida / "ventas_online_limpias.json", orient='records', force_ascii=False, indent=2)
        res_on["cuarentena"].to_json(salida / "cuarentena_online.json", orient='records', force_ascii=False, indent=2)
        print(f"✅ Online completada -> Limpios: {len(df_on)} | Cuarentena: {len(res_on['cuarentena'])}")
    except FileNotFoundError:
        print("❌ Archivo ventas_online.json no encontrado.")

if __name__ == "__main__":
    main()