# Reporte de integración ETL — Ingeniero 2

## Resumen de ejecución
- Registros finales integrados: **562**
- Registros en cuarentena de esta ejecución: **227**
- Discrepancias entre total original y recalculado: **0**
- Advertencias de precio contra catálogo (>1% o base no comparable): **0**
- `ventas_cuarentena.csv` heredado se excluyó completamente del flujo de ventas; no se reincorporaron registros.
- Registros en cuarentena heredada (solo auditoría): **0**; estado=ausente.
- Fuente online: **cargado**.

## Extracción
- **pos** (C:\Users\alexm\Documents\Escuela\Semestre 7\Ingenería de Datos\Proyecto Pipeline automatico\salida\ventas_fisica_limpias.csv): estado=cargado; filas leídas=589; duplicados exactos eliminados=0.
- **online** (C:\Users\alexm\Documents\Escuela\Semestre 7\Ingenería de Datos\Proyecto Pipeline automatico\salida\ventas_online_limpias.json): estado=cargado; filas leídas=200; duplicados exactos eliminados=0.

## Transformaciones
- Los importes MXN se redondearon a dos decimales y `monto_total_mxn` se recalculó como cantidad × precio unitario.
- Tipo de cambio configurable del ejercicio: **1 USD = 19.50 MXN**; solo se convierte si existe evidencia de USD.
- Min-Max: mínimo **332.7 MXN**, máximo **33744.88 MXN**. Se calcula solo con ventas integradas.
- Segmentación: Cuantiles qcut; intervalos obtenidos=3; límites=[332.7, 2457.44, 5972.24, 33744.88].
- Min-Max depende del conjunto válido de esta ejecución; sus resultados pueden diferir de los campos derivados del CSV de entrada.
- Los precios fuera de tolerancia generan advertencia, no rechazo, porque pueden reflejar descuentos o promociones.

## Distribución de segmentos
| Segmento | Registros | Porcentaje |
|---|---:|---:|
| Bajo | 191 | 33.99% |
| Medio | 184 | 32.74% |
| Alto | 187 | 33.27% |

## Limitaciones y decisiones
- La cuarentena heredada se considera únicamente auditoría y no una fuente recuperable automáticamente.
- Fechas con interpretación ambigua se desvían a cuarentena; se aceptan ISO, año/mes/día y formatos día/mes o mes/día solo si no son ambiguos.
- Si la fuente online no está, se procesa la fuente física disponible. Un JSON mal formado o incompatible se registra y excluye sin simular datos.
- Los cuantiles dependen de los valores observados; valores repetidos pueden reducir el número de segmentos.
- No se encontró `GUIA_LINEAMIENTOS_PROYECTO_ALUMNOS.pdf` entre los archivos del proyecto; las reglas aplicadas corresponden al prompt proporcionado.

## Artefactos
- `ventas_integradas.csv`: esquema final de nueve columnas.
- `ventas_integradas.xlsx`: libro de Excel con valores numéricos y formato explícito para montos y escala Min-Max.
- `cuarentena_integracion.csv`: rechazos de esta ejecución, con motivo y fase F2/F4.
- `reporte_discrepancias.csv`: diferencias superiores a $0.01 MXN respecto al total recalculado.
- `etl_ingeniero2.log`: detalle cronológico de la ejecución.

## Ejecución
Instalar dependencia: `python -m pip install "pandas>=2.0"`.
Ejecutar desde la carpeta del proyecto: `python etl_ingeniero2.py`.
Opciones: `--ventas-csv`, `--catalogo-csv`, `--output-dir` y `--online-json`.

Ejecución completada y salidas validadas antes de su publicación.
