"""
Suite de pruebas unitarias para el módulo de Validación, Resiliencia y Base de Datos.
Verifica:
1. Validación estricta del modelo Pydantic y su @field_validator.
2. Configuración DDL de tablas.
3. Aislamiento transaccional: commit en éxitos, rollback y cuarentena en fallos.
4. Resiliencia total (no crash).
"""

import unittest
from unittest.mock import MagicMock, call
from datetime import date
from pydantic import ValidationError

from pipeline_validacion_resiliencia import (
    Transaccion,
    configurar_tablas,
    cargar_lote_resiliente
)


class TestPipelineETL(unittest.TestCase):

    def setUp(self):
        self.registro_valido = {
            "id_orden": "ORD-101",
            "fecha": "2026-10-22",
            "id_producto": "PROD-A",
            "cantidad": 10,
            "precio_unitario_mxn": 50.0,
            "monto_total_mxn": 500.0,
            "email": "test@empresa.com",
            "segmento_importe": "MEDIO",
            "monto_escalado": 0.5
        }

    def test_modelo_transaccion_valido(self):
        """Verifica que un registro conforme sea parseado correctamente."""
        t = Transaccion(**self.registro_valido)
        self.assertEqual(t.id_orden, "ORD-101")
        self.assertEqual(t.fecha, date(2026, 10, 22))
        self.assertEqual(t.cantidad, 10)
        self.assertEqual(t.precio_unitario_mxn, 50.0)

    def test_validador_cantidad_negativa(self):
        """Verifica que cantidad <= 0 lance ValidationError por el @field_validator."""
        datos_invalidos = dict(self.registro_valido)
        datos_invalidos["cantidad"] = -5
        with self.assertRaises(ValidationError) as ctx:
            Transaccion(**datos_invalidos)
        self.assertIn("cantidad", str(ctx.exception))
        self.assertIn("mayor a 0", str(ctx.exception))

    def test_validador_cantidad_cero(self):
        """Verifica que cantidad = 0 lance ValidationError."""
        datos_invalidos = dict(self.registro_valido)
        datos_invalidos["cantidad"] = 0
        with self.assertRaises(ValidationError):
            Transaccion(**datos_invalidos)

    def test_configurar_tablas(self):
        """Verifica la ejecución de los DDL y el commit correspondiente."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        configurar_tablas(mock_conn)

        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_conn.commit.assert_called_once()

    def test_segregacion_y_resiliencia_transaccional(self):
        """
        Verifica que en un lote mixto:
        - Registro válido: INSERT en limpias + commit()
        - Registro inválido: rollback() + INSERT en cuarentena + commit()
        - El pipeline completa sin colapsar.
        """
        lote_mixto = [
            self.registro_valido,  # Válido
            {
                # Inválido: cantidad negativa
                "id_orden": "ORD-FAIL-01",
                "fecha": "2026-10-22",
                "id_producto": "PROD-B",
                "cantidad": -1,
                "precio_unitario_mxn": 100.0,
                "monto_total_mxn": 100.0,
                "email": "fail@empresa.com",
                "segmento_importe": "BAJO",
                "monto_escalado": 0.1
            },
            {
                # Inválido: campo requerido 'email' ausente
                "id_orden": "ORD-FAIL-02",
                "fecha": "2026-10-22",
                "id_producto": "PROD-C",
                "cantidad": 2,
                "precio_unitario_mxn": 50.0,
                "monto_total_mxn": 100.0,
                # email omitido
                "segmento_importe": "BAJO",
                "monto_escalado": 0.1
            }
        ]

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        metricas = cargar_lote_resiliente(mock_conn, lote_mixto)

        # Validar conteos
        self.assertEqual(metricas["total_procesados"], 3)
        self.assertEqual(metricas["insertados_limpias"], 1)
        self.assertEqual(metricas["enviados_cuarentena"], 2)
        self.assertEqual(metricas["errores_criticos"], 0)

        # Validar llamadas transaccionales:
        # 1 commit por el válido + 2 commits por los 2 de cuarentena = 3 commits
        self.assertEqual(mock_conn.commit.call_count, 3)
        # 2 rollbacks ejecutados por los 2 registros que fallaron en Pydantic
        self.assertEqual(mock_conn.rollback.call_count, 2)


if __name__ == "__main__":
    unittest.main()
