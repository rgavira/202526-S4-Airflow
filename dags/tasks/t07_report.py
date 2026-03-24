"""
t07_report.py
-------------
TAREA 07/08 — Analíticas con storytelling + generación de reporte.

Ejecuta tres análisis de negocio sobre la tabla PostgreSQL:

  A. Ineficiencia térmica
     → Exterior < 15 °C pero interior > 23 °C
       (calefacción encendida con posible ventana abierta)

  B. Calidad del aire crítica
     → Cualquier zona con air_* > 1500 ppm
       (CO₂ elevado, riesgo para la salud, necesita ventilación)

  C. Resumen estadístico general del periodo analizado

El reporte se guarda como JSON legible en:
  /opt/airflow/logs_analytics/reporte_YYYYMMDD_HHMMSS.log
"""

import json
import logging
import os
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from tasks.config import LOGS_DIR, PG_TABLE, POSTGRES_CONN_ID


def _rows_to_dicts(rows, cols):
    """Convierte filas de get_records en lista de dicts con fechas serializadas."""
    return [
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in zip(cols, row)}
        for row in rows
    ]


def run_analytics_and_report(**context):
    hook    = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    exec_dt = context["execution_date"]

    logging.info("=" * 60)
    logging.info("TAREA 07/08 — ANALÍTICAS Y REPORTE")
    logging.info("=" * 60)

    # ── A: Ineficiencia Térmica ───────────────────────────────────────────────
    sql_a = f"""
        SELECT
            timestamp,
            ROUND(temperature_exterieur::NUMERIC, 2)              AS temp_exterior,
            ROUND(avg_indoor_temperature::NUMERIC, 2)             AS avg_temp_interior,
            ROUND((avg_indoor_temperature
                   - temperature_exterieur)::NUMERIC, 2)          AS thermal_gap_c,
            ROUND(temperature_salon::NUMERIC, 2)                  AS temp_salon,
            ROUND(temperature_chambre::NUMERIC, 2)                AS temp_chambre,
            ROUND(temperature_bureau::NUMERIC, 2)                 AS temp_bureau
        FROM {PG_TABLE}
        WHERE temperature_exterieur < 15
          AND avg_indoor_temperature > 23
        ORDER BY thermal_gap_c DESC
        LIMIT 20;
    """
    cols_a  = ["timestamp","temp_exterior","avg_temp_interior",
               "thermal_gap_c","temp_salon","temp_chambre","temp_bureau"]
    events_a = _rows_to_dicts(hook.get_records(sql_a), cols_a)

    analysis_a = {
        "titulo": "Detección de ineficiencia térmica",
        "descripcion": (
            "Momentos donde la temperatura exterior < 15 °C "
            "pero la temperatura interior media > 23 °C. "
            "Posible causa: calefacción encendida con ventanas abiertas."
        ),
        "umbrales": {"exterior_max_c": 15, "interior_min_c": 23},
        "severidad": "ALTA" if len(events_a) > 5 else "MEDIA" if events_a else "NORMAL",
        "eventos_detectados": len(events_a),
        "top_10_eventos": events_a[:10],
        "recomendacion": (
            f"Se detectaron {len(events_a)} episodios. "
            "Revisa la programación del termostato y el estado de las ventanas."
            if events_a else
            "No se detectaron episodios de ineficiencia térmica en este lote."
        ),
    }

    # ── B: Calidad del Aire Crítica ───────────────────────────────────────────
    sql_b = f"""
        SELECT
            timestamp,
            ROUND(air_salon::NUMERIC, 0)              AS air_salon_ppm,
            ROUND(air_chambre::NUMERIC, 0)            AS air_chambre_ppm,
            ROUND(air_bureau::NUMERIC, 0)             AS air_bureau_ppm,
            ROUND(avg_indoor_air_quality::NUMERIC, 0) AS avg_air_ppm,
            CASE
                WHEN air_salon   > 1500
                 AND air_chambre > 1500
                 AND air_bureau  > 1500 THEN 'TODA LA CASA'
                WHEN air_salon   > 1500
                 AND air_chambre > 1500 THEN 'SALON + HABITACION'
                WHEN air_salon   > 1500
                 AND air_bureau  > 1500 THEN 'SALON + DESPACHO'
                WHEN air_chambre > 1500
                 AND air_bureau  > 1500 THEN 'HABITACION + DESPACHO'
                WHEN air_salon   > 1500 THEN 'SALON'
                WHEN air_chambre > 1500 THEN 'HABITACION'
                WHEN air_bureau  > 1500 THEN 'DESPACHO'
                ELSE 'MULTIPLE'
            END AS zona_afectada
        FROM {PG_TABLE}
        WHERE air_salon > 1500 OR air_chambre > 1500 OR air_bureau > 1500
        ORDER BY avg_air_ppm DESC
        LIMIT 20;
    """
    cols_b  = ["timestamp","air_salon_ppm","air_chambre_ppm",
               "air_bureau_ppm","avg_air_ppm","zona_afectada"]
    events_b = _rows_to_dicts(hook.get_records(sql_b), cols_b)

    zone_counts: dict = {}
    for e in events_b:
        zone_counts[e["zona_afectada"]] = zone_counts.get(e["zona_afectada"], 0) + 1

    analysis_b = {
        "titulo": "Calidad del aire crítica (CO₂ elevado)",
        "descripcion": (
            "Periodos donde alguna zona del hogar supera los 1500 ppm. "
            "Valores > 1500 ppm indican ventilación insuficiente."
        ),
        "umbral_ppm": 1500,
        "severidad": "CRITICA" if len(events_b) > 10 else "ALTA" if events_b else "OK",
        "eventos_detectados": len(events_b),
        "zonas_afectadas": zone_counts,
        "top_10_eventos": events_b[:10],
        "recomendacion": (
            f"Se detectaron {len(events_b)} periodos críticos. "
            f"Zonas afectadas: {zone_counts}. Ventilación inmediata recomendada."
            if events_b else
            "La calidad del aire se mantiene dentro de los umbrales recomendados."
        ),
    }

    # ── C: Resumen Estadístico ────────────────────────────────────────────────
    sql_c = f"""
        SELECT
            COUNT(*)                                         AS total_registros,
            MIN(timestamp)                                   AS primer_registro,
            MAX(timestamp)                                   AS ultimo_registro,
            ROUND(AVG(avg_indoor_temperature)::NUMERIC, 2)  AS media_temp_interior,
            ROUND(MIN(avg_indoor_temperature)::NUMERIC, 2)  AS min_temp_interior,
            ROUND(MAX(avg_indoor_temperature)::NUMERIC, 2)  AS max_temp_interior,
            ROUND(AVG(avg_indoor_humidity)::NUMERIC, 2)     AS media_hum_interior,
            ROUND(AVG(temperature_exterieur)::NUMERIC, 2)   AS media_temp_exterior,
            ROUND(MIN(temperature_exterieur)::NUMERIC, 2)   AS min_temp_exterior,
            ROUND(MAX(temperature_exterieur)::NUMERIC, 2)   AS max_temp_exterior,
            ROUND(AVG(avg_indoor_air_quality)::NUMERIC, 0)  AS media_aire_ppm
        FROM {PG_TABLE};
    """
    cols_c = [
        "total_registros","primer_registro","ultimo_registro",
        "media_temp_interior","min_temp_interior","max_temp_interior",
        "media_hum_interior",
        "media_temp_exterior","min_temp_exterior","max_temp_exterior",
        "media_aire_ppm",
    ]
    row_c   = hook.get_first(sql_c) or []
    summary = {
        k: (v.isoformat() if hasattr(v, "isoformat") else v)
        for k, v in zip(cols_c, row_c)
    }

    analysis_c = {
        "titulo": "Resumen estadístico del periodo",
        "descripcion": "Estadísticas generales de todas las lecturas en la base de datos.",
        "estadisticas": summary,
    }

    # ── Construir y guardar el reporte ────────────────────────────────────────
    report = {
        "metadata": {
            "pipeline":       "Entregable 3 — Home Sensor Pipeline",
            "dag_run_id":     context["run_id"],
            "execution_date": exec_dt.isoformat(),
            "generado_en":    datetime.now().isoformat(),
            "tabla_fuente":   PG_TABLE,
        },
        "analisis": {
            "A_ineficiencia_termica": analysis_a,
            "B_calidad_aire_critica": analysis_b,
            "C_resumen_estadistico":  analysis_c,
        },
    }

    os.makedirs(LOGS_DIR, exist_ok=True)
    report_path = os.path.join(LOGS_DIR, f"reporte_{exec_dt.strftime('%Y%m%d_%H%M%S')}.log")
    sep = "=" * 70

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"{sep}\n REPORTE ANALÍTICO — HOME SENSOR PIPELINE\n")
        f.write(f" Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{sep}\n\n")
        f.write(json.dumps(report, indent=2, ensure_ascii=False, default=str))
        f.write(f"\n\n{sep}\n FIN DEL REPORTE\n{sep}\n")

    logging.info(f"Reporte guardado en: {report_path}")
    logging.info(f"\n{'='*60}")
    logging.info(f"  Registros en BD         : {summary.get('total_registros', 'N/A')}")
    logging.info(f"  Ineficiencia térmica    : {analysis_a['eventos_detectados']} eventos ({analysis_a['severidad']})")
    logging.info(f"  Calidad aire crítica    : {analysis_b['eventos_detectados']} eventos ({analysis_b['severidad']})")
    logging.info(f"{'='*60}")

    return report_path
