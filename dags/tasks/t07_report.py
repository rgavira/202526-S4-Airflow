"""
t07_report.py
-------------
TAREA 07/08 — Analíticas con storytelling + generación de reporte.

Ejecuta seis análisis de negocio sobre la tabla PostgreSQL:

  A. Ineficiencia térmica
     → Exterior < 15 °C pero interior > 23 °C

  B. Calidad del aire crítica
     → Cualquier zona con air_* > 1500 ppm

  C. Resumen estadístico general del periodo

  D. Confort por habitación
     → % de lecturas dentro de zona de confort (temp 20-24°C,
       humedad 40-60%, CO₂ < 1000 ppm) por sala

  E. Ritmo circadiano del CO₂
     → CO₂ medio por franja horaria (madrugada/mañana/tarde/noche)
       Revela rutinas del hogar reflejadas en el aire

  F. Evolución mensual de temperatura
     → Temperatura exterior e interior media por mes
       Con indicador de "riesgo de helada" (exterior < 3 °C)

El reporte se guarda como JSON legible en:
  /opt/airflow/logs_analytics/reporte_YYYYMMDD_HHMMSS.log
"""

import json
import logging
import os
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from tasks.config import LOGS_DIR, PG_TABLE, POSTGRES_CONN_ID
from tasks.log_utils import setup_file_logging


def _rows_to_dicts(rows, cols):
    """Convierte filas de get_records en lista de dicts con fechas serializadas."""
    return [
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in zip(cols, row)}
        for row in rows
    ]


def run_analytics_and_report(**context):
    hook    = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    exec_dt = context["execution_date"]
    ti      = context["task_instance"]
    SEP     = "=" * 60

    setup_file_logging("t07_report", context["execution_date"])
    logging.info(SEP)
    logging.info("TAREA 07/08 — ANALÍTICAS Y REPORTE")
    logging.info(f"  DAG run : {context['run_id']}")
    logging.info(SEP)

    # ── A: Ineficiencia Térmica ───────────────────────────────────────────────
    logging.info("[A] Calculando ineficiencia térmica...")
    count_a = hook.get_first(f"""
        SELECT COUNT(*) FROM {PG_TABLE}
        WHERE temperature_exterieur < 15 AND avg_indoor_temperature > 23
    """)[0]

    events_a = _rows_to_dicts(hook.get_records(f"""
        SELECT
            timestamp,
            ROUND(temperature_exterieur::NUMERIC, 2)                       AS temp_exterior,
            ROUND(avg_indoor_temperature::NUMERIC, 2)                      AS avg_temp_interior,
            ROUND((avg_indoor_temperature - temperature_exterieur)::NUMERIC, 2) AS thermal_gap_c,
            ROUND(temperature_salon::NUMERIC, 2)                           AS temp_salon,
            ROUND(temperature_chambre::NUMERIC, 2)                         AS temp_chambre,
            ROUND(temperature_bureau::NUMERIC, 2)                          AS temp_bureau
        FROM {PG_TABLE}
        WHERE temperature_exterieur < 15 AND avg_indoor_temperature > 23
        ORDER BY thermal_gap_c DESC
        LIMIT 10
    """), ["timestamp","temp_exterior","avg_temp_interior",
           "thermal_gap_c","temp_salon","temp_chambre","temp_bureau"])

    analysis_a = {
        "titulo": "Detección de ineficiencia térmica",
        "descripcion": (
            "Momentos donde la temperatura exterior < 15 °C "
            "pero la temperatura interior media > 23 °C. "
            "Posible causa: calefacción encendida con ventanas abiertas."
        ),
        "umbrales": {"exterior_max_c": 15, "interior_min_c": 23},
        "severidad": "ALTA" if count_a > 10 else "MEDIA" if count_a > 0 else "NORMAL",
        "total_eventos": count_a,
        "muestra_top10": events_a,
        "recomendacion": (
            f"Se detectaron {count_a} episodios en total. "
            "Revisa la programación del termostato y el estado de las ventanas."
            if count_a > 0 else
            "No se detectaron episodios de ineficiencia térmica."
        ),
    }

    # ── B: Calidad del Aire Crítica ───────────────────────────────────────────
    logging.info("[B] Calculando episodios de CO₂ crítico...")
    count_b = hook.get_first(f"""
        SELECT COUNT(*) FROM {PG_TABLE}
        WHERE air_salon > 1500 OR air_chambre > 1500 OR air_bureau > 1500
    """)[0]

    events_b = _rows_to_dicts(hook.get_records(f"""
        SELECT
            timestamp,
            ROUND(air_salon::NUMERIC, 0)              AS air_salon_ppm,
            ROUND(air_chambre::NUMERIC, 0)            AS air_chambre_ppm,
            ROUND(air_bureau::NUMERIC, 0)             AS air_bureau_ppm,
            ROUND(avg_indoor_air_quality::NUMERIC, 0) AS avg_air_ppm,
            CASE
                WHEN air_salon > 1500 AND air_chambre > 1500 AND air_bureau > 1500 THEN 'TODA LA CASA'
                WHEN air_salon > 1500 AND air_chambre > 1500 THEN 'SALON + HABITACION'
                WHEN air_salon > 1500 AND air_bureau  > 1500 THEN 'SALON + DESPACHO'
                WHEN air_chambre > 1500 AND air_bureau > 1500 THEN 'HABITACION + DESPACHO'
                WHEN air_salon   > 1500 THEN 'SALON'
                WHEN air_chambre > 1500 THEN 'HABITACION'
                WHEN air_bureau  > 1500 THEN 'DESPACHO'
                ELSE 'MULTIPLE'
            END AS zona_afectada
        FROM {PG_TABLE}
        WHERE air_salon > 1500 OR air_chambre > 1500 OR air_bureau > 1500
        ORDER BY avg_air_ppm DESC
        LIMIT 10
    """), ["timestamp","air_salon_ppm","air_chambre_ppm","air_bureau_ppm","avg_air_ppm","zona_afectada"])

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
        "severidad": "CRITICA" if count_b > 10 else "ALTA" if count_b > 0 else "OK",
        "total_eventos": count_b,
        "zonas_afectadas_muestra": zone_counts,
        "muestra_top10": events_b,
        "recomendacion": (
            f"Se detectaron {count_b} periodos críticos en total. "
            f"Zonas afectadas (muestra): {zone_counts}. Ventilación inmediata recomendada."
            if count_b > 0 else
            "La calidad del aire se mantiene dentro de los umbrales recomendados."
        ),
    }

    # ── C: Resumen Estadístico ────────────────────────────────────────────────
    logging.info("[C] Calculando resumen estadístico global...")
    row_c = hook.get_first(f"""
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
        FROM {PG_TABLE}
    """) or []
    cols_c = [
        "total_registros","primer_registro","ultimo_registro",
        "media_temp_interior","min_temp_interior","max_temp_interior",
        "media_hum_interior",
        "media_temp_exterior","min_temp_exterior","max_temp_exterior",
        "media_aire_ppm",
    ]
    summary = {
        k: (v.isoformat() if hasattr(v, "isoformat") else v)
        for k, v in zip(cols_c, row_c)
    }

    analysis_c = {
        "titulo": "Resumen estadístico del periodo completo",
        "descripcion": "Estadísticas globales sobre todas las lecturas almacenadas en la base de datos.",
        "estadisticas": summary,
    }

    # ── D: Confort por habitación ─────────────────────────────────────────────
    # Zona de confort: temp 20-24°C, humedad 40-60%, CO₂ < 1000 ppm
    # Revela qué sala vive mejor acondicionada de forma habitual
    logging.info("[D] Calculando confort por habitación...")
    comfort_rows = hook.get_records(f"""
        SELECT
            sala,
            total_lecturas,
            momentos_confort,
            ROUND(100.0 * momentos_confort / NULLIF(total_lecturas, 0), 1) AS pct_confort,
            media_temp,
            media_humedad,
            media_co2
        FROM (
            SELECT 'salon' AS sala,
                COUNT(*)                                                    AS total_lecturas,
                COUNT(*) FILTER (
                    WHERE temperature_salon BETWEEN 20 AND 24
                      AND humidity_salon    BETWEEN 40 AND 60
                      AND air_salon        < 1000
                )                                                           AS momentos_confort,
                ROUND(AVG(temperature_salon)::NUMERIC, 1)                  AS media_temp,
                ROUND(AVG(humidity_salon)::NUMERIC,    1)                  AS media_humedad,
                ROUND(AVG(air_salon)::NUMERIC,         0)                  AS media_co2
            FROM {PG_TABLE}
            UNION ALL
            SELECT 'habitacion',
                COUNT(*),
                COUNT(*) FILTER (
                    WHERE temperature_chambre BETWEEN 20 AND 24
                      AND humidity_chambre    BETWEEN 40 AND 60
                      AND air_chambre        < 1000
                ),
                ROUND(AVG(temperature_chambre)::NUMERIC, 1),
                ROUND(AVG(humidity_chambre)::NUMERIC,    1),
                ROUND(AVG(air_chambre)::NUMERIC,         0)
            FROM {PG_TABLE}
            UNION ALL
            SELECT 'despacho',
                COUNT(*),
                COUNT(*) FILTER (
                    WHERE temperature_bureau BETWEEN 20 AND 24
                      AND humidity_bureau    BETWEEN 40 AND 60
                      AND air_bureau        < 1000
                ),
                ROUND(AVG(temperature_bureau)::NUMERIC, 1),
                ROUND(AVG(humidity_bureau)::NUMERIC,    1),
                ROUND(AVG(air_bureau)::NUMERIC,         0)
            FROM {PG_TABLE}
        ) t
        ORDER BY pct_confort DESC
    """)
    comfort_cols = ["sala","total_lecturas","momentos_confort","pct_confort","media_temp","media_humedad","media_co2"]
    comfort_data = _rows_to_dicts(comfort_rows, comfort_cols)
    mejor_sala   = comfort_data[0]["sala"] if comfort_data else "N/A"
    peor_sala    = comfort_data[-1]["sala"] if comfort_data else "N/A"

    analysis_d = {
        "titulo": "Confort por habitación",
        "descripcion": (
            "Porcentaje de lecturas en zona de confort por sala "
            "(temperatura 20-24 °C, humedad 40-60 %, CO₂ < 1000 ppm). "
            "Permite identificar qué habitación está mejor acondicionada."
        ),
        "zona_confort": {"temp_c": "20-24", "humedad_pct": "40-60", "co2_max_ppm": 1000},
        "por_sala": comfort_data,
        "sala_mas_confortable": mejor_sala,
        "sala_menos_confortable": peor_sala,
        "recomendacion": (
            f"La sala más confortable es '{mejor_sala}'. "
            f"'{peor_sala}' necesita más atención en climatización o ventilación."
        ),
    }

    # ── E: Ritmo circadiano del CO₂ ──────────────────────────────────────────
    # Agrupa por franja horaria para revelar rutinas del hogar
    # Madrugada 0-6h, Mañana 7-12h, Tarde 13-19h, Noche 20-23h
    logging.info("[E] Calculando ritmo circadiano del CO₂...")
    circadian_rows = hook.get_records(f"""
        SELECT
            franja,
            ROUND(AVG(avg_indoor_air_quality)::NUMERIC, 0)  AS co2_medio_ppm,
            ROUND(MAX(avg_indoor_air_quality)::NUMERIC, 0)  AS co2_max_ppm,
            ROUND(AVG(avg_indoor_temperature)::NUMERIC, 1)  AS temp_media_c,
            COUNT(*)                                         AS lecturas
        FROM (
            SELECT *,
                CASE
                    WHEN EXTRACT(HOUR FROM timestamp) BETWEEN  0 AND  6 THEN '1_madrugada (00-06h)'
                    WHEN EXTRACT(HOUR FROM timestamp) BETWEEN  7 AND 12 THEN '2_manana    (07-12h)'
                    WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 13 AND 19 THEN '3_tarde     (13-19h)'
                    ELSE                                                      '4_noche     (20-23h)'
                END AS franja
            FROM {PG_TABLE}
        ) t
        GROUP BY franja
        ORDER BY franja
    """)
    circ_cols   = ["franja","co2_medio_ppm","co2_max_ppm","temp_media_c","lecturas"]
    circ_data   = _rows_to_dicts(circadian_rows, circ_cols)
    franja_peor = max(circ_data, key=lambda x: x["co2_medio_ppm"])["franja"] if circ_data else "N/A"

    analysis_e = {
        "titulo": "Ritmo circadiano del CO₂ — ¿cuándo respira la casa?",
        "descripcion": (
            "Nivel medio de CO₂ interior agrupado por franja horaria. "
            "Revela las rutinas del hogar: cocina, sueño, trabajo desde casa. "
            "Un CO₂ elevado de madrugada indica ventilación nocturna insuficiente; "
            "picos en la tarde suelen coincidir con cocinar o mayor ocupación."
        ),
        "por_franja": circ_data,
        "franja_mas_contaminada": franja_peor,
        "recomendacion": (
            f"La franja con peor calidad de aire es '{franja_peor}'. "
            "Considera ventilar activamente en ese periodo."
        ),
    }

    # ── F: Evolución mensual de temperatura ──────────────────────────────────
    # Temperatura media exterior e interior por mes + alertas de helada
    logging.info("[F] Calculando evolución mensual de temperatura...")
    monthly_rows = hook.get_records(f"""
        SELECT
            TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM')  AS mes,
            ROUND(AVG(temperature_exterieur)::NUMERIC, 1)       AS media_exterior_c,
            ROUND(MIN(temperature_exterieur)::NUMERIC, 1)       AS min_exterior_c,
            ROUND(MAX(temperature_exterieur)::NUMERIC, 1)       AS max_exterior_c,
            ROUND(AVG(avg_indoor_temperature)::NUMERIC, 1)      AS media_interior_c,
            COUNT(*) FILTER (WHERE temperature_exterieur < 3)   AS lecturas_riesgo_helada,
            COUNT(*)                                             AS total_lecturas
        FROM {PG_TABLE}
        GROUP BY DATE_TRUNC('month', timestamp)
        ORDER BY DATE_TRUNC('month', timestamp)
    """)
    month_cols  = ["mes","media_exterior_c","min_exterior_c","max_exterior_c",
                   "media_interior_c","lecturas_riesgo_helada","total_lecturas"]
    monthly_data = _rows_to_dicts(monthly_rows, month_cols)
    mes_mas_frio = min(monthly_data, key=lambda x: x["media_exterior_c"])["mes"] if monthly_data else "N/A"
    mes_mas_cal  = max(monthly_data, key=lambda x: x["media_exterior_c"])["mes"] if monthly_data else "N/A"
    total_helada = sum(int(m["lecturas_riesgo_helada"] or 0) for m in monthly_data)

    analysis_f = {
        "titulo": "Evolución mensual de temperatura y riesgo de helada",
        "descripcion": (
            "Temperatura exterior e interior media por mes. "
            "Incluye contador de lecturas con riesgo de helada (exterior < 3 °C), "
            "útil para proteger tuberías y plantas."
        ),
        "por_mes": monthly_data,
        "mes_mas_frio": mes_mas_frio,
        "mes_mas_caluroso": mes_mas_cal,
        "total_lecturas_riesgo_helada": total_helada,
        "recomendacion": (
            f"El mes más frío registrado es {mes_mas_frio} "
            f"y el más cálido {mes_mas_cal}. "
            f"Se detectaron {total_helada} lecturas con riesgo de helada (exterior < 3 °C)."
            if monthly_data else
            "Aún no hay suficientes datos para calcular evolución mensual."
        ),
    }

    # ── Construir y guardar el reporte ────────────────────────────────────────
    report = {
        "metadata": {
            "pipeline":       "Entregable 3 — Home Sensor Pipeline",
            "dag_run_id":     context["run_id"],
            "execution_date": exec_dt.isoformat(),
            "generado_en":    datetime.now().isoformat(),
            "tabla_fuente":   PG_TABLE,
            "total_registros_bd": summary.get("total_registros"),
        },
        "analisis": {
            "A_ineficiencia_termica":    analysis_a,
            "B_calidad_aire_critica":    analysis_b,
            "C_resumen_estadistico":     analysis_c,
            "D_confort_por_habitacion":  analysis_d,
            "E_ritmo_circadiano_co2":    analysis_e,
            "F_evolucion_mensual_temp":  analysis_f,
        },
    }

    os.makedirs(LOGS_DIR, exist_ok=True)
    report_path = os.path.join(LOGS_DIR, f"reporte_{exec_dt.strftime('%Y%m%d_%H%M%S')}.log")
    sep = "=" * 70

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"{sep}\n REPORTE ANALÍTICO — HOME SENSOR PIPELINE\n")
        f.write(f" Generado : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f" Registros: {summary.get('total_registros')}  |  "
                f"Periodo: {summary.get('primer_registro')} → {summary.get('ultimo_registro')}\n")
        f.write(f"{sep}\n\n")
        f.write(json.dumps(report, indent=2, ensure_ascii=False, default=str))
        f.write(f"\n\n{sep}\n FIN DEL REPORTE\n{sep}\n")

    # ── Funnel + resumen en log ───────────────────────────────────────────────
    batch_size      = ti.xcom_pull(task_ids="01_ingest_csv_batch",    key="batch_size")
    batch_offset    = ti.xcom_pull(task_ids="01_ingest_csv_batch",    key="batch_offset")
    n_hdfs_read     = ti.xcom_pull(task_ids="05_transform_normalize", key="n_hdfs_read")
    n_dropped_ts    = ti.xcom_pull(task_ids="05_transform_normalize", key="n_dropped_ts")
    n_dropped_wm    = ti.xcom_pull(task_ids="05_transform_normalize", key="n_dropped_wm")
    n_dropped_dedup = ti.xcom_pull(task_ids="05_transform_normalize", key="n_dropped_dedup")
    row_count_t05   = ti.xcom_pull(task_ids="05_transform_normalize", key="row_count")
    rows_loaded     = ti.xcom_pull(task_ids="06b_load_to_postgres",   key="rows_loaded")

    logging.info(SEP)
    logging.info("  FUNNEL COMPLETO DEL PIPELINE")
    logging.info(f"  [T01] Lote extraído del CSV         : {batch_size} filas  (offset={batch_offset})")
    logging.info(f"  [T02] Mensajes enviados a Kafka      : {batch_size}")
    logging.info(f"  [T05] Leídos de HDFS (acumulado)    : {n_hdfs_read}")
    logging.info(f"  [T05]   - Timestamp nulo            : -{n_dropped_ts}")
    logging.info(f"  [T05]   - Ya en PG (watermark)      : -{n_dropped_wm}")
    logging.info(f"  [T05]   - Duplicados (timestamp)    : -{n_dropped_dedup}")
    logging.info(f"  [T05]   = Registros nuevos           : {row_count_t05}")
    logging.info(f"  [T06b] Cargados en PostgreSQL        : {rows_loaded}  (upsert)")
    logging.info(f"  [T07]  Total acumulado en BD         : {summary.get('total_registros')}")
    logging.info(SEP)
    logging.info("  RESULTADOS ANALÍTICOS")
    logging.info(f"  [A] Ineficiencia térmica            : {count_a} episodios ({analysis_a['severidad']})")
    logging.info(f"  [B] CO₂ crítico (>1500 ppm)         : {count_b} episodios ({analysis_b['severidad']})")
    logging.info(f"  [D] Sala más confortable            : {analysis_d['sala_mas_confortable']}")
    logging.info(f"  [D] Sala menos confortable          : {analysis_d['sala_menos_confortable']}")
    logging.info(f"  [E] Franja con más CO₂              : {analysis_e['franja_mas_contaminada']}")
    logging.info(f"  [F] Mes más frío / más cálido       : {mes_mas_frio} / {mes_mas_cal}")
    logging.info(f"  [F] Lecturas con riesgo de helada   : {total_helada}")
    logging.info(f"  Reporte guardado en : {report_path}")
    logging.info(SEP)

    return report_path
