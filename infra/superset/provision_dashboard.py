#!/usr/bin/env python3
"""
provision_dashboard.py
Programmatically provisions the GDELT Global Geopolitical Command Center
dashboard and all accompanying charts in Apache Superset 6.1.0.
"""

import json
import logging
from superset.app import create_app

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("provision_dashboard")

app = create_app()

with app.app_context():
    from superset import db, security_manager
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    from superset.connectors.sqla.models import SqlaTable, TableColumn

    # Fetch admin user
    admin_user = security_manager.find_user(username="admin")

    # 1. Ensure Presto Database Connection
    presto_db = db.session.query(Database).filter(
        (Database.database_name.in_(["Presto", "Presto (Lakehouse)"])) | 
        (Database.sqlalchemy_uri.like("%presto-coordinator%"))
    ).first()

    if not presto_db:
        logger.info("Registering Presto (Lakehouse) database connection...")
        presto_db = Database(
            database_name="Presto (Lakehouse)",
            sqlalchemy_uri="presto://presto-coordinator:8080/iceberg",
            expose_in_sqllab=True,
            allow_run_async=True,
            allow_ctas=True,
            allow_cvas=True,
            allow_dml=False,
        )
        db.session.add(presto_db)
        db.session.commit()
        logger.info("Created Presto Database ID=%d", presto_db.id)
    else:
        logger.info("Using existing Presto Database ID=%d (%s)", presto_db.id, presto_db.database_name)

    # 2. Ensure Required Datasets
    def ensure_table(table_name: str, schema: str) -> SqlaTable:
        tbl = db.session.query(SqlaTable).filter_by(
            database_id=presto_db.id,
            table_name=table_name,
            schema=schema,
        ).first()
        if not tbl:
            logger.info("Registering dataset: %s.%s ...", schema, table_name)
            tbl = SqlaTable(
                table_name=table_name,
                schema=schema,
                database_id=presto_db.id,
            )
            db.session.add(tbl)
            db.session.commit()
            try:
                tbl.fetch_metadata()
            except Exception as e:
                logger.warning("Could not auto-fetch metadata for %s.%s: %s", schema, table_name, e)
        return tbl

    tbl_unified = ensure_table("vw_gdelt_events_unified", "gdelt_serving")
    tbl_stability = ensure_table("country_stability_daily", "gold_gdelt")
    tbl_matrix = ensure_table("global_conflict_matrix", "gold_gdelt")

    # Helper: Ensure calculated column exists on a dataset
    def ensure_calculated_column(tbl, col_name: str, expr: str, col_type: str, groupby: bool = True, verbose_name: str = ""):
        col = db.session.query(TableColumn).filter_by(table_id=tbl.id, column_name=col_name).first()
        if not col:
            col = TableColumn(
                table_id=tbl.id,
                column_name=col_name,
                expression=expr,
                type=col_type,
                groupby=groupby,
                filterable=True,
                is_dttm=False,
                verbose_name=verbose_name or col_name,
            )
            db.session.add(col)
            db.session.commit()

    ensure_calculated_column(
        tbl=tbl_unified,
        col_name="event_category",
        expr="""CASE 
  WHEN quad_class = 1 THEN 'Verbal Cooperation (Diplomacy)'
  WHEN quad_class = 2 THEN 'Material Cooperation (Aid/Accords)'
  WHEN quad_class = 3 THEN 'Verbal Conflict (Threats/Sanctions)'
  WHEN quad_class = 4 THEN 'Material Conflict (Violence/War)'
  ELSE 'Unclassified'
END""",
        col_type="VARCHAR",
        groupby=True,
        verbose_name="Geopolitical Event Category",
    )

    ensure_calculated_column(
        tbl=tbl_unified,
        col_name="sentiment_tone",
        expr="ROUND(avg_tone, 2)",
        col_type="DOUBLE",
        groupby=False,
        verbose_name="Sentiment Tone",
    )

    logger.info("Verified tables: Unified=%d, Stability=%d, Matrix=%d",
                tbl_unified.id, tbl_stability.id, tbl_matrix.id)

    # Clean existing dashboard & obsolete slices to ensure fresh state
    existing_dash = db.session.query(Dashboard).filter_by(slug="gdelt-geopolitical-command-center").first()
    if existing_dash:
        logger.info("Cleaning previous dashboard instance...")
        db.session.delete(existing_dash)
        db.session.commit()

    obsolete_slices = db.session.query(Slice).filter(
        Slice.slice_name.in_([
            "Lakehouse Storage Tier Distribution",
            "Global Conflict Intensity by Country (% Conflict)",
            "Global Geopolitical Incident Density by Country",
        ])
    ).all()
    for obs in obsolete_slices:
        logger.info("Purging obsolete idle slice: %s", obs.slice_name)
        db.session.delete(obs)
    db.session.commit()

    # Helpers for concise chart definitions
    def sql_metric(expr: str, label: str) -> dict:
        return {"expressionType": "SQL", "sqlExpression": expr, "label": label}

    def make_kpi(slice_name: str, expr: str, label: str, subheader: str) -> dict:
        return {
            "slice_name": slice_name,
            "viz_type": "big_number_total",
            "datasource_id": tbl_unified.id,
            "params": {
                "datasource": f"{tbl_unified.id}__table",
                "viz_type": "big_number_total",
                "metric": sql_metric(expr, label),
                "subheader": subheader,
                "header_font_size": 0.35,
                "subheader_font_size": 0.15,
                "time_range": "No filter",
            },
        }

    # Metric definitions for reuse
    map_metric = sql_metric("ROUND(SUM(conflict_events) * 100.0 / NULLIF(SUM(total_events), 0), 1)", "Conflict Rate (%)")
    citations_metric = sql_metric("SUM(total_mentions)", "Total Media Citations")
    conflicts_metric = sql_metric("SUM(conflict_count)", "Conflicts")

    # Slice definitions ordered logically matching dashboard grid layout
    slice_definitions = [
        # Tile 0: KPI 1 - Total Events
        make_kpi(
            "Total Global Events Tracked",
            "COUNT(*)",
            "Total Events",
            "Unified Hot (MongoDB) & Cold (Iceberg) Tiers",
        ),
        # Tile 1: KPI 2 - Active Crises
        make_kpi(
            "Active Breaking Crises (QuadClass 4)",
            "COUNT(CASE WHEN quad_class = 4 THEN 1 END)",
            "Active Crises",
            "Severe Material Conflict & Escalations",
        ),
        # Tile 2: KPI 3 - Media Attention
        make_kpi(
            "Global Media Attention Index",
            "SUM(num_mentions)",
            "Total Mentions",
            "Cumulative Citations Across News Agencies",
        ),
        # Tile 3: KPI 4 - Mean Stability
        make_kpi(
            "Mean Goldstein Stability Score",
            "ROUND(AVG(goldstein_scale), 2)",
            "Mean Stability",
            "Scale: -10 (Total Conflict) to +10 (Peace)",
        ),
        # Tile 4: World Map - Conflict Heatmap
        {
            "slice_name": "Global Conflict Intensity by Country (Darker = Higher % Conflict)",
            "viz_type": "world_map",
            "datasource_id": tbl_stability.id,
            "params": {
                "datasource": f"{tbl_stability.id}__table",
                "viz_type": "world_map",
                "entity": "action_country",
                "groupby": ["action_country"],
                "metric": map_metric,
                "secondary_metric": sql_metric("SUM(total_events)", "Total Incidents"),
                "orderby": [[map_metric, False]],
                "country_fieldtype": "cca3",
                "color_scheme": "supersetColors",
                "row_limit": 250,
                "time_range": "No filter",
            },
        },
        # Tile 5: Donut - Conflict vs Diplomacy
        {
            "slice_name": "Global Conflict vs Diplomacy Breakdown (QuadClass)",
            "viz_type": "pie",
            "datasource_id": tbl_unified.id,
            "params": {
                "datasource": f"{tbl_unified.id}__table",
                "viz_type": "pie",
                "groupby": ["event_category"],
                "metric": sql_metric("COUNT(*)", "Event Count"),
                "donut": True,
                "show_labels": True,
                "show_legend": True,
                "labels_outside": True,
                "color_scheme": "supersetColors",
                "time_range": "No filter",
            },
        },
        # Tile 6: Line - 30-Day Trend
        {
            "slice_name": "30-Day Geopolitical Trajectory: Conflict vs Cooperation",
            "viz_type": "echarts_timeseries_line",
            "datasource_id": tbl_stability.id,
            "params": {
                "datasource": f"{tbl_stability.id}__table",
                "viz_type": "echarts_timeseries_line",
                "granularity_sqla": "event_date",
                "time_grain_sqla": "P1D",
                "time_range": "No filter",
                "metrics": [
                    sql_metric("SUM(conflict_events)", "Conflict Events"),
                    sql_metric("SUM(cooperation_events)", "Cooperation Events"),
                ],
                "groupby": [],
                "rich_tooltip": True,
                "show_legend": True,
                "series_type": "line",
                "color_scheme": "supersetColors",
            },
        },
        # Tile 7: Bar - Top 10 Hotspots
        {
            "slice_name": "Top 10 Geopolitical Hotspots by Media Attention",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": tbl_stability.id,
            "params": {
                "datasource": f"{tbl_stability.id}__table",
                "viz_type": "echarts_timeseries_bar",
                "x_axis": "action_country",
                "time_range": "No filter",
                "metrics": [citations_metric],
                "groupby": [],
                "orderby": [[citations_metric, False]],
                "row_limit": 10,
                "show_legend": False,
                "rich_tooltip": True,
                "color_scheme": "supersetColors",
            },
        },
        # Tile 8: Table - Bilateral Dyads
        {
            "slice_name": "Bilateral Conflict Matrix (Top Antagonist Pairs)",
            "viz_type": "table",
            "datasource_id": tbl_matrix.id,
            "params": {
                "datasource": f"{tbl_matrix.id}__table",
                "viz_type": "table",
                "query_mode": "aggregate",
                "groupby": ["actor1_country", "actor2_country"],
                "metrics": [
                    conflicts_metric,
                    sql_metric("SUM(cooperation_count)", "Cooperations"),
                    sql_metric("ROUND(AVG(avg_goldstein_score), 2)", "Mean Goldstein"),
                    sql_metric("SUM(total_media_coverage)", "Media Mentions"),
                ],
                "orderby": [[conflicts_metric, False]],
                "server_page_length": 10,
                "include_search": True,
                "time_range": "No filter",
            },
        },
        # Tile 9: Table - Breaking Incident Ticker
        {
            "slice_name": "Breaking Incident Feed & Real-Time Crisis Ticker",
            "viz_type": "table",
            "datasource_id": tbl_unified.id,
            "params": {
                "datasource": f"{tbl_unified.id}__table",
                "viz_type": "table",
                "query_mode": "raw",
                "all_columns": [
                    "event_time",
                    "action_country",
                    "actor1_name",
                    "actor2_name",
                    "event_category",
                    "goldstein_scale",
                    "num_mentions",
                    "sentiment_tone",
                    "source_url",
                ],
                "order_by_cols": ['["event_time", false]'],
                "server_page_length": 15,
                "row_limit": 100,
                "include_search": True,
                "time_range": "No filter",
            },
        },
    ]

    # Save slices
    created_slices = []
    for s_def in slice_definitions:
        existing = db.session.query(Slice).filter_by(slice_name=s_def["slice_name"]).first()
        if existing:
            db.session.delete(existing)
            db.session.flush()

        slc = Slice(
            slice_name=s_def["slice_name"],
            viz_type=s_def["viz_type"],
            datasource_id=s_def["datasource_id"],
            datasource_type="table",
            params=json.dumps(s_def["params"], indent=2),
            owners=[admin_user] if admin_user else [],
        )
        db.session.add(slc)
        db.session.flush()
        created_slices.append(slc)
        logger.info("Configured Slice %d: %s", slc.id, slc.slice_name)

    chart_ids = [f"CHART_{s.id}" for s in created_slices]

    # Build responsive dashboard layout grid
    position_layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "parents": ["ROOT_ID"],
            "children": ["ROW-KPIS", "ROW-MAP", "ROW-TRENDS", "ROW-TABLES"],
        },
    }

    # Row layouts: (row_id, [(slice_index, width, height)])
    row_configs = [
        ("ROW-KPIS", [(0, 3, 26), (1, 3, 26), (2, 3, 26), (3, 3, 26)]),
        ("ROW-MAP", [(4, 7, 55), (5, 5, 55)]),
        ("ROW-TRENDS", [(6, 7, 50), (7, 5, 50)]),
        ("ROW-TABLES", [(8, 6, 55), (9, 6, 55)]),
    ]

    for row_id, charts in row_configs:
        position_layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": [chart_ids[i] for i, _, _ in charts],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for i, w, h in charts:
            position_layout[chart_ids[i]] = {
                "type": "CHART",
                "id": chart_ids[i],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "children": [],
                "meta": {
                    "chartId": created_slices[i].id,
                    "sliceName": created_slices[i].slice_name,
                    "width": w,
                    "height": h,
                },
            }

    dash = Dashboard(
        dashboard_title="GDELT Global Geopolitical Command Center",
        slug="gdelt-geopolitical-command-center",
        published=True,
        position_json=json.dumps(position_layout, indent=2),
        slices=created_slices,
        owners=[admin_user] if admin_user else [],
    )
    db.session.add(dash)
    db.session.commit()
    logger.info("Successfully provisioned Dashboard ID=%d, Slug=%s", dash.id, dash.slug)
