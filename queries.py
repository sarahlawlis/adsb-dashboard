import pandas as pd
import plotly.graph_objects as go

from db import get_connection


def get_current_summary():
    query = """
        with latest_snapshot as (
            select snapshot_id
            from adsb.snapshot
            order by source_timestamp_utc desc
            limit 1
        )
        select
            count(*) as aircraft_now,
            avg(ao.alt_baro_ft) as avg_altitude_ft,
            avg(ao.gs) as avg_ground_speed
        from adsb.aircraft_observation ao
        join latest_snapshot ls
            on ao.snapshot_id = ls.snapshot_id
        where ao.seen is not null
          and ao.seen <= 60;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()

    return {
        "aircraft_now": row[0],
        "avg_altitude_ft": round(row[1], 1) if row[1] is not None else None,
        "avg_ground_speed": round(row[2], 1) if row[2] is not None else None,
    }


def get_live_aircraft_positions():
    query = """
        with latest_snapshot as (
            select snapshot_id
            from adsb.snapshot
            order by source_timestamp_utc desc
            limit 1
        )
        select
            ao.hex,
            ao.flight,
            ao.lat,
            ao.lon,
            ao.alt_baro_ft,
            ao.gs,
            ao.track,
            ao.seen,
            am.manufacturer_name,
            am.model
        from adsb.aircraft_observation ao
        join latest_snapshot ls
            on ao.snapshot_id = ls.snapshot_id
        left join adsb.aircraft_metadata am
            on lower(ao.hex) = lower(am.hex)
        where ao.seen is not null
          and ao.seen <= 60
          and ao.lat is not null
          and ao.lon is not null
        order by ao.alt_baro_ft desc nulls last;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    aircraft = []

    for row in rows:
        aircraft.append({
            "hex": row[0],
            "flight": row[1].strip() if row[1] else None,
            "lat": float(row[2]),
            "lon": float(row[3]),
            "alt_baro_ft": round(row[4], 1) if row[4] is not None else None,
            "gs": round(row[5], 1) if row[5] is not None else None,
            "track": round(row[6], 1) if row[6] is not None else None,
            "seen": round(row[7], 1) if row[7] is not None else None,
            "manufacturer_name": row[8],
            "model": row[9],
        })

    return aircraft


def get_collection_chart_html():
    query = """
        with hourly as (
            select
                date_trunc('hour', source_timestamp_utc) as hour_bucket,
                sum(aircraft_count) as aircraft_reports
            from adsb.snapshot
            where source_timestamp_utc >= now() - interval '7 days'
            group by 1
        ),

        last_24 as (
            select *
            from hourly
            where hour_bucket >= now() - interval '24 hours'
        ),

        seven_day_avg as (
            select
                extract(hour from hour_bucket)::int as hour_of_day,
                avg(aircraft_reports) as avg_reports
            from hourly
            group by 1
        )

        select
            l.hour_bucket,
            l.aircraft_reports as last_24_reports,
            s.avg_reports
        from last_24 l
        left join seven_day_avg s
            on extract(hour from l.hour_bucket) = s.hour_of_day
        order by l.hour_bucket;
    """

    with get_connection() as conn:
        df = pd.read_sql_query(query, conn)

    if df.empty:
        return None

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["hour_bucket"],
        y=df["last_24_reports"],
        mode="lines+markers",
        name="Last 24 Hours",
        line=dict(width=3),
        marker=dict(size=6)
    ))

    fig.add_trace(go.Scatter(
        x=df["hour_bucket"],
        y=df["avg_reports"],
        mode="lines",
        name="7 Day Average",
        line=dict(width=2, dash="dash")
    ))

    fig.update_layout(
        height=420,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.03)",
        font=dict(color="#f4f7fb"),
        margin=dict(l=50, r=30, t=30, b=50),
        xaxis=dict(
            title="Time (Last 24 Hours)",
            gridcolor="rgba(255,255,255,0.08)",
        ),
        yaxis=dict(
            title="Aircraft Reports",
            gridcolor="rgba(255,255,255,0.08)",
        )
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)


def get_hourly_heatmap_html():
    query = """
        select
            date(source_timestamp_utc) as report_date,
            extract(hour from source_timestamp_utc)::int as hour_of_day,
            sum(aircraft_count) as aircraft_reports
        from adsb.snapshot
        where source_timestamp_utc >= now() - interval '7 days'
        group by 1, 2
        order by 1, 2;
    """

    with get_connection() as conn:
        df = pd.read_sql_query(query, conn)

    if df.empty:
        return None

    pivot = df.pivot(
        index="report_date",
        columns="hour_of_day",
        values="aircraft_reports"
    ).fillna(0)

    for hour in range(24):
        if hour not in pivot.columns:
            pivot[hour] = 0

    pivot = pivot[sorted(pivot.columns)]

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=[f"{h}:00" for h in pivot.columns],
        y=[str(d) for d in pivot.index],
        colorscale=[
            [0.0, "#bae6fd"],
            [0.25, "#38bdf8"],
            [0.50, "#2563eb"],
            [0.75, "#1e3a8a"],
            [1.0, "#020617"]
        ],
        colorbar=dict(title="Reports")
    ))

    fig.update_layout(
        height=360,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.03)",
        font=dict(color="#f4f7fb"),
        margin=dict(l=70, r=30, t=30, b=50),
        xaxis=dict(
            title="Hour of Day",
            gridcolor="rgba(255,255,255,0.08)"
        ),
        yaxis=dict(
            title="Date",
            gridcolor="rgba(255,255,255,0.08)"
        )
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)