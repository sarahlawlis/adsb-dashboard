import logging

from flask import Flask, render_template
from psycopg2 import Error as PsycopgError
from queries import (
    get_current_summary,
    get_live_aircraft_positions,
    get_collection_chart_html,
    get_hourly_heatmap_html,
)

app = Flask(__name__)

logger = logging.getLogger(__name__)


@app.route("/")
def dashboard():
    summary = {
        "aircraft_now": None,
        "avg_altitude_ft": None,
        "avg_ground_speed": None,
    }

    aircraft_positions = []
    collection_chart_html = None
    heatmap_html = None
    error_message = None

    try:
        summary = get_current_summary()
        aircraft_positions = get_live_aircraft_positions()
        collection_chart_html = get_collection_chart_html()
        heatmap_html = get_hourly_heatmap_html()

    except PsycopgError:
        logger.exception("Failed to load dashboard data from PostgreSQL")
        error_message = (
            "The dashboard could not load live data from PostgreSQL. "
            "Check the database connection settings and try again."
        )

    return render_template(
        "dashboard.html",
        summary=summary,
        aircraft_positions=aircraft_positions,
        collection_chart_html=collection_chart_html,
        heatmap_html=heatmap_html,
        error_message=error_message,
    )


if __name__ == "__main__":
    app.run()