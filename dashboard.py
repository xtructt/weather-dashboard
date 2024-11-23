import streamlit as st
import polars as pl
import plotly.graph_objects as go
from datetime import datetime


# Load the data
def load_data():
    location = pl.read_csv("./output/location.csv")
    merged = pl.read_csv("./output/merged.csv")
    stats = pl.read_csv("./output/stats.csv")

    return location, merged, stats


def create_range_chart(df: pl.DataFrame, city):
    # Filter data for the selected city
    fc_data = (
        df.filter(pl.col("name") == city)
        .select(
            [
                "forecast_date",
                "created_date_local",
                "forecast_mintemp_c",
                "forecast_maxtemp_c",
                "forecast_avgtemp_c",
            ]
        )
        .filter(pl.col("created_date_local") == pl.col("created_date_local").max())
    )

    # Convert dates to just show the day
    dates = [d.split()[0] for d in fc_data.get_column("forecast_date").to_list()]
    min_temps = fc_data.get_column("forecast_mintemp_c").to_list()
    max_temps = fc_data.get_column("forecast_maxtemp_c").to_list()
    avg_temps = fc_data.get_column("forecast_avgtemp_c").to_list()

    fig = go.Figure()

    # Add range bars for min-max temperature
    fig.add_trace(
        go.Bar(
            name="Temperature Range",
            x=dates,
            y=[max_t - min_t for max_t, min_t in zip(max_temps, min_temps)],
            base=min_temps,
            marker_color="#22a7f0",
            hovertemplate="Date: %{x}<br>"
            + "Max: %{base:.1f}°C<br>"
            + "Min: %{customdata:.1f}°C<br><extra></extra>",
            customdata=max_temps,
        )
    )

    # Add line for average temperature
    fig.add_trace(
        go.Scatter(
            name="Average Temperature",
            x=dates,
            y=avg_temps,
            mode="lines+markers",
            line=dict(color="#de6e56", width=2),
            marker=dict(size=8),
            hovertemplate="Date: %{x}<br>" + "Avg: %{y:.1f}°C<br><extra></extra>",
        )
    )

    # Update layout with cleaner x-axis
    fig.update_layout(
        title=f"Temperature Forecast for {city}",
        xaxis_title="Date",
        yaxis_title="Temperature (°C)",
        showlegend=True,
        hovermode="x unified",
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        xaxis=dict(tickangle=45, tickmode="array", ticktext=dates, tickvals=dates),
    )

    return fig


def main():
    st.set_page_config(page_title="Weather Dashboard", layout="wide")
    location, merged, stats = load_data()
    loc_name = location.select(pl.col("name")).to_dict()["name"]
    st.title("_WEATHER DASHBOARD_ :thermometer: :mostly_sunny: :rain_cloud:")
    st.header("Current Temperature Overview")
    current_forecast_compare = merged.select(
        pl.col(["name", "current_temp_c", "forecast_current_temp_diff"]).filter(
            pl.col("day_diff") == 0
        )
    )
    city_nums = len(current_forecast_compare)
    cols = st.columns(city_nums)
    i = 0
    for row in current_forecast_compare.iter_rows(named=True):
        with cols[i]:
            st.metric(
                f"{row['name']}",
                f"{row['current_temp_c']}°C",
                f"{row['forecast_current_temp_diff']}°C vs forecast",
                delta_color="inverse",
            )
        i += 1
    st.markdown("<br>", unsafe_allow_html=True)
    st.header("Key Insights")
    col1, col2 = st.columns(2)
    with col1:
        hottest_city = merged.select(pl.col(["name", "current_temp_c"])).filter(
            pl.col("current_temp_c") == pl.col("current_temp_c").max()
        )
        st.info(
            f"🌡️ Currently hottest city is {hottest_city.item(0, 'name')} at {hottest_city.item(0, 'current_temp_c')}°C"
        )
        highest_forecast = merged.select(pl.col(["name", "forecast_maxtemp_c"])).filter(
            pl.col("forecast_maxtemp_c") == pl.col("forecast_maxtemp_c").max()
        )
        st.info(
            f"📈 Highest forecasted temperature is {highest_forecast.item(0, 'forecast_maxtemp_c')}°C in {highest_forecast.item(0, 'name')}"
        )
    with col2:
        coldest_city = merged.select(pl.col(["name", "current_temp_c"])).filter(
            pl.col("current_temp_c") == pl.col("current_temp_c").min()
        )
        st.info(
            f"❄️ Currently coolest city is {coldest_city.item(0, 'name')} at {coldest_city.item(0, 'current_temp_c')}°C"
        )
        lowest_forecast = merged.select(pl.col(["name", "forecast_mintemp_c"])).filter(
            pl.col("forecast_mintemp_c") == pl.col("forecast_mintemp_c").min()
        )
        st.info(
            f"📉 Lowest forecasted temperature is {lowest_forecast.item(0, 'forecast_mintemp_c')}°C in {lowest_forecast.item(0, 'name')}"
        )
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Forecast by city")
    col1, _ = st.columns(2)
    with col1:
        option = st.selectbox("Choose city to show forecast", loc_name, index=0)
    col1, col2 = st.columns(2)
    st.markdown("<br>", unsafe_allow_html=True)
    with col1:
        st.subheader("Forecast Temperature Trends")
        # Replace the original line chart with our new range chart
        fig = create_range_chart(merged, option)
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.subheader("Detail")
        stats_detail = stats.filter(pl.col("name") == option)
        additional_data = (
            merged.filter(pl.col("name") == option)
            .filter(pl.col("created_date_local") == pl.col("created_date_local").max())
            .group_by(["name", "region", "country"])
            .agg(pl.col("uv").mean(), pl.col("forecast_maxwind_kph").mean())
        )
        st.info(
            f"🔥 The hottest day is {stats_detail.item(0, 7)} at {stats_detail.item(0, 10)}°C"
        )
        st.info(
            f"🌡️ The average temperature based on the forecast is {stats_detail.item(0, 6)}°C"
        )
        st.info(
            f"🌤️ The uv index based on the forecast is {additional_data.item(0, 3):.2f}"
        )
        st.info(f"💨 Wind is expected at around {additional_data.item(0, 4):.2f} kph")
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Forecast details")
    st.dataframe(
        stats.drop("avg_temp").to_pandas(),
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
