import streamlit as st
import polars as pl


# Load the data
def load_data():
    location = pl.read_csv("./output/location.csv")
    merged = pl.read_csv("./output/merged.csv")
    stats = pl.read_csv("./output/stats.csv")

    return location, merged, stats


def main():
    st.set_page_config(page_title="Weather Dashboard", layout="wide")
    location, merged, stats = load_data()
    loc_name = location.select(pl.col("name")).to_dict()["name"]
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
    st.subheader("Forecast by city")
    option = st.selectbox("Choose city to show forecast", loc_name, index=0)
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Forecast Temperature Trends")
        fc_trends = merged.select(
            pl.col(
                [
                    "name",
                    "forecast_avgtemp_c",
                    "forecast_maxtemp_c",
                    "forecast_mintemp_c",
                    "forecast_date",
                ]
            )
        ).filter(pl.col("name") == option)
        fc_trends = fc_trends.unpivot(index=["name", "forecast_date"])
        st.line_chart(fc_trends, x="forecast_date", y="value", color="variable")
    with col2:
        st.subheader("Detail")
        stats_detail = stats.filter(pl.col("name") == option)
        additional_data = (
            merged.filter(pl.col("name") == option)
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
    st.subheader("Forecast details")
    st.dataframe(stats.drop("avg_temp").to_pandas())
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


if __name__ == "__main__":
    main()
