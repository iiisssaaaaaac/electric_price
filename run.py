import polars as pl
import requests
from io import StringIO
import json
import xml.etree.ElementTree as ET
import plotly.graph_objects as go


def penis():
    print("this is working")
    test = pl.DataFrame()
    print(test)
    

def get_ws_data(date_from, date_to, format="csv"):
    get_string = f"https://data.elexon.co.uk/bmrs/api/v1/balancing/pricing/market-index?from={date_from}Z&to={date_to}Z&format={format}"
    return requests.get(get_string)


def parse_csv_response(response):
    """Parse CSV response into Polars DataFrame"""
    df = pl.read_csv(StringIO(response.text))
    df.filter(pl.col("DataProvider") == "APXMIDP")
    starttimeSeries = pl.Series(df["StartTime"])
    starttimeSeries.str.to_datetime("%Y-%m-%dT%H:%M:%S%#z", ambiguous='raise')
    print(starttimeSeries)
    df.drop("StartTime")
    df.insert_column("StartTime", starttimeSeries)
    return df


def parse_json_response(response):
    """Parse JSON response into Polars DataFrame"""
    data = response.json()
    # If the JSON is a list of dicts, convert directly
    if isinstance(data, list):
        return pl.DataFrame(data)
    # If the data is nested, you might need to extract the relevant key
    # Adjust based on your API's actual structure
    return pl.DataFrame(data)


def parse_xml_response(response):
    """Parse XML response into Polars DataFrame"""
    root = ET.fromstring(response.content)
    
    # Extract data from XML (adjust based on your API's structure)
    data = []
    for item in root.findall('.//record'):  # Adjust the tag name as needed
        row = {child.tag: child.text for child in item}
        data.append(row)
    
    return pl.DataFrame(data)


def plot_timeseries(df, x_col, y_col, title="Timeseries Data"):
    """Plot timeseries data using Plotly"""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df[x_col],
        y=df[y_col],
        mode='lines+markers',
        name=y_col
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title=x_col,
        yaxis_title=y_col,
        hovermode='x unified',
        template='plotly_white'
    )
    
    fig.show()


if __name__ == "__main__":
    print("This script is being run directly.")
    
    # Example: CSV format
    csv_data = get_ws_data("2026-01-10T00%3A00", "2026-01-11T00%3A00", format="csv")
    df_csv = parse_csv_response(csv_data)
    print("CSV Data:")
    print(df_csv)
    
    # Plot the data - adjust column names based on your actual data
    # Common column names might be: 'timestamp', 'price', 'value', etc.
    # plot_timeseries(df_csv, x_col='SettlementDate', y_col='Price', 
    #                 title="Market Index Price Data")

