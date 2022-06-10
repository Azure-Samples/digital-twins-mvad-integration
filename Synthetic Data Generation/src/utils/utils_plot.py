"""Function to plot multivariate time-series as line graphs and anomalies as bar area plots"""
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def plot_ts_anom(df, df_anom, plot_anom=True):
    """Outputs multivariate Plot, optionally with anomaly as area-plots
    df_plot:df with "timestamp" and time-series as columns
    series_cols: list of columns in df_plot to plot
    plot_cp, boolean: whether to include identified change-points as area-plots
    bkpts, list[int]: list of change-points, i.e. breakpoints to plot as alternating area shadings
    title, str: title for the plot
    """

    # Make df long to wide
    df["ts_name"] = df["Id"] + "-" + df["Key"]
    dict_ts = {}
    for ts in df["ts_name"].unique():
        dict_ts[ts] = (
            df[(df["ts_name"] == ts)][["Timestamp", "Value"]]
            .reset_index(drop=True)
            .sort_values(by="Timestamp")
        )

    # TS plots
    subfig = make_subplots(specs=[[{"secondary_y": True}]])
    for ind, df_ts1 in dict_ts.items():
        subfig.add_trace(go.Scatter(x=df_ts1["Timestamp"], y=df_ts1["Value"], name=ind))

    if plot_anom:
        # Create ts for the change-points
        max_val = df["Value"].max()
        df_anom["Value"] = df_anom["isAnomaly"].apply(
            lambda x: max_val if x == 1 else 0
        )
        subfig.add_trace(
            go.Scatter(
                x=df_anom.index,
                y=df_anom["Value"],
                name="anomaly",
                fill="tonexty",
                fillcolor="rgba(250, 10, 10, 0.5)",
                line_shape="hv",
                line_color="rgba(0,0,0,0)",
                showlegend=True,
            ),
            row=1,
            col=1,
            secondary_y=True,
        )
        subfig.update_layout(
            yaxis2_range=[-0, 1],
            yaxis2_showgrid=False,
            yaxis2_tickfont_color="rgba(0,0,0,0)",
        )
        title = "Time-series with anom labels"
    else:
        title = "Time-series"

    subfig.update_layout(
        height=600,
        width=1200,
        title=title,
        legend=dict(font=dict(size=9), yanchor="top", y=-0.1, x=0.25),
    )
    subfig.layout.yaxis.title = "Telemetry Signal Values"
    subfig.show()
    return dict_ts
