import numpy as np
import requests
from dash import Dash, html, dcc, dash_table, Input, Output
import plotly.express as px
import pandas as pd

import queries
from queries import get_issues_summary, get_entries_table, get_issue_details
import boto3
from dotenv import load_dotenv
from sqlalchemy.engine import Engine, create_engine
import os
import dash_bootstrap_components as dbc
import dash_cytoscape as cyto
import aws
import h3
import json


load_dotenv()
px.set_mapbox_access_token(os.getenv("MAPBOX_TOKEN"))

denylist_engine = create_engine(os.getenv("DENYLIST_DB_CONNECTION_STRING"))

s3 = boto3.resource("s3")

app = Dash("Helium Denylist Reports", external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

PAGE_SIZE = 10


issues = get_issues_summary(denylist_engine, limit=None)
entries = [
    {
        "address": None,
        "issue_number": None,
        "reports_generated": None,
        "review_status": None,
        "name": None,
        "location": None,
        "owner": None,
        "payer": None,
        "maker": None,
        "long_country": None,
        "long_state": None,
        "long_city": None,
        "first_block": None,
        "other_mentioned_issues": None,
        "closed_pulls": None,
        "open_pulls": None
    }
]
accepted_entries = [
    {
        "address": None,
        "issue": None,
        "issue_type": None
    }
]

app.layout = html.Div(children=[
    html.H1(children="Helium Denylist Reports"),

    html.Div([
        html.H4("Issues"),
        dash_table.DataTable(
            id="issues-table",
            data=issues,
            columns=[{"id": i, "name": i} for i in issues[0].keys()],
            page_size=PAGE_SIZE,
            page_current=0,
            # filter_action="native",
            # sort_action="native",
            style_table={'overflowX': 'auto'},
        ),
        html.H4(id="issue-title", children="Select an issue to view details"),
        dbc.Badge(id="issue-type"),
        dbc.Accordion(
            [
                dbc.AccordionItem(
                    dcc.Markdown(id="issue-body", children=""), title="Issue Body"
                )
            ],
            start_collapsed=True
        ),
        dcc.Graph(id="entry-locations"),
        dash_table.DataTable(
            id="entries-table",
            data=entries,
            columns=[{"id": i, "name": i} for i in entries[0].keys()],
            row_selectable="multi",
            page_size=PAGE_SIZE,
            page_current=0,
            # filter_action="native",
            # sort_action="native",
            hidden_columns=["issue_number", "reports_generated", "review_status", "location", "payer"],
            style_table={'overflowX': 'auto'},
            include_headers_on_copy_paste=True
        ),
        html.Div([
            dbc.Button(children="Select All", id="select-all-button", n_clicks=0, color="primary"),
            dbc.Button("Add Selected to PR", id="add-selected-button", n_clicks=0, color="danger")
        ],
        className="d-grid gap-2 d-md-flex justify-content-md-end"),

        html.Div([
            html.H4(id="hotspot-name", children=""),
            html.P(id="hotspot-details", children=""),
            html.Div(id="explorer-links", children=[]),
            html.Div(
                [
                    dcc.Graph(id="distance-vs-rssi", style={"display": "inline-block"}),
                    dcc.Graph(id="witnessed-makers", style={"display": "inline-block"}),
                    dcc.Graph(id="rssi-vs-snr", style={"display": "inline-block"})
                ]
            ),
            cyto.Cytoscape(
                id="witness-graph",
                elements=[],
                layout={"name": "random"},
                stylesheet=[
                            # Class selectors
                            {
                                'selector': '.red',
                                'style': {
                                    'background-color': 'red',
                                    'line-color': 'red'
                                }
                            },
                            {
                                'selector': '.green',
                                'style': {
                                    'background-color': 'green',
                                    'line-color': 'green'
                                }
                            }
                        ]
            ),
        ], style={"padding_top": "4%"}),
        html.H4("Accepted Entries"),
        dash_table.DataTable(
            id="accepted-entries",
            data=[],
            columns=[{"id": i, "name": i} for i in accepted_entries[0].keys()],
            row_deletable=True,
            page_size=PAGE_SIZE,
            page_current=0,
            hidden_columns=[],
            style_table={'overflowX': 'auto'}
        ),
        html.Div([
            dbc.Button("Download Denylist with Updates", color="info", id="download-btn", className="d-grid gap-2 d-md-flex justify-content-md-end"),
            dbc.Button("Download Additions Only", color="success", id="download-additions-btn"),
            dbc.Button("Download Removals Only", color="danger", id="download-removals-btn"),
            dcc.Download(id="download-denylist"),
            dcc.Download(id="download-additions"),
            dcc.Download(id="download-additions-pr"),
            dcc.Download(id="download-removals"),
            dcc.Download(id="download-removals-pr"),
        ],
        className="d-grid gap-2 d-md-flex justify-content-md-start"),

        dcc.Textarea(id="pr-message"),
        dcc.Clipboard(target_id="pr-message", title="copy")
    ],
    style={"padding-left": "5%", "padding-right": "5%"}),
])


def r_squared(x, y):
    return round(np.corrcoef(x, y)[0,1]**2, 3)


def draw_witness_graph(witness_edges):
    unique_nodes = []
    for i, e in enumerate(witness_edges["transmitter_address"]):
        if e not in unique_nodes:
            unique_nodes.append({"address": e, "hop": witness_edges["hop"][i]})
    for i, e in enumerate(witness_edges["witness_address"]):
        if e not in unique_nodes:
            unique_nodes.append({"address": e, "hop": witness_edges["hop"][i]})

    elements = [
        {"data":
             {"id": a["address"]},
        "classes": "red" if a["hop"] == 1 else "green"
        } for a in unique_nodes
    ]
    edges = [
        {"data":
             {
                 "source": witness_edges["transmitter_address"][i],
                 "target": witness_edges["witness_address"][i]
             }
        } for i in range(len(witness_edges))
    ]
    return elements + edges



@app.callback(
    Output(component_id="entries-table", component_property="data"),
    Output(component_id="issue-title", component_property="children"),
    Output(component_id="issue-body", component_property="children"),
    Output(component_id="issue-type", component_property="children"),
    Output(component_id="issue-type", component_property="color"),
    Output(component_id="add-selected-button", component_property="children"),
    Output(component_id="add-selected-button", component_property="color"),
    Output(component_id="entry-locations", component_property="figure"),
    Input(component_id="issues-table", component_property="active_cell"),
    Input(component_id="issues-table", component_property="page_current"),
)
def update_output_div(selected_cell, page_current):
    issue_number = issues[selected_cell["row"] + page_current * PAGE_SIZE]["number"]
    issue_details = get_issue_details(denylist_engine, issue_number, with_body=True)
    entries_table = get_entries_table(denylist_engine, issue_number)
    if issue_details["issue_type"] == "addition":
        color = "primary"
        label = "Add Selected to PR"
    elif issue_details["issue_type"] == "removal":
        color = "danger"
        label = "Remove Selected from PR"
    else:
        color = "light"
        label = "Add Selected to PR"

    entry_df = pd.DataFrame(entries_table)
    entry_df["coords"] = entry_df["location"].apply(lambda x: h3.h3_to_geo(x))
    entry_df["lat"] = entry_df["coords"].apply(lambda x: x[0])
    entry_df["lon"] = entry_df["coords"].apply(lambda x: x[1])

    map_fig = px.scatter_mapbox(entry_df, lat="lat", lon="lon", color="owner", hover_name="name")
    map_fig.update_layout(transition_duration=500, showlegend=False)

    return entries_table, issue_details["title"], issue_details["body"], issue_details["issue_type"].capitalize(), color, label, color, map_fig


@app.callback(
    Output(component_id="entries-table", component_property="selected_rows"),
    Output(component_id="select-all-button", component_property="children"),
    Input(component_id="select-all-button", component_property="n_clicks"),
    Input(component_id="entries-table", component_property="data"),
)
def select_all_entries(n_clicks, entries):
    if n_clicks % 2 == 1:
        return [i for i in range(len(entries))], "Deselect All"
    else:
        return [], "Select All"


@app.callback(
    Output(component_id="distance-vs-rssi", component_property="figure"),
    Output(component_id="witnessed-makers", component_property="figure"),
    Output(component_id="rssi-vs-snr", component_property="figure"),
    Output(component_id="hotspot-name", component_property="children"),
    Output(component_id="explorer-links", component_property="children"),
    Output(component_id="witness-graph", component_property="elements"),
    Output(component_id="hotspot-details", component_property="children"),
    Input(component_id="entries-table", component_property="data"),
    Input(component_id="entries-table", component_property="active_cell"),
    Input(component_id="entries-table", component_property="page_current")

)
def select_entry(entries, selected_cell, page_current):
    # if selected_cell:
    entry_idx = selected_cell["row"] + page_current * PAGE_SIZE
    issue_number = entries[entry_idx]["issue_number"]
    address = entries[entry_idx]["address"]
    owner = entries[entry_idx]["owner"]
    hotspot_name = entries[entry_idx]["name"]
    maker = entries[entry_idx]["maker"]
    #
    distance_vs_rssi = aws.get_object(s3, os.getenv("S3_BUCKET"), key=f"issues/{issue_number}/entries/{address}/distance_vs_rssi")
    witnessed_makers = aws.get_object(s3, os.getenv("S3_BUCKET"), key=f"issues/{issue_number}/entries/{address}/witnessed_makers")
    hotspot_details = json.dumps(aws.get_object(s3, os.getenv("S3_BUCKET"), key=f"issues/{issue_number}/entries/{address}/hotspot_details"))
    witness_graph = pd.DataFrame(aws.get_object(s3, os.getenv("S3_BUCKET"), key=f"issues/{issue_number}/entries/{address}/witness_graph"))
    try:
        rssi_vs_snr = aws.get_object(s3, os.getenv("S3_BUCKET"), key=f"issues/{issue_number}/entries/{address}/rssi_vs_snr")
    except:
        rssi_vs_snr = {"rssi": [], "snr": []}

    dvr_fig = px.scatter(pd.DataFrame(distance_vs_rssi), x="distance_m", y="rssi", trendline="ols", trendline_color_override="black",
                         title="Distance vs. RSSI")
    wm_fig = px.pie(pd.DataFrame(witnessed_makers), names="maker", values="n_witnessed", title="Witnessed Makers")
    rvs_fig = px.scatter(pd.DataFrame(rssi_vs_snr), x="rssi", y="snr", title="RSSI vs. SNR")

    dvr_fig.update_layout(transition_duration=50)
    wm_fig.update_layout(transition_duration=50)
    rvs_fig.update_layout(transition_duration=50)

    hotspot_link = f"https://explorer.helium.com/hotspots/{address}"
    owner_link = f"https://explorer.helium.com/accounts/{owner}"

    explorer_links = [
        html.A("View Hotspot on Explorer", href=hotspot_link),
        html.Br(),
        html.A("View Owner on Explorer", href=owner_link)
    ]
    elements = draw_witness_graph(witness_graph)
    return dvr_fig, wm_fig, rvs_fig, f"{hotspot_name} ({maker})", explorer_links, elements, hotspot_details


@app.callback(
    Output(component_id="accepted-entries", component_property="data"),
    Output(component_id="add-selected-button", component_property="n_clicks"),
    Input(component_id="accepted-entries", component_property="data"),
    Input(component_id="entries-table", component_property="selected_rows"),
    Input(component_id="add-selected-button", component_property="n_clicks"),
    Input(component_id="issue-type", component_property="children"),
    Input(component_id="entries-table", component_property="data")
)
def accept_selected(current_list, selected_rows, n_clicks, type, entries):
    if n_clicks > 0:
        additions = [{"address": entries[r]["address"], "issue": entries[r]["issue_number"], "issue_type": type} for r in selected_rows]
        return current_list + additions, 0


@app.callback(
    Output(component_id="download-denylist", component_property="data"),
    Output(component_id="pr-message", component_property="value"),
    Input(component_id="accepted-entries", component_property="data"),
    Input(component_id="download-btn", component_property="n_clicks"),
    prevent_initial_call=True
)
def generate_pr(accepted_entries, n_clicks):
    if n_clicks > 0:
        current_denylist = requests.get("https://raw.githubusercontent.com/helium/denylist/main/denylist.csv").text.split(",\n")[:-1]
        for e in accepted_entries:
            if e["issue_type"] == "Addition":
                current_denylist.append(e["address"])
            elif e["issue_type"] == "Removal":
                current_denylist.remove(e["address"])

        new_denylist = pd.DataFrame(set(current_denylist), index=None)
        closed_issues = set([a["issue"] for a in accepted_entries])
        pr_message = ""
        for issue in closed_issues:
            pr_message += f"Closes #{issue}\n"

        return dcc.send_data_frame(new_denylist.to_csv(index=False, header=False), "denylist.csv"), pr_message


@app.callback(
    Output(component_id="download-additions", component_property="data"),
    Output(component_id="download-additions-pr", component_property="data"),
    Input(component_id="accepted-entries", component_property="data"),
    Input(component_id="download-additions-btn", component_property="n_clicks"),
    prevent_initial_call=True
)
def download_additions(accepted_entries, n_clicks):
    if n_clicks > 0:
        additions = [e["address"] for e in accepted_entries if e["issue_type"] == "Addition"]
        closed_issues = set([e["issue"] for e in accepted_entries if e["issue_type"] == "Addition"])
        pr_message = ""
        for issue in closed_issues:
            pr_message += f"Closes #{issue}\n"
        additions_str = ""
        for i, addition in enumerate(additions):
            additions_str += addition + ",\n" if i != len(additions) - 1 else addition
        return dcc.send_string(additions_str, "additions.csv"), dcc.send_string(pr_message, "pr-message.txt")


@app.callback(
    Output(component_id="download-removals", component_property="data"),
    Output(component_id="download-removals-pr", component_property="data"),
    Input(component_id="accepted-entries", component_property="data"),
    Input(component_id="download-removals-btn", component_property="n_clicks"),
    prevent_initial_call=True
)
def download_additions(accepted_entries, n_clicks):
    if n_clicks > 0:
        removals = [e["address"] for e in accepted_entries if e["issue_type"] == "Removal"]
        closed_issues = set([e["issue"] for e in accepted_entries if e["issue_type"] == "Removal"])
        pr_message = ""
        for issue in closed_issues:
            pr_message += f"Closes #{issue}\n"
        removals_str = ""
        for i, removal in enumerate(removals):
            removals_str += removal + ",\n" if i != len(removals) - 1 else removal
        return dcc.send_string(removals_str, "removals.csv"), dcc.send_string(pr_message, "pr-message.txt")


if __name__ == "__main__":
    app.run_server(debug=True, host='0.0.0.0', port=8050)
