import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output

from data_model import get_recent_stats, get_discounts, get_daily_stats, get_new_offers

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP])

renovation_mapping = {'З ремонтом': 1, 'Без ремонту': 0}


def get_overview(rf=None, last_days=None):
    return px.bar(
        get_recent_stats(rf, last_days),
        x="Вибірка", y="Ціна м2", color="Розмір", barmode="group",
        range_y=(0, 4000),
        color_discrete_sequence=["#fdd835", "#ffb300", "#fb8c00", "#f4511e"]
    )


def get_daily_chart():
    df = get_daily_stats()
    return go.Figure(data=[go.Candlestick(x=df.index,
         open=df['q3'],
         high=df['q3'],
         low=df['q1'],
         close=df['q1'])])


def get_update_block(content):
    a = html.A(href=f"https://flatfy.ua/uk/realty/{content['flat_id']}", children=f'#{content["flat_id"]}')
    color = "#28a745" if content['diff'] < 0 else "#fa8c00"
    shape = "arrow-down-circle-fill" if content['diff'] < 0  else "arrow-up-circle-fill"
    return html.Div(
            [
                html.I(className=f"bi bi-{shape} me-2", style={'color': color}),
                html.Span(children=[
                    content['title'] + ": ", "квартира ", a, (" здорожчала " if content['diff'] > 0 else " здешевшала "),
                    "на ", html.B(f"{abs(content['diff'])}$ "),
                    html.Span(
                        f"з {content['prev']}$ до {content['now']}$, тепер {int(content['sqm'])}$ за м2 " +
                        (" з ремонтом" if content['renovation'] else ''),
                        className="text-secondary"
                    )
                ])
            ],
            className="d-flex align-items-center bg-white p-2",
    )


def get_insert_block(content):
    a = html.A(href=f"https://flatfy.ua/uk/realty/{content['flat_id']}", children=f'#{content["flat_id"]}')
    color = "#4892ed"
    shape = "plus-circle-fill"
    return html.Div(
            [
                html.I(className=f"bi bi-{shape} me-2", style={'color': color}),
                html.Span(children=[
                    content['title'] + ": ", "додана квартира ", a, " з ціною ", html.B(f"{abs(int(content['price']))}$ "),
                    html.Span(
                        f"{int(content['sqm'])}$ за м2 " + (" з ремонтом" if content['renovation'] else ''),
                        className="text-secondary"
                    )
                ])
            ],
            className="d-flex align-items-center bg-white p-2",
    )


new_offers = get_new_offers()
new_discounts = get_discounts()
news_sids = {row['sid']: row['title'] for row in [*new_offers, *new_discounts]}


app.layout = html.Div(children=[
    html.H1(children='Золотий мурашник', className="text-center"),

    html.Div(children='''
        Тепер жадібність має окремий вимір
    ''', className="text-center"),

    dcc.Graph(
        id='overview-graph',
        figure=get_overview()
    ),

    dcc.Checklist(
        list(renovation_mapping.keys()),
        list(renovation_mapping.keys()),
        id='overview_renovation',
        labelStyle={'display': 'inline-block', 'marginTop': '5px'},
        labelClassName='p-2',
        inputClassName='m-2'
    ),

    html.Br(),

    html.Div(children='''
        Тільки за останні n днів:
    '''),

    dcc.Input(
        id="last_days", type="number",
        debounce=True, placeholder="Дні"
    ),

    html.Br(),
    html.Br(),

    html.H2(children="Новини", className="text-center"),

    dcc.Dropdown({'*': 'Будь-який ЖК', **news_sids}, '*', id='news-selection-dd'),

    html.Div(id='news_block', children=[], className="p-2")
], className="p-5")


@app.callback(
    Output('overview-graph', 'figure'),
    Input('overview_renovation', 'value'),
    Input('last_days', 'value'))
def update_graph(overview_renovation, last_days):
    if not overview_renovation:
        return
    elif len(overview_renovation) == 2:
        rf = None
    else:
        rf = renovation_mapping[overview_renovation[0]]
    return get_overview(rf, last_days)


@app.callback(
    Output('news_block', 'children'),
    Input('news-selection-dd', 'value')
)
def update_news(dd):
    if dd != '*':
        dd = int(dd)
    return [
        *[html.Div(children=get_insert_block(content)) for content in new_offers if dd == '*' or content['sid'] == dd],
        *[
            html.Div(children=get_update_block(content)) for content in new_discounts
            if dd == '*' or content['sid'] == dd
        ]
    ]


if __name__ == '__main__':
    app.run_server(debug=True, port=8051, host="0.0.0.0")
