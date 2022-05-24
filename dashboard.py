import ast
import json
import random
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from enum import Enum, auto, unique
from itertools import groupby, product
from tkinter.tix import MAIN
from typing import Any, Callable, Dict, List

import dash
import dash_bootstrap_components as dbc
import dask.dataframe as ddf
import numpy as np
import pandas as pd
import plotly.express as px
import requests
import spacy
from bs4 import BeautifulSoup
from dash import Dash, dash_table, dcc, html
from dash_extensions.enrich import (Dash, Input, Output, State, dcc, html,
                                    no_update)
from dask.distributed import Client, LocalCluster, get_worker
import warnings; warnings.simplefilter(action = "ignore", category = FutureWarning)


def load_config():
    #nested import to avoid problems with Dask workers 
    from daskapp.config import Config, StrEnum
    config = Config.from_ini()
    storage = dict(
        key=config.awsacademy.key,
        secret=config.awsacademy.secret,
        token=config.awsacademy.token
    )
    return config, storage

def read_s3(path:str):
    return ddf.read_parquet(
        path, 
        storage_options=storage
    )

def load_df_from_s3():
    return read_s3(
        "s3://daskdata21/raw_data/experiment90.parquet/"
    )

def resample_by_month(df):
    conv = lambda date: f"0{date}" if len(str(date)) == 1 else date
    signature = lambda row: f"{row.year}_{conv(row.month)}"
    return (
        df.date
          .astype('M8[us]')
          .apply(
              lambda row: signature(row),
              meta=('date', 'object')
          )
          .compute()
          .value_counts()
          .sort_index()
    )

external_stylesheets=[dbc.themes.GRID, 'https://codepen.io/chriddyp/pen/bWLwgP.css']
app = Dash(__name__, external_stylesheets=external_stylesheets)
nlp = spacy.load("pl_core_news_lg")
client = Client("tcp://localhost:8152")
config, storage = load_config()

_MAIN_DF = load_df_from_s3()
_MAIN_DF['hashtags'] = _MAIN_DF['hashtags'].apply(lambda row: ["#" +x for x in row])
MAIN_DF_TREND = resample_by_month(_MAIN_DF)
MAIN_DF_LEN = len(_MAIN_DF)
MAIN_DF = client.persist(_MAIN_DF)

colors = [
    "(0, 0, 255",
    "(255, 0, 0",
    "(0, 255, 0",
    "(255, 128, 0",
    "(255, 0, 255",
    "(0, 255, 255"
]

tab_colors = [
    "rgba(238, 108, 77, .45)",
    "rgba(23, 163, 152, .45)",
    "rgba(50, 13, 109, .45)",
    "rgba(50, 194, 109, .45)",
    "rgba(255, 194, 109, .45)",
    "rgba(231, 70, 109, .45)",
    "rgba(50, 177, 221, .45)"
]

class StrEnum(str, Enum):

    def _generate_next_value_(_name, _a, _b, _c):
        return str(_name)

    def __str__(self):
        return self.value

class TableAsset(StrEnum):
    unique_lemmata = auto()
    hashtags = auto()
    bigrams = auto()
    trigrams = auto()
    lemmatized_sent_strings = auto()

class TabNames:
    WORDS = "Top words"
    HASHTAGS = "Top hashtags"
    BIGRAMS = "Top bigrams"
    TRIGRAMS = "Top trigrams"
    NEXTPREV = "Top next-prev words"
    ASSOC = "Search assoc"
    TOPIC = "Topic criteria"

datatable_style = {
    'style_table':{
        'overflowY': 'auto',
        'height':700,
        'width':1200,
        'font-family':'sans-serif',
        'font_size':13,
        "border":"2px solid black"
    },
    'style_header':{
        'backgroundColor': 'rgb(30, 30, 30)',
        'color': 'white',
        'font-family':'sans-serif',
        'border': '1px solid #121212'
    },
    'style_cell':{
        'textAlign':'left',   
        'overflow': 'hidden',
        'textOverflow': 'ellipsis',
        'maxWidth': 0,
        'padding':10,
        'font-family':'sans-serif',
    },
    'style_data':{
        'whiteSpace': 'normal',
        'height': 'auto',
        'font-family':'sans-serif',
        'border': '1px solid #121212',
        'color':'black',
        'line-height':1.4
    },
    'markdown_options':{
        'html':True
    },
    "style_cell_conditional":[
        {'if': {'column_id': 'index'},
         'width': '5%'},
        {'if': {'column_id': 'text'},
         'width': '60%'},
    ]
}

asset_table_style = {
    'style_table':{
        'overflowY': 'auto',
        'height':400,
        'font-family':'sans-serif',
        'font_size':11,
        "border":"2px solid black"
    },
}

def synonyms_scraper(word:str, lang:str="pl") -> set:
    USERAGENTS = (
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1", 
        "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36"
    )
    ua = USERAGENTS[random.randrange(len(USERAGENTS))]
    headers = {'user-agent': ua}
    url = f"https://synonyms.reverso.net/synonim/{lang}/{word}"
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    synonyms = [x.text for x in soup.select("a.synonym")][:12]
    return set(synonyms)

def datatable(df: pd.DataFrame, id:str, style:Dict[str, Any], page_size:int = 50):
    return dash_table.DataTable(
        data=df.to_dict('records'), 
        columns=[{"name": i, "id": i} for i in df.columns], 
        page_size=page_size,
        id=id,
        **style
    )

def tab(label:str, *children):
    tab_color = tab_colors.pop()
    return dcc.Tab(
        label=label, 
        children=[*children], 
        style={
            "backgroundColor":tab_color,
            "border": "1px solid black"
        }
    )

def text_input(id:str, **kwargs):
    return dcc.Input(
        id=f'txt_input_{id}',
        type='text',
        debounce=True,
        **kwargs
    )

def most_similar(word:str, n:int=15) -> set:
    vectors = nlp.vocab.vectors
    strings = nlp.vocab.strings
    word = vectors[strings[word]]
    res = vectors.most_similar(
        np.asarray([word]), 
        n=n
    )
    res = {strings[w].lower() for w in res[0][0]}
    return {str(nlp(x)[0].lemma_) for x in res}

def retrieve_all_forms_of_root_word(
    root:str,
    _dict = defaultdict(set)
):
    def inner():
        if not _dict:
            lookups = nlp.get_pipe("lemmatizer").lookups
            for table in lookups.tables:
                _table = lookups.get_table(table)
                for k, v in _table.items():
                    _dict[v].add(k)
        result = {root}
        for x in _dict[root]:
            try:
                result.add(nlp.vocab[x].text)
            except KeyError:
                pass
        return result
    return inner()

def read_asset(asset:TableAsset):
    df = pd.read_csv(
        f"s3://daskdata21/processed/{asset}.csv",
        storage_options=storage
    )
    if asset == TableAsset.hashtags:
        df['phrases'] = df.phrases.apply(lambda x: "#"+x)
    return df

def multiple_replace(_dict, text):
    regex = re.compile("|".join(map(re.escape, _dict.keys())))
    return regex.sub(lambda match: _dict.get(match.group(0)), text)

def colour_phrase(phrase, color="(8, 175, 123"):
    color_bg = f'{color}, 0.4)'
    return f'''<strong 
    style="color:black; 
    background-color: rgba{color_bg}; 
    padding: 1px; 
    border-radius: 3px; 
    border: 1px solid rgb{color})">{phrase[0]+"<span></span>"+phrase[1:]}</strong>'''

def colour_sent(sent, color ="(8, 175, 123"):
    color_bg = f'{color}, 0.33)'
    return f'''<span
    style="color:black; 
    background-color: rgba{color_bg}; 
    padding: 1px; 
    ">{sent}</span>'''

class Filters:

    def restrict_df_to_rows_containing_ent(df, col, entity):
        return df[
            df[col].apply(set, meta=(col, 'object'))
                   .apply(lambda x: entity in x, meta=(col, 'bool'))
        ]

    def restrict_df_to_rows_containing(all_any:str, df, col, entity):
        func = {
            'all':all,
            'any':any
        }.get(all_any)
    
        return df[
            df[col].apply(set, meta=(col, 'object'))
                .apply(
                    lambda row: func(x in row for x in entity), 
                    meta=(col, 'bool')
                )
        ]

    def experimental_restrict(df, col, product_vals):
        return df[
            df[col].apply(
                lambda row: any(all(word in row for word in pair) for pair in product_vals)
            )
        ]
    
    def experimental_restrict_sents(df, col, product_vals):
        return df[col].apply(
                lambda row: [sent for sent in row if any(all(word in sent for word in pair) for pair in product_vals)]
            )
        
    def restrict_row_values_to_containing_any(df, col, forms):
        return df[col].apply(
            lambda row: [x for x in row if any([y in x for y in forms])],
            meta=(col, 'object')
        )

    def colour_sentences(df, sent_col, target_col='sent_replacements', color ="(8, 175, 123"):
        return df[sent_col].apply(
            lambda row: {sent: colour_sent(sent, color=color) for sent in row},
            meta=(target_col, 'object')
    )
    
    def replace_sents_in_text(df, repl_col, text_col):
        return df.apply(
            lambda row: multiple_replace(row[repl_col], row[text_col]), 
            axis=1,
            meta=(text_col, 'object')
    )

    def replace_words_in_text(df, repl_dict, text_col):
        return df[text_col].apply(
            lambda row: multiple_replace(repl_dict, row), 
            meta=(text_col, 'object')
    )

    def generate_word_replacements(forms):
        replacements = {form:colour_phrase(form) for form in forms}
        return {
            x:y for x, y in 
            sorted(
                replacements.items(), 
                key=lambda x: len(x[0]), 
                reverse=True
            )
        }

    def replace_words(df, forms):
        replacements = Filters.generate_word_replacements(forms)
        return Filters.replace_words_in_text(df, replacements, 'text')
    
    def unravel_attr(topic_list, attr):
        res = [getattr(x, attr) for x in topic_list]
        return {x for y in res for x in y}

    def retrieve_vals_from_form_string(cell_value):
        vals = [retrieve_all_forms_of_root_word(form) 
                        for form in ast.literal_eval(cell_value)]
        vals = [x for y in vals for x in y]
        return sorted(vals, key=len, reverse=True)


    def _retrieve_vals_from_form_string(cell_value):
        vals = [retrieve_all_forms_of_root_word(form) 
                        for form in ast.literal_eval(cell_value)]
        product_vals = list(product(*vals))
        vals = [x for y in vals for x in y]
        return sorted(vals, key=len, reverse=True), product_vals

def filter_decorator(fn):
    def inner(*args, **kwargs):
        df = fn(*args, **kwargs)
        df = df[['text', 'date', 'url', 'domain']]
        return df.reset_index()
    return inner

@filter_decorator
def filter_df_single(_df, col, entity, forms):
    df = _df.copy()
    df = Filters.restrict_df_to_rows_containing_ent(df, col, entity)
    df['unique_lemmata'] = df['unique_lemmata'].apply(
        set, 
        meta=('unique_lemmata', 'object')
    )
    df['sents'] = Filters.restrict_row_values_to_containing_any(df, 'sents', forms)
    df['sent_replacements'] = Filters.colour_sentences(df, 'sents')
    df['text'] = Filters.replace_sents_in_text(df, 'sent_replacements', 'text')
    df['text'] = Filters.replace_words(df, forms)
    return df.reset_index()

@filter_decorator
def filter_df_multi(_df, col, entity, forms, product_vals):
    entity = ast.literal_eval(entity)
    df = _df.copy()
    df = Filters.experimental_restrict(df, col, product_vals)
    df['sents'] = Filters.experimental_restrict_sents(df, 'sents', product_vals)
    df['sent_replacements'] = Filters.colour_sentences(df, 'sents')
    df['text'] = Filters.replace_sents_in_text(df, 'sent_replacements', 'text')
    df['text'] = Filters.replace_words(df, forms)
    return df

def filter_df_any(_df, col, entity, forms):
    df = _df.copy()
    df = Filters.restrict_df_to_rows_containing("any", df, col, entity)
    df['text'] = Filters.replace_words(df, forms)
    return df

@filter_decorator
def filter_df_any_color(_df, col, topics):
    df = _df.copy()
    unique_entities = Filters.unravel_attr(topics, 'phrases')
    all_forms = Filters.unravel_attr(topics, 'all_forms')
    color_mappings = {}
    for top in topics:
        for key in top.color_phrase_mapping:
            color_mappings[key] = top.color_phrase_mapping[key]
    key_func = lambda pair: pair[1]
    gb = groupby(color_mappings.items(), key_func)
    color_groups = [[x, [p[0] for p in y]] for x, y in gb]    
    df = Filters.restrict_df_to_rows_containing('any', df, col, unique_entities)
    for color, phrase_list in color_groups:
        df[f'_sents'] = Filters.restrict_row_values_to_containing_any(df, 'sents', phrase_list) 
        df["sents"] = df.apply(
            lambda row: [x for x in row['sents'] if x not in row[f'_sents']], 
            axis=1, 
            meta=('sents', 'object')
        )       
        df[f'sent_replacements'] = Filters.colour_sentences(df, f'_sents', color=color)
        df['text'] = Filters.replace_sents_in_text(df, f'sent_replacements', 'text')
    replacements = {form:colour_phrase(form, color_mappings[form]) for form in all_forms}
    df['text'] = Filters.replace_words_in_text(df, replacements, 'text')
    return df

def next_prev(_df, col, word):
    longstring = " ".join(
        _df[col].apply(
            lambda row: " ".join(row), 
            meta=("lemmatized_sent_strings", "object")
        )
    )
    sort_nested = lambda x: [" ".join(sorted(y.split())) for y in x]
    prevs = re.findall(f"\w+\s{word}", longstring)
    nexts = re.findall(f"{word}\s\w+", longstring)
    raw_res = sort_nested([*prevs, *nexts])
    df = pd.DataFrame(Counter(raw_res).most_common(50))
    df.columns = ['phrase', 'count']
    df['phrase'] = df.phrase.apply(lambda row: str(tuple(row.split())))
    return df

def line_from_df(df):
    data = resample_by_month(df)
    return px.line(
        data,
        title="Mentions by month"
    )

def create_line_chart():
    fig = px.line(MAIN_DF_TREND, title="Mentions by month")
    return dcc.Graph(id='line', figure=fig)

def create_asset_table(asset:TableAsset):
    df = read_asset(asset)
    return datatable(
        df,
        page_size=500,
        id=asset.value,
        style=asset_table_style
    )

def create_empty_asset_table(id):
    return datatable(
        pd.DataFrame(),
        page_size=50,
        id=id,
        style=asset_table_style
    )

def create_main_table():
    df = MAIN_DF[['text', 'date', 'domain', 'url']].head(50)
    return datatable(
        df,
        id='main_table',
        page_size=50,
        style=datatable_style
    )

def create_text_input(id):
    return text_input(id)

def create_topic_input():
    return dbc.Container([
        dcc.Textarea(
            id='topic-textarea',
            value='',
            style={
                'width': '100%', 
                'height': 200,
                'font-family': 'consolas'
            },
        ),
        html.Button(
            'Submit', 
            id='topic-button', 
            n_clicks=0
        ),
    ])

MAIN_TITLE = html.H1(
    "SOCIAL LISTENING TOOLKIT - DISCOVER INSIGHTS",
    style={'font-family':"Montserrat"}
)
MAIN_TABLE = create_main_table()
WORDS = create_asset_table(TableAsset.unique_lemmata)
HASHTAGS = create_asset_table(TableAsset.hashtags)
BIGRAMS= create_asset_table(TableAsset.bigrams)
TRIGRAMS = create_asset_table(TableAsset.trigrams)
ASSOC = create_text_input(id="assoc")
NEXTPREV = create_text_input(id="nextprev")
TOPIC = create_topic_input()
NEXTPREV_SUBTABLE = create_empty_asset_table("nextprev_subtable")
LINE = create_line_chart()
COUNTER_HEADING = html.H3(f"There are {MAIN_DF_LEN} mentions")
ASSOC_SYNONYMS = html.H6()
TOPIC_BAR = dcc.Graph(id='topic_bar')

class Components:
    MAIN_TABLE = MAIN_TABLE
    WORDS = WORDS
    HASHTAGS = HASHTAGS
    BIGRAMS = BIGRAMS
    TRIGRAMS= TRIGRAMS
    ASSOC = ASSOC
    NEXTPREV = NEXTPREV
    TOPIC = TOPIC
    NEXTPREV_SUBTABLE = NEXTPREV_SUBTABLE
    LINE = LINE
    COUNTER_HEADING = COUNTER_HEADING
    ASSOC_SYNONYMS = ASSOC_SYNONYMS
    TOPIC_BAR = TOPIC_BAR

main_components = {
    TabNames.WORDS: Components.WORDS,
    TabNames.BIGRAMS: Components.BIGRAMS,
    TabNames.TRIGRAMS: Components.TRIGRAMS,
    TabNames.HASHTAGS: Components.HASHTAGS,
    TabNames.ASSOC: Components.ASSOC,
    TabNames.NEXTPREV: Components.NEXTPREV,
    TabNames.TOPIC:Components.TOPIC
}

TABS = dcc.Tabs(
    [
        tab(
            TabNames.WORDS, 
            html.H4("Top words"), 
            html.H6("Click a word to filter the main table"), 
            WORDS
        ),
        tab(
            TabNames.BIGRAMS, 
            html.H4("Top bigrams"), 
            html.H6("Click a bigram to filter the main table"), 
            BIGRAMS
        ),
        tab(
            TabNames.TRIGRAMS, 
            html.H4("Top trigrams"), 
            html.H6("Click a trigram to filter the main table"), 
            TRIGRAMS
        ),
        tab(
            TabNames.HASHTAGS, 
            html.H4("Top hashtags"), 
            html.H6("Click a hashtag to filter the main table"), 
            HASHTAGS
        ),
        tab(
            TabNames.ASSOC, 
            html.H4("Associative search"), 
            html.H6("Enter a phrase to filter the main table with its synonyms & contextually-associated phrases"), 
            ASSOC, 
            ASSOC_SYNONYMS
        ),
        tab(
            TabNames.NEXTPREV, 
            html.H4("Top next/previous words"), 
            html.H6("Enter a phrase to see its most frequent neighbooring phrases; then click a pair to filter the main table"), 
            NEXTPREV, 
            NEXTPREV_SUBTABLE
        ),
        tab(
            TabNames.TOPIC, 
            html.H4("Phrase-based topic modelling"), 
            html.H6("Enter topic:phrase list pairs as a JSON dict below."), 
            TOPIC, 
            TOPIC_BAR
        )
    ],
    style={
        'margin-bottom':'20px'
    }
)

layout = dbc.Container([
            MAIN_TITLE,
            dbc.Row([    
            dbc.Col([ TABS ], width="4", style={"height": "90%"}),
            dbc.Col([COUNTER_HEADING, MAIN_TABLE, LINE], width="8", style={"height": "90%"}),
        ], 
        style={"height": "100%"})],
    fluid=True,
    style={"height": "90vh"}
)

app.layout = layout

component_column_map ={
    Components.WORDS: TableAsset.unique_lemmata,
    Components.HASHTAGS: TableAsset.hashtags,
    Components.BIGRAMS: TableAsset.unique_lemmata,
    Components.TRIGRAMS: TableAsset.unique_lemmata,
    Components.ASSOC: TableAsset.unique_lemmata,
    Components.NEXTPREV: TableAsset.lemmatized_sent_strings,
    Components.TOPIC: TableAsset.unique_lemmata,
    Components.NEXTPREV_SUBTABLE: TableAsset.unique_lemmata
}

component_filter_mapping = {
    Components.WORDS: filter_df_single,
    Components.HASHTAGS: filter_df_single,
    Components.BIGRAMS: filter_df_multi,
    Components.TRIGRAMS: filter_df_multi,
    Components.NEXTPREV_SUBTABLE: filter_df_multi
}

components_val_setter_mapping = {
    Components.WORDS: retrieve_all_forms_of_root_word,
    Components.HASHTAGS: retrieve_all_forms_of_root_word,
    Components.BIGRAMS: Filters._retrieve_vals_from_form_string,
    Components.TRIGRAMS: Filters._retrieve_vals_from_form_string,
    Components.NEXTPREV_SUBTABLE: Filters._retrieve_vals_from_form_string
}

def get_cell_value(component, active_cell):
    if not active_cell:
        return no_update
    row = active_cell['row']
    column = active_cell['column_id']
    try:
        res = component.data[row][column]
    except:
        res = component[row][column]
    return res

def handle_cell_value(component, active_cell, data):
    try:
        cell_value = get_cell_value(component, active_cell)
    except:
        cell_value = get_cell_value(data, active_cell)
    return cell_value

def return_filtered_df(df):
    try:
        df = df.compute()
    except AttributeError:
        pass
    columns = [{"id":c, "name":c, "presentation":"markdown"} for c in df.columns]
    return df.to_dict('records'), columns

def dispatch_basic_callback(component, cell_value):
    try:
        vals, product_vals = components_val_setter_mapping[component](cell_value)
    except ValueError:
        vals = components_val_setter_mapping[component](cell_value)
    df = component_filter_mapping[component](
        MAIN_DF,
        component_column_map[component],
        cell_value,
        vals,
        product_vals
    )
    return df

def update_graph(df):
    subline = resample_by_month(df)
    dates = pd.concat((subline, MAIN_DF_TREND), axis=1)
    dates.columns = ['selected mentions', 'all mentions']
    dates = dates.reset_index().melt(id_vars='index')
    fig = px.line(
        dates,
        x='index',
        y='value',
        color='variable',
        title = "Mentions by month",
        markers=True,
        text='value'
    )
    fig.update_traces(textposition=f'top center')
    return fig

def compute_counter_heading(df, cell_value):
    return f"There are {len(df)} mentions that contain {cell_value}"

def table_by(component):
    @app.callback(
        Output(Components.COUNTER_HEADING, "children"),
        Output(Components.LINE, "figure"),
        Output(Components.MAIN_TABLE, 'data'), 
        Output(Components.MAIN_TABLE, 'columns'), 
        Input(component, 'active_cell'),
        State(component, 'data'),
        prevent_initial_call=True
    )
    def _table_by(active_cell, data):
        cell_value = handle_cell_value(component, active_cell, data)
        df = dispatch_basic_callback(component, cell_value)
        new_line = update_graph(df)
        df, columns = return_filtered_df(df)
        return (
            compute_counter_heading(df, cell_value), 
            new_line, 
            df, 
            columns
        )

@app.callback(
    Output(Components.ASSOC_SYNONYMS, "children"),
    Output(Components.COUNTER_HEADING, "children"),
    Output(Components.LINE, "figure"),
    Output(Components.MAIN_TABLE, 'data'), 
    Output(Components.MAIN_TABLE, 'columns'), 
    Input(Components.ASSOC, 'value'),
    prevent_initial_call=True
)
def table_by_assoc(value):
    vals = most_similar(value)
    synonyms = synonyms_scraper(value)
    vals = {*vals, *synonyms}
    vals = {x for x in vals if len(x) > 2}
    vals_forms = [retrieve_all_forms_of_root_word(x) for x in vals]
    vals_forms = {x for y in vals_forms for x in y}
    vals_forms = {x for x in vals_forms if len(x) >2}
    df = filter_df_any(
        MAIN_DF, 
        component_column_map[Components.ASSOC],
        vals,
        vals_forms
    )
    new_line = update_graph(df)
    df = df[['text', 'date', 'url', 'domain']]
    df, columns = return_filtered_df(df)
    return (
        " | ".join(vals),
        f"There are {len(df)} mentions that contain {value} or its synonyms", 
        new_line, 
        df, 
        columns
    )

@app.callback(
    Output(Components.NEXTPREV_SUBTABLE, 'data'),
    Output(Components.NEXTPREV_SUBTABLE, 'columns'),
    Input(Components.NEXTPREV, 'value'),
    prevent_initial_call=True
)
def table_by_np(value):
    df = next_prev(
        MAIN_DF, 
        component_column_map[Components.NEXTPREV],
        value
    )
    return return_filtered_df(df)

def create_topic_chart(df, topics):
    counts = []    
    for top in topics:
        counts.append((
            top.topic,
            top.color + ", .5)",
            len(df[df['text'].str.contains(str(top.color), regex=False)])
        ))
    counts = sorted(counts, key= lambda x: x[2])
    counts_df = pd.DataFrame(counts)
    counts_df.columns = ['category', 'color', 'count']
    counts_df.color = counts_df.color.apply(
        lambda x: "rgba" + x.replace(" ", "").rsplit(",",1)[0] + ",.5)"
    )
    bar = px.bar(
        counts_df,
        x = 'count',
        y = 'category',
        color='color',
        color_discrete_sequence=counts_df.color.values,
        title="Topics: number of mentions"
    )
    bar.layout.update(showlegend=False)
    return bar

@dataclass
class Topic:
    topic:str
    phrases: List[str]
    color:str = None

    def __post_init__(self):
        vals = [retrieve_all_forms_of_root_word(form) for form in self.phrases]
        self._all_forms = [x for y in vals for x in y]

    @property
    def color_phrase_mapping(self):
        return {phrase:self.color for phrase in self.all_forms}
    
    @property
    def all_forms(self):
        return self._all_forms

@app.callback(
    Output(Components.TOPIC_BAR, "figure"),
    Output(Components.MAIN_TABLE, 'data'), 
    Output(Components.MAIN_TABLE, 'columns'), 
    Input('topic-button', 'n_clicks'),
    State('topic-textarea', 'value'),
    prevent_initial_call=True
)
def match_topics(n_clicks, value):
    if n_clicks > 0:
        topics = [Topic(topic, phrases) for topic, phrases in json.loads(value).items()]
        for color, topic in zip(colors, topics):
            topic.color = color
        df = filter_df_any_color(
            MAIN_DF,
            component_column_map[Components.TOPIC],
            topics
        )     
        bar = create_topic_chart(df, topics)
        df, columns = return_filtered_df(df)
        return bar, df, columns

table_by(Components.BIGRAMS)
table_by(Components.TRIGRAMS)
table_by(Components.HASHTAGS)
table_by(Components.WORDS)
table_by(Components.NEXTPREV_SUBTABLE)

if __name__ == '__main__':
    app.run_server(debug=False, threaded=True)