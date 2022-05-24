import ctypes
import gc
import re
from collections import Counter
from dataclasses import asdict, dataclass
from enum import auto
from itertools import chain
from time import perf_counter
from typing import Any, Dict, List, Set, Tuple, Union

import dask.dataframe as ddf
import nltk
import pandas as pd
import spacy
from dask.distributed import Client, get_worker

from config import Config, StrEnum

def read_stopwords(storage) -> set:
    res = set(
        pd.read_csv(
            "s3://daskdata21/assets/stopwords.txt", 
            encoding="ISO-8859-2", 
            sep="\t", 
            header=None,
            storage_options=storage
        ).squeeze("columns")
    )
    res = {
        x.replace("š", "ą")
         .replace("\x9c", "ś")
         .replace("\x9f", "ć")
        for x in res
    }
    return res


def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


def token_criteria(
    _token: spacy.tokens.token.Token, 
    stopwords: Union[set, list]
) -> bool:
    token = str(_token)
    return (
        2 < len(token) < 20
        and token.isalnum()
        and token not in stopwords
        and str(_token.lemma_) not in stopwords
    )


def clean_sents(
    text: spacy.tokens.doc.Doc,
    stopwords: set
) -> Tuple[list, list]:
    _sents = list(text.sents)
    raw_sents = [str(x) for x in _sents]
    sents = [
        [str(tok.lemma_) for tok in sent if token_criteria(tok, stopwords)] 
        for sent in _sents
    ]
    return raw_sents, sents


def dict_from_varnames(_dict: dict, names: list) -> Dict[str, Any]:
    return {name: _dict.get(name) for name in names if name in _dict}


def find_hashtags(text: str) -> List[str]:
    return re.findall(r"#(\w+)", text)


def transform_text(text: spacy.tokens.doc.Doc, stopwords:set) -> Dict[str, Any]:
    _sents = clean_sents(text, stopwords)
    sents = _sents[0]
    lemmatized_sent_lists = _sents[1]
    lemmatized_sent_strings = [" ".join(sent) for sent in lemmatized_sent_lists]
    lemmata = list(chain.from_iterable(lemmatized_sent_lists))
    unique_lemmata = list(set(lemmata))
    lemmata_bigrams = list(nltk.bigrams(lemmata))
    lemmata_trigrams = list(nltk.trigrams(lemmata))
    text = str(text)
    hashtags = find_hashtags(text)
    return dict_from_varnames(locals(), [x for x in locals() if not x.startswith("_")])


def dict_to_df(d: dict) -> pd.DataFrame:
    data = d.items()
    index = d.keys()
    return pd.DataFrame(data, index=index).transpose().tail(1)


class MetaStrEnum(StrEnum):
    def __init_subclass__(cls):
        cls.as_meta = lambda: sorted(
            [(x, None) for x in cls.__dict__.get("_member_names_", [])]
        )
        return cls


class PreProcessingColumns(MetaStrEnum):
    text = auto()
    date = auto()
    domain = auto()
    url = auto()


class PostProcessingColumns(MetaStrEnum):
    lemmata = auto()
    lemmata_bigrams = auto()
    lemmata_trigrams = auto()
    lemmatized_sent_lists = auto()
    lemmatized_sent_strings = auto()
    sents = auto()
    unique_lemmata = auto()
    hashtags = auto()
    text = auto()
    stopwords = auto()
    date = auto()
    domain = auto()
    url = auto()


def process_text(partition: pd.DataFrame, stopwords:set) -> pd.DataFrame:
    worker = get_worker()
    try:
        nlp = worker.nlp
    except AttributeError:
        nlp = spacy.load("pl_core_news_sm", disable=["ner", 'tagger'])
        worker.nlp = nlp

    res = []
    for doc in nlp.pipe(partition):
        res.append(dict_to_df(transform_text(doc, stopwords)))
    concated_df = pd.concat(res)
    concated_df.index = partition.index
    concated_df = concated_df[sorted(concated_df.columns)]
    return concated_df


def gather_counters(counter_series: pd.Series) -> Counter:
    c = Counter()
    for counter in counter_series:
        c += counter
    return c


def count_subelements(partition: pd.DataFrame) -> Counter:
    return Counter([x for y in partition for x in y])

def export_counts(df, colname) -> None:
    config = Config.from_ini()
    c = gather_counters(
        df[colname].map_partitions(
            lambda p: count_subelements(p), meta=(None, str)
        )
    )
    df = pd.DataFrame(c.most_common(500))
    df.columns = ("phrases", "count")

    path = getattr(config.s3, f"{colname}_path")

    df.to_csv(path, index=False)


def run(export_counts=export_counts):
    s = perf_counter()
    config = Config.from_ini()
    storage = dict(
        key=config.awsacademy.key,
        secret=config.awsacademy.secret,
        token=config.awsacademy.token
    )

    STOPWORDS = read_stopwords(storage)

    df = ddf.read_parquet(
        config.s3.raw_file_path,
        storage_options=storage, 
        columns=['text', 'date', 'url', 'domain']   
    )

    df = df.repartition(config.dask.num_partitions)

    _df = df.map_partitions(
        lambda df: process_text(df.text, stopwords=STOPWORDS), 
        meta = PostProcessingColumns.as_meta()
    )

    df = df.drop('text', axis=1)
    df = ddf.concat([_df, df], axis=1)   
    df = df.persist()

    export_counts(df, 'hashtags')
    export_counts(df, 'unique_lemmata')
    export_counts(df, "lemmata_bigrams")
    export_counts(df, "lemmata_trigrams")

    droppable_cols = [
        'lemmata_trigrams',
        'lemmata_bigrams',
        'lemmatized_sent_lists',
        'stopwords'
    ]

    for col in droppable_cols:
        df = df.drop(col, axis=1)

    df.to_parquet(
        "s3://daskdata21/raw_data/experiment90.parquet", 
        storage_options=storage,
        engine='pyarrow',
        compression='snappy',
        write_index=False
    )
    print(perf_counter() - s)

def clean_memory(client):
    client.run(trim_memory)
    client.run(gc.collect)

if __name__ == "__main__":
    client = Client("tcp://localhost:8152")
    clean_memory(client)
    run(export_counts=export_counts)
    clean_memory(client)
