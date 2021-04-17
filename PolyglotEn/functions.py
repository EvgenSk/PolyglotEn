import os
import logging
import asyncio

import pathlib
import spacy
import json

import azure.functions as func
from collections.abc import Iterable
from azure.servicebus import ServiceBusClient, ServiceBusSender, ServiceBusMessage
from azure.servicebus.management import ServiceBusAdministrationClient, SqlRuleFilter

nlp = None

async def run(msg: func.ServiceBusMessage):
    try:
        global nlp
        if nlp is None:
            nlp = get_model()
        if msg.message_id == "warmup-message":
            return

        paragraph_text = msg.get_body().decode('utf-8')
        topics_connection_string = os.environ["WordTopicsConnection"]
        q_connection_string = os.environ["TextQueuesConnection"]

        if nlp is None:
            nlp = get_model()

        doc = nlp(paragraph_text)

        paragraphs_task = asyncio.create_task(send_paragraph(q_connection_string, doc, msg.correlation_id, msg.user_properties))

        lemmas = get_lemmas(doc)
        create_rule_for_dictionary_articles(topics_connection_string, msg.correlation_id, msg.user_properties["ParagraphNumber"], lemmas)
        lemmas_task = asyncio.create_task(send_lemmas(topics_connection_string, lemmas, msg.correlation_id, msg.user_properties)) 

        await paragraphs_task
        await lemmas_task
    except Exception as ex:
        message = str.format("Error: {}", ex)
        logging.error(message)


async def send_paragraph(q_connection_string: str, paragraph: spacy.language.Doc, correlation_id: str, application_properties: dict) -> None:
    with ServiceBusClient.from_connection_string(q_connection_string, logging_enable=True) as q_client:
        with q_client.get_queue_sender("annotated-paragraphs") as paragraph_sender:
            message = ServiceBusMessage(
                json.dumps(paragraph.to_json()),
                correlation_id=correlation_id,
                application_properties=application_properties
                )
            paragraph_sender.send_messages(message)

async def send_lemmas(topics_connection_string: str, lemmas: Iterable[str], correlation_id: str, application_properties: dict) -> None:
    with ServiceBusClient.from_connection_string(topics_connection_string, logging_enable=True) as topics_client:
        with topics_client.get_topic_sender("lemmas") as lemma_sender:
            all_lemmas_message = ServiceBusMessage(
                json.dumps(list(lemmas)),
                correlation_id=correlation_id,
                application_properties=application_properties
            )
            lemma_sender.send_messages(all_lemmas_message)

            def get_lemma_messages(lemmas: Iterable[str]) -> Iterable[ServiceBusMessage]:
                for l in lemmas:
                    yield ServiceBusMessage(
                        l.lower(),
                        message_id=str.format("{}-{}", correlation_id, l),
                        application_properties=application_properties,
                        subject="lemma"
                    )

            lemma_sender.send_messages([message for message in get_lemma_messages(lemmas)])

def get_lemmas(doc: spacy.language.Doc) -> Iterable[str]:
    return set([token.lemma_ for token in doc if token.is_alpha and len(token.lemma_) > 1])

def get_model():
    model_name = os.environ["ModelName"]
    return spacy.load(get_model_path(model_name))

def get_model_path(model_name: str):     
    current_path = pathlib.Path(__file__).parent    
    return str(current_path / model_name)

def create_rule_for_dictionary_articles(topics_connection_string: str, correlation_id: str, paragraph_number: int, lemmas: Iterable[str]):
    with ServiceBusAdministrationClient.from_connection_string(topics_connection_string) as adm_client:
        rule_name = str.format("paragraph-{}-rule", paragraph_number)
        return adm_client.create_rule("dictionary-articles", correlation_id, rule_name, filter=create_filter(lemmas))

def create_filter(lemmas: Iterable[str]) -> SqlRuleFilter:
    
    def keys(start_index: int = 0) -> Iterable[str]:
        while True:
            yield str.format("@w{}", start_index)
            start_index+=1

    parameters = { k: w for (k,w) in zip(keys(start_index=1), lemmas) }
    sql_expression = str.format("sys.label IN ({})", ", ".join(parameters.keys()))
    return SqlRuleFilter(sql_expression, parameters)
    