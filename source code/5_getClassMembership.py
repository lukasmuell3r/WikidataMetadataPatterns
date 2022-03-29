import json
import logging
import os.path
import re

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

re_wd = re.compile('http:\/\/www\.wikidata\.org\/entity\/')


def chunks(wd_items_list, n):
    """
    Splitting a list into chunks.
    :param wd_items_list: List to split into chunks
    :param n: chunk size
    :return: a tuple of list chunks
    """
    n = max(1, n)
    return (wd_items_list[i:i + n] for i in range(0, len(wd_items_list), n))


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
def perform_query(items, output_suffix, class_memberships_dir):
    """
    Performs a sparql query to the Wikidata endpoint
    :param items: the items to be queried
    :param output_suffix: the filename of the respective patterns
    :param class_memberships_dir: the ouput directory to store the class memberships of the supporting items per pattern
    :return:
    """
    logging.debug('"perform_query"-method')
    endpoint_url = 'https://query.wikidata.org/sparql'
    logging.debug('Setting SPARQL endpoint to "%s"', endpoint_url)
    sparql = SPARQLWrapper(endpoint_url,
                           agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36')
    sparql.setReturnFormat(JSON)
    logging.debug('Receiving wd_items to construct the query')
    wd_items = items
    l_wd_item_class_iri = list()
    l_wd_items_iri = list()
    for chunk in chunks(wd_items.split(), 300):
        logging.debug('SPARQL-Chunk (Len:\t%s)', len(chunk))
        sparql.setQuery('''
                SELECT ?item ?o
                    WHERE
                    {
                        VALUES ?item {''' + ' '.join(chunk) + '''} # use all values in brackets for variable ?item
                        ?item wdt:P31 ?o . # display all objects which are connected via P31 oder P279 from the item
                    }
                    ''')
        logging.debug('Performing query now.')
        results = sparql.query().convert()
        logging.debug("Received results!")
        logging.debug(
            "Normalizing .json Output to a pandas dataframe and merging output from return chunks to a single list")
        l_wd_item_class_iri = l_wd_item_class_iri + pd.json_normalize(results['results']['bindings'])[
            'o.value'].to_list()
        l_wd_items_iri = l_wd_items_iri + pd.json_normalize(results['results']['bindings'])['item.value'].to_list()
    logging.debug('Removing IRI from results and constructing new lists')
    l_wd_items_class = list()
    l_wd_items = list()
    for wd_item, wd_item_class in zip(l_wd_items_iri, l_wd_item_class_iri):
        if len(list(re.findall(re_wd, wd_item))) == 1 and len(list(re.findall(re_wd, wd_item_class))) == 1:
            l_wd_items.append(wd_item.replace(re.findall(re_wd, wd_item)[0], '').upper())
            l_wd_items_class.append(wd_item_class.replace(re.findall(re_wd, wd_item_class)[0], '').upper())

    logging.debug('Results of wd_items (Len:\t%s):\t%s', len(l_wd_items), l_wd_items)
    logging.debug('Result classes of wd_items(Len:\t%s):\t%s', len(l_wd_items_class), l_wd_items_class)
    logging.debug('Constructing a dictionary for the output of the query')
    d_result = dict()
    for item, item_class in zip(l_wd_items, l_wd_items_class):
        if item not in d_result:
            d_result[item] = []
            d_result.get(item).append(item_class)
        else:
            d_result.get(item).append(item_class)
    logging.debug('Dumping result to .json file')
    file = open(class_memberships_dir + '\\' + output_suffix + '.json', 'w')
    json.dump(d_result, file)
    logging.debug('Finished dumping')


def construct_querys(patterns_with_support_file, class_memberships_dir):
    """
    Constructs sparql querys per patterns for to get all class memberships of their supporting items.
    Needed for the entry into the Wikidata class hierarchy.
    :param patterns_with_support_file: the file containing the patterns with their supporting Wikidata items
    :param class_memberships_dir: the ouput directory to store the class memberships of the supporting items per pattern
    :return:
    """
    logging.info('"construct_querys"-method')
    logging.info('Reading .json pattern file with supporting wd-items.')
    patterns = open(patterns_with_support_file, 'r')
    logging.info('Deserializing into python dictionary')
    d_patterns = json.load(patterns)
    for key in d_patterns:
        logging.info("Pattern:\t%s", key)
        if not os.path.isfile(class_memberships_dir + "\\" + '_'.join(key.split()) + '.json'):
            logging.debug('Pattern support %s > 2000', key.split()[-1][1:-1])
            logging.debug('Constructing SPARQL Query now!')
            wd_items = d_patterns.get(key)
            wd_items = ['wd:{0}'.format(element) for element in wd_items]
            query_items = ' '.join(wd_items)
            logging.debug('Pattern:\t%s', '_'.join(key.split()))
            logging.debug('Calling "perform_query"-method')
            perform_query(query_items, '_'.join(key.split()), class_memberships_dir)
    logging.info("Finished program!")


if __name__ == '__main__':
    """
    The main method reads paths and makes sure they exist before calculations start.
    """
    logging.basicConfig(format='%(levelname)s:\t\t%(message)s', level=logging.INFO)
    logging.info('Starting program!')
    supporting_items_file = input(
        "Enter the path of supportingItemsPerPattern.json file (Example: C:\dump\\analysis\supportingItemsPerPattern.json):\t")
    supporting_items_file = supporting_items_file.replace('"', '').replace("'", "")
    assert os.path.exists(supporting_items_file), "Path not found at:\t" + str(supporting_items_file)
    class_memberships_dir = input("Enter the path of output dir (Example: C:\dump\\analysis\class_memberships):\t")
    class_memberships_dir = class_memberships_dir.replace('"', '').replace("'", "")
    assert os.path.exists(class_memberships_dir), "Path not found at:\t" + str(class_memberships_dir)
    construct_querys(supporting_items_file, class_memberships_dir)
