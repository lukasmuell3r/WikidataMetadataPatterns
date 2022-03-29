import csv
import itertools
import json
import logging
import os
import re
import sys
import time
import traceback

import numpy as np
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

re_wd = re.compile('http:\/\/www\.wikidata\.org\/entity\/')

all_results = dict()
hierarchy = None
P279_class_hierarchy = None
P31_class_hierarchy = None
max_item_class = None
max_p31_class = None
skipped_patterns = dict()
too_new_patterns = dict()
modeling_errors = dict()

instance_requests = dict()
requests = 0


def dump_skipped(results_dir):
    """
    Dumps skipped, too new and modeling error patterns to .json files into a directory using the global dictionarys.
    :param results_dir: directory path
    :return:
    """
    global skipped_patterns
    global too_new_patterns
    global modeling_errors
    global requests
    logging.info('Made %s SPARQL Requests!', requests)
    logging.info('Error patterns:\t%s', len(modeling_errors))
    logging.info('Too new patterns:\t%s', len(too_new_patterns))
    logging.info('Skipped patterns:\t%s', len(skipped_patterns))
    file = open(results_dir + '\\skipped_patterns.json', 'w')
    file2 = open(results_dir + '\\too_new_patterns.json', 'w')
    file3 = open(results_dir + '\\modeling_errors.json', 'w')
    logging.info('Dumping now patterns which where skipped, are too new or have modelling errors')
    json.dump(skipped_patterns, file)
    json.dump(too_new_patterns, file2)
    json.dump(modeling_errors, file3)
    file.close()
    file2.close()
    file3.close()


def dump(results_dir):
    """
    Dumps the global dictionary all_results as .json file into a directory.
    :param results_dir: directory path
    :return:
    """
    global all_results
    logging.debug('Length of Results dict:\t%s', len(all_results))
    file = open(results_dir + '\\results.json', 'w')
    json.dump(all_results, file)
    file.close()


def get_files(dir):
    """
    Provides a list of all files in a directory.
    :param dir: directory path
    :return:
    """
    file_list = next(os.walk(dir))[2]
    path = next(os.walk(dir))[0]
    file_list = [path + '\\{0}'.format(element) for element in file_list]
    return file_list


def load_current_state(dir):
    """
    Loads the current analysis state.
    :param dir: The directory where the .json files with the current state are stored.
    :return:
    """
    global all_results
    global modeling_errors
    global too_new_patterns
    global skipped_patterns
    filehandle = open(dir + '\\results.json', 'r')
    filehandle2 = open(dir + '\\modeling_errors.json', 'r')
    filehandle3 = open(dir + '\\skipped_patterns.json', 'r')
    filehandle4 = open(dir + '\\too_new_patterns.json', 'r')
    all_results = json.load(filehandle)
    modeling_errors = json.load(filehandle2)
    skipped_patterns = json.load(filehandle3)
    too_new_patterns = json.load(filehandle4)
    logging.info('Loaded %s valid results from file!', len(all_results))
    logging.info('Loaded %s modelling error from file!', len(modeling_errors))
    logging.info('Loaded %s skipped patterns from file!', len(skipped_patterns))
    logging.info('Loaded %s too new patterns from file!', len(too_new_patterns))
    filehandle.close()
    filehandle2.close()
    filehandle3.close()
    filehandle4.close()
    logging.info("Finished loading current state!")


def load_classes_via_P31(file):
    """
    Loads the P31 class hierarchy as an array for constant access time and stores it in a global variable.
    :param file: the P31 class hierarchy file
    :return:
    """
    file = open(file, 'r')
    class_via_instance_of = json.load(file)
    file.close()
    logging.info('Loaded %s classes via P31 object relations!', len(class_via_instance_of))
    global P31_class_hierarchy
    P31_class_hierarchy = np.array([None for i in range(class_via_instance_of[-1] + 1)])
    global max_p31_class
    max_p31_class = class_via_instance_of[-1]
    [np.put(P31_class_hierarchy, c, c) for c in class_via_instance_of]
    logging.info('Finished array initialization')


def load_hierarchy(file):
    """
    Loads the P279 class hierarchy as an array for constant access time and stores it in a global variable.
    :param file: the P279 class hierarchy file
    :return:
    """
    file = open(file, 'r')
    hierarchy_json = json.load(file)
    keys = list(hierarchy_json.keys())
    value_lists = list(hierarchy_json.values())
    hierarchy = {int(key): [int(value) for value in value_list] for key, value_list in zip(keys, value_lists)}
    int_keys = hierarchy.keys()
    int_value_lists = hierarchy.values()
    sorted_hierarchy_tuples = sorted(zip(int_keys, int_value_lists))
    hierarchy_sorted = {sorted_tuples[0]: sorted_tuples[1] for sorted_tuples in sorted_hierarchy_tuples}
    hierarchy_array = np.array(list(hierarchy_sorted.items()), dtype='object')
    global max_item_class
    max_item_class = hierarchy_array[-1][0]
    global P279_class_hierarchy
    P279_class_hierarchy = np.array(
        [hierarchy_sorted[i] if i in hierarchy_sorted.keys() else None for i in range(hierarchy_array[-1][0])],
        dtype="object")
    logging.info("Generated sorted hierarchy with length:\t%s", int(hierarchy_array.size / 2))


def do_work(file_list, results_dir):
    """
    Initiates the processing of all patterns in the provided file_list.
    :param file_list: A list of pattern files to process
    :param results_dir: he directory to store the results
    :return:
    """
    file_counter = 0
    for file in file_list:
        get_classes(file, results_dir)
        file_counter += 1
        if file_counter % 10000 == 0 and file_counter != 0:
            logging.info('Processed 1000 files, dumping!')
            dump(results_dir)
            dump_skipped(results_dir)
    dump(results_dir)
    dump_skipped(results_dir)


def construct_result(pattern, pattern_hierarchy, weights, indices, common_superclasses, distinct_class_counts):
    """
    Stores all analysis results into the global all_results dictionary.
    :param pattern: the pattern to process
    :param pattern_hierarchy: the complete relative class hierarchy
    :param weights: the weight values of common superclasses found
    :param indices: the indices of common superclasses found
    :param common_superclasses: list of common superclasses found
    :param distinct_class_counts: all distinct class combinations with their respective count
    :return:
    """
    hierarchy = dict()
    hierarchy['Hierarchy'] = pattern_hierarchy
    result = dict()
    try:
        weighted_indexes = {superclass: ({'Weight': weight}, {'Indices': index}) for superclass, weight, index in
                            zip(common_superclasses, weights, indices)}
    except Exception as e:
        logging.error('Pattern:\t%s', pattern)
        logging.error('Hierarchy:\t%s', pattern_hierarchy)
        logging.error('Weights:\t%s', weights)
        logging.error('Indices:\t%s', indices)
        logging.error('Common superclasses:\t%s', common_superclasses)
    result['Superclasses'] = weighted_indexes
    result['Distinct class counts'] = distinct_class_counts
    global all_results
    all_results[pattern] = [hierarchy, result]


def write_to_csv(pattern, distinct_classes, weights, common_superclasses, superclass_indices, class_percentage, perfect,
                 results_dir):
    """
    Writes the best results per patterns into a .csv file.
    :param pattern: the pattern to process
    :param distinct_classes: the distinct class combinations to count the combinations
    :param weights: the weight values of common superclasses found
    :param common_superclasses: list of common superclasses found
    :param superclass_indices: the indices of common superclasses found
    :param class_percentage: the initial class percentage of a pattern
    :param perfect: boolean the label a perfect pattern
    :param results_dir: the directory to store the results
    :return:
    """
    sorted_results = sorted(zip(weights, common_superclasses, superclass_indices))
    logging.info('Zipped:\t%s', sorted_results)
    best_superclass = sorted_results[0][1]
    if perfect:
        avg_hierarchy_level = sum(sorted_results[0][2]) / (len(sorted_results[0][2]) + 1)
        fields = [pattern, len(distinct_classes), best_superclass, avg_hierarchy_level, max(sorted_results[0][2]),
                  min(sorted_results[0][2]), class_percentage]
    else:
        avg_hierarchy_level = (sum(sorted_results[0][2]) / (len(sorted_results[0][2]) + 1)) + 1
        fields = [pattern, len(distinct_classes), best_superclass, avg_hierarchy_level, max(sorted_results[0][2]) + 1,
                  min(sorted_results[0][2]) + 1, class_percentage]

    logging.debug('Best superclass:\t%s', best_superclass)
    logging.debug('Average hierarchy level of best superclass:\t%s', avg_hierarchy_level)
    logging.debug('CSV fields\t%s', fields)
    csv_file = open(results_dir + r'\analysis.csv', 'a', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(fields)
    csv_file.close()


def perfect_pattern(dict_class_counts, distinct_classes, pattern, supporting_items, results_dir):
    """
    Performs checking whether the pattern is perfect and a common class can be found directly without constructing the
    relative class hierarchy.
    :param dict_class_counts:  the count of each distinct class combinations to weight the superclass
    :param distinct_classes: the distinct class combinations of relative hierarchy level 0
    :param pattern: the pattern to evaluate
    :param supporting_items: the classes to which the supporting items belong to
    :param results_dir: the directory to store the results
    :return:
    """
    base_case = False
    superclasses = list()
    for key in dict_class_counts:
        for subkey in key.split():
            logging.debug('Subkey:\t%s', set([subkey]))
            subkey_class = True
            for cl in distinct_classes:
                if set([subkey]).issubset(set(cl)):
                    pass  # do nothing
                else:
                    logging.debug('%s is no subset of %s', set([subkey]), set(cl))
                    subkey_class = False
                    break
            if subkey_class and subkey not in superclasses:
                logging.debug('Setting base_case to True!')
                base_case = True
                superclasses.append(subkey)
    weights = [0 for superclass in superclasses]
    weights_indexes = [[0] for superclass in superclasses]
    if base_case:
        logging.info('Found pattern on first hierarchy level!')
        construct_result(pattern, {}, weights, weights_indexes, superclasses, dict_class_counts)
        write_to_csv(pattern, distinct_classes, weights, superclasses, weights_indexes,
                     get_distribution(supporting_items), True, results_dir)
    return base_case


def evaluate(distinct_classes, dict_class_counts, pattern, supporting_items, results_dir):
    """
    Find the common superclasses for a pattern and builds up a relative class hierarchy in a dictionary to do this.
    :param distinct_classes: the distinct class combinations used to build up the relative hierarchy
    :param dict_class_counts: the count of each distinct class combinations to weight the superclass
    :param pattern: the pattern to evaluate
    :param supporting_items: the classes to which the supporting items belong to
    :param results_dir: the directory to store the results
    :return:
    """
    logging.debug('Evaluating pattern:\t%s', pattern)
    logging.debug('Class counts keys:\t%s', dict_class_counts.keys())
    logging.debug('Distinct classes:\t%s', distinct_classes)
    level_0_superclasses, too_new, modelling_error, modelling_errors_list, too_new_list = get_superclasses(
        list(dict_class_counts.keys()))
    global too_new_patterns
    global modeling_errors
    # log too new patterns and patterns with modelling error and process only normal patterns
    if too_new:
        logging.warning('Pattern %s was reported as too new!', pattern)
        too_new_patterns[pattern] = too_new_list
        return
    elif modelling_error:
        logging.error('Pattern %s was reported as modelling error!', pattern)
        modeling_errors[pattern] = modelling_errors_list
        return
    logging.info('Initial pre superclasses:\t%s', level_0_superclasses)
    hierarchy_levels_merged = dict()
    for distinct_class in distinct_classes:
        dict_key = ' '.join(distinct_class)
        logging.debug('Constructed Key for merged hierarchy:\t%s', dict_key)
        merged_superclasses = list()
        for c in distinct_class:
            merged_superclasses.extend(level_0_superclasses[c])
        merged_superclasses = sorted(list(set(merged_superclasses)), key=lambda x: int(x[1:]))
        logging.debug('Merged Superclasses for "%s":\t%s', dict_key, merged_superclasses)
        hierarchy_levels_merged[dict_key] = merged_superclasses
    logging.info('Merged hierarchy:\t%s', hierarchy_levels_merged)
    logging.debug('Constructing the hierarchy-dict now')
    hierarchy = {key: [hierarchy_levels_merged[key].copy()] for key in hierarchy_levels_merged}
    logging.debug('Initial Hierarchy:\t%s', hierarchy)
    logging.debug('Initial Hierarchy values:\t%s', list(hierarchy.values()))
    logging.debug('Checking for superclasses on initial level')
    common_superclasses = intersect_classes(hierarchy_levels_merged.values())
    hierarchy_level = 0
    while len(common_superclasses) == 0:
        for key in hierarchy:
            logging.debug('Getting superclasses for:\t%s', hierarchy[key][hierarchy_level])
            next_superclasses, too_new, _, _, _ = get_superclasses(hierarchy[key][hierarchy_level])
            if too_new:
                too_new_patterns[pattern] = []
                logging.info(
                    'At least one class we received is too new. This can only be the case if it is retrieved via SPARQL instance relation.')
                return
            next_superclasses = sorted(list(set(itertools.chain(*next_superclasses.values()))),
                                       key=lambda x: int(x[1:]))
            hierarchy[key].append(next_superclasses)
            logging.debug('Merging new superclasses to merged hierarchy')
            hierarchy_levels_merged[key].extend(next_superclasses)
            hierarchy_levels_merged[key] = sorted(list(set(hierarchy_levels_merged[key])), key=lambda x: int(x[1:]))
        hierarchy_level += 1
        if hierarchy_level == 50:
            global skipped_patterns
            skipped_patterns[pattern] = hierarchy_level
            return
        logging.debug('Expanded Hierarchy dict:\t%s', hierarchy)
        logging.debug('Extended merged hierarchy dict:\t%s', hierarchy_levels_merged)
        logging.info('Increased Hierarchy level to:\t%s', hierarchy_level)
        logging.debug('Checking for superclasses now')
        common_superclasses = intersect_classes(hierarchy_levels_merged.values())
    logging.info('Found superclass for pattern:\t%s', pattern)
    superclass_indices = get_indices(common_superclasses, hierarchy.values())
    weights = get_weights(dict_class_counts, superclass_indices)
    construct_result(pattern, hierarchy, weights, superclass_indices, common_superclasses, dict_class_counts)
    write_to_csv(pattern, distinct_classes, weights, common_superclasses, superclass_indices,
                 get_distribution(supporting_items), False, results_dir)


def get_indices(intersect_results, hierarchy_values):
    """
    Finds the indice levels of the common superclasses per distinct initial class combination
    :param intersect_results: list of common superclasses
    :param hierarchy_values: the relative class hierarchy of a pattern
    :return:
    """
    logging.debug('Analyzing:\t%s', intersect_results)
    logging.debug('tGot Hierarchy values:\t%s', hierarchy_values)
    indices = list()
    for res in intersect_results:
        res_indices = list()
        for class_hierarchy in hierarchy_values:
            for i in range(0, len(class_hierarchy)):
                if res in class_hierarchy[i]:
                    res_indices.append(i)
                    # leave the class hierarchy at first hit and search in the next hierarchy item
                    break
        indices.append(res_indices)
    logging.debug('Found indices:\t%s', indices)
    return indices


def get_weights(dict_class_counts, superclass_indices):
    """
    Computes the weight values to decide which common superclass is the best one if there are more than one.
    :param dict_class_counts: count per distinct class combinations
    :param superclass_indices: indices of hierarchy level where the common superclass is located
    :return: weight values
    """
    logging.debug('Evaluating weight of indices:\t%s', superclass_indices)
    logging.debug('Class counts:\t%s', dict_class_counts)
    weights = [sum([dict_value * index for dict_value, index in zip(dict_class_counts.values(), superclass_index)]) for
               superclass_index in superclass_indices]
    logging.debug('Calculated the following weights:\t%s', weights)
    return weights


def get_distribution(supporting_items):
    """
    Checks how much percent of the items are classes in the Wikidata class hierarchy.
    :param supporting_items: list of items to check
    :return: class percentage of the list
    """
    sup = len(supporting_items)
    global P31_class_hierarchy
    global max_p31_class
    global max_item_class
    global P279_class_hierarchy
    is_class = dict()
    for item in supporting_items:
        query_item = int(item[1:])
        # print(fucking_fancy_array[query_item])
        # if one of the both queries is not None, we have a class! if both are None, we have an Wikidata Item at lowest depth
        if query_item <= max_p31_class and query_item <= max_item_class and (
                P279_class_hierarchy[query_item] is not None or P31_class_hierarchy[query_item] is not None):
            logging.debug('Item %s is a subclass of %s', item, P279_class_hierarchy[query_item])
            is_class[item] = True
        else:
            is_class[item] = False
    class_count = sum(1 for value in is_class.values() if value)
    logging.info('Class count:\t%s', class_count)
    class_percentage = class_count / sup
    logging.info('Class distribution:\t%s', class_percentage)
    return class_percentage


def intersect_classes(superclass_list):
    """
    Intersects a list to find a common superclass.
    :param superclass_list: list of class hierarchy items per initial distinct class combination.
    :return: intersect result
    """
    logging.debug('Merged hierarchy:\t%s', superclass_list)
    intersect_result = set.intersection(*map(set, superclass_list))
    logging.debug('Intersect result:\t%s', set.intersection(*map(set, superclass_list)))
    return list(intersect_result)


def get_classes(pattern_path, results_dir):
    """
    Reads the classes of a pattern file and initiates the analysis of it.
    :param pattern_path: the path of the pattern
    :param results_dir: the result directory to store results
    :return:
    """
    file = open(pattern_path, 'r')
    pattern = pattern_path.split('\\')[-1:][0].split('.')[0].replace('_', ' ')
    support = int(pattern_path.split('_')[-1].split('.')[0][1:-1])
    logging.info('Pattern:\t%s', pattern)
    logging.debug("Support:\t%s", support)
    pattern_dict = json.load(file)
    pattern_dict_values = list(pattern_dict.values())
    list_classes = list()
    distinct_classes = sorted([list(x) for x in set(tuple(x) for x in pattern_dict_values)])
    logging.info('Distinct classes Length:\t%s', len(distinct_classes))
    logging.debug('Distinct classes:\t%s', distinct_classes)
    for values in pattern_dict_values:
        for value in values:
            list_classes.append(value)
    logging.debug('Dict:\t%s', pattern_dict)
    logging.debug('Dict values:\t%s', pattern_dict_values)
    logging.debug('Classes List (Length: %s):\t%s', len(list_classes), list_classes)
    dict_class_counts = dict()
    for distinct_class_combination in distinct_classes:
        dict_class_counts[' '.join(distinct_class_combination)] = pattern_dict_values.count(distinct_class_combination)
        logging.debug('Counting item "%s":\t%s', ' '.join(distinct_class_combination),
                      dict_class_counts[' '.join(distinct_class_combination)])
    logging.debug('Class counts:\t%s', dict_class_counts)
    logging.info('Distinct value sets:\t%s', distinct_classes)
    global all_results
    global too_new_patterns
    global modeling_errors
    global skipped_patterns
    if pattern not in all_results and pattern not in modeling_errors and pattern not in too_new_patterns and pattern not in skipped_patterns and not perfect_pattern(
            dict_class_counts, distinct_classes, pattern, list(pattern_dict.keys()), results_dir):
        evaluate(distinct_classes, dict_class_counts, pattern, list(pattern_dict.keys()), results_dir)
    file.close()


def get_superclasses(items):
    """
    Generates a list of superclasses for the provided Wikidata items and provides list and booleans of modeling errors
    or too new Wikidata items which cannot be queried with the state of dump.
    :param items: list of Wikidata items
    :return: dictionary with superclasses per item of the items param, a bool if an item is too new, a bool if an item
    is a modeling error, a list of too new items, a list of modeling error items
    """
    logging.debug('Getting superclass for item:\t%s', items)
    splitted_items = [item.split() for item in items]
    logging.debug('Splitted items:\t%s', splitted_items)
    query_items = [int(item[1:]) for sublist in splitted_items for item in sublist]
    global P279_class_hierarchy
    global max_item_class
    modelling_error = False
    too_new = False
    too_new_list = list()
    sparql_query_items = list()
    localquery_items = list()
    for query_item in query_items:
        if query_item > max_item_class:
            too_new = True
            too_new_list.append(query_item)
        elif P279_class_hierarchy[query_item] is None:
            sparql_query_items.append(query_item)
            logging.debug('Trying to get instance from SPARQL, item Q%s has no superclass!', query_item)
        else:
            localquery_items.append(query_item)
    sparql_query_results = dict()
    modelling_errors_list = list()
    if len(sparql_query_items) > 0:
        logging.debug('Trying to get a instances for items which have no further superclass:\t%s', sparql_query_items)
        sparql_query_results = {'Q' + str(sparql_item): search_for_instance('Q' + str(sparql_item)) for sparql_item in
                                sparql_query_items}
        if [] in sparql_query_results.values():
            logging.error('A item with no superclass and no further instance was found, modelling error!')
            modelling_errors_list = [key for key in sparql_query_results.keys() if sparql_query_results[key] == []]
            modelling_error = True
    logging.debug('Getting hierarchy!')
    d_results = sparql_query_results | {
        'Q' + str(query_item): ['Q' + str({0}).format(element) for element in sorted(P279_class_hierarchy[query_item])]
        for query_item in localquery_items}
    logging.debug('Finished getting hierarchy')
    logging.debug('Results:\t%s', d_results)
    return d_results, too_new, modelling_error, modelling_errors_list, too_new_list


def search_for_instance(item):
    """
    Performs a sparql query to find a class via the P31 relation.
    Used only if no further P279 relation can be found.
    :param item: Wikidata item used in the query
    :return:
    """
    global instance_requests
    if item not in instance_requests:
        for attempt in range(5):
            headers = None
            try:
                global requests
                requests += 1
                logging.info('Getting Superclass via SPARQL with P31 relation for item:\t%s', item)
                endpoint_url = 'https://query.wikidata.org/sparql'
                sparql = SPARQLWrapper(endpoint_url,
                                       agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36 Edg/97.0.1072.69')
                sparql.setReturnFormat(JSON)
                ############# x
                sparql.setQuery('SELECT ?instance  WHERE { wd:' + item + ' wdt:P31 ?instance . }')
                logging.debug('Performing query now.')
                query = sparql.query()
                headers = query.response.headers
                results = query.convert()
                logging.debug("Received results!")
                logging.debug("Normalizing .json Output to a pandas dataframe")
                instances = []
                if results['results']['bindings'] != []:
                    list_results = pd.json_normalize(results['results']['bindings'])['instance.value'].to_list()
                    re_wd = re.compile('http:\/\/www\.wikidata\.org\/entity\/')
                    instances = [res.replace(re.findall(re_wd, res)[0], '').upper() for res in list_results]
                    logging.debug('Removed IRI from results')
                    instance_requests[item] = instances
                    return instances
                else:
                    instance_requests[item] = instances
                    return instances
            except Exception as e:
                logging.error('Exception:\t%s', traceback.format_exception(*sys.exc_info()))
                logging.error('Header keys:\t%s', headers.keys())
                logging.error('Header values:\t%s', headers.values())
                if 'Retry-After' in headers.keys():
                    logging.info("Found 'Retry-After' in header:\t%s", headers['Retry-After'])
                    secs = 60 + int(headers['Retry-After'])
                    time.sleep(secs)
                else:
                    logging.info("Could not find'Retry-After' in header, sleeping 5 minutes")
                    time.sleep(20)
    else:
        logging.debug('Already found instance!')
        return instance_requests[item]


def clean_results_directory(dir):
    """
    Generates empty .json files used to load the current state of analysis
    :param dir:  directory to generate files in
    :return:
    """
    d = dict()
    res_file = open(dir + "\\results.json", 'w')
    json.dump(d, res_file)
    too_new_file = open(dir + "\\too_new_patterns.json", 'w')
    json.dump(d, too_new_file)
    modeling_errors = open(dir + "\\modeling_errors.json", 'w')
    json.dump(d, modeling_errors)
    skipped_file = open(dir + "\\skipped_patterns.json", 'w')
    json.dump(d, skipped_file)
    skipped_file.close()
    res_file.close()
    too_new_file.close()
    modeling_errors.close()


if __name__ == '__main__':
    """
    The main method reads paths and makes sure they exist before calculations start.
    Also one can cleanup or generate empty .json file used to load the current state of analysis.
    """
    logging.basicConfig(format='%(asctime)s - %(funcName)15s:%(lineno)s - %(levelname)s:\t%(message)s',
                        datefmt='%H:%M:%S', level=logging.INFO)
    results_dir = input("Enter the path the results directory (Example: C:\dump\\analysis\\results):\t")
    results_dir = results_dir.replace('"', '').replace("'", "")
    assert os.path.exists(results_dir), "Path not found at:\t" + str(results_dir)
    cleanup_dir = input("Do you want to create analysis files or clean them up to default state? (y/n):\t")
    cleanup_dir = cleanup_dir.upper()
    if cleanup_dir == 'Y':
        clean_results_directory(results_dir)
    class_memberships_dir = input(
        "Enter the path of the class memberships directory (Example: C:\dump\\analysis\class_memberships):\t")
    class_memberships_dir = class_memberships_dir.replace('"', '').replace("'", "")
    assert os.path.exists(class_memberships_dir), "Path not found at:\t" + str(class_memberships_dir)
    P279_class_hierarchy_file = input(
        "Enter the path of the P279_class_hierarchy (Example: C:\dump\\analysis\class_hierarchy\class_hierarchy_P279.json):\t")
    P279_class_hierarchy_file = P279_class_hierarchy_file.replace('"', '').replace("'", "")
    assert os.path.exists(P279_class_hierarchy_file), "Path not found at:\t" + str(P279_class_hierarchy_file)
    P31_class_hierarchy_file = input(
        "Enter the path of the P31_class_hierarchy (Example: C:\dump\\analysis\class_hierarchy\class_hierarchy_P31.json):\t")
    P31_class_hierarchy_file = P31_class_hierarchy_file.replace('"', '').replace("'", "")
    assert os.path.exists(P31_class_hierarchy_file), "Path not found at:\t" + str(P31_class_hierarchy_file)
    logging.info('Analysis of patterns started')
    files = get_files(class_memberships_dir)
    logging.info('Files Length:\t%s', len(files))
    logging.info("Loading current State")
    load_current_state(results_dir)
    logging.info("Loading P279 Class Hierarchy")
    load_hierarchy(P279_class_hierarchy_file)
    logging.info("Loading classes which are P31-objects")
    load_classes_via_P31(P31_class_hierarchy_file)
    do_work(files, results_dir)
