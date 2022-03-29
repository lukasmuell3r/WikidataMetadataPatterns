import bz2
import json
import os
import re
from ast import literal_eval

grep_property_line = re.compile('(^<wd:[Qq][^>]+>\s<p:[Pp][^>]+>\s<wds:[Qq][^>]+>)')
grep_wds = re.compile('((?<=wds:)[Qq][^>]+)')
grep_property = re.compile('((?<=p:)[Pp][^>]+)')
grep_qualifier = re.compile('((?<=pq:)[Pp][^>]+)')
grep_wditem = re.compile('^<wds?:([Qq][^->]+)')
match_instance = re.compile('((?<=p:)[Pp]31(?=>))')


def create_transaction(item_cache, temp_wditem, dict_all_items, file_wd_transaction, file_wd_item):
    """
    Creates the transaction string for the transaction database
    :param item_cache: list of strings to construct a transaction
    :param temp_wditem: a temporary variable to know about which item the transaction is about
    :param dict_all_items: the item database
    :param file_wd_transaction: the output file for the transaction database
    :param file_wd_item: the outputfile for the Wikidata items corresponding to the transaction database
    :return:
    """
    temp_prop = ''
    final_transaction = ''
    temp_wds = ''
    qualifier_set = set()
    transaction = []
    has_instance = False
    for item_line in item_cache:
        if len(re.findall(match_instance, item_line)) == 1:
            has_instance = True
        if re.findall(grep_wds, item_line)[0].upper() == temp_wds:
            if len(re.findall(grep_qualifier, item_line)) == 1:
                qualifier = re.findall(grep_qualifier, item_line)[0].upper()
                qualifier_set.add(qualifier)
            else:
                pass
                # print("Line not needed, ps: or <wd:Q> <p:P> <wds:Q> handled in next operation!")
        else:
            dict_tuple = tuple([temp_prop] + list(sorted(qualifier_set)))
            if dict_tuple in dict_all_items.keys() and len(dict_tuple) >= 2:
                if transaction.count(dict_all_items[dict_tuple]) == 0:
                    transaction.append(dict_all_items[dict_tuple])
                final_transaction = " ".join(list(map(str, sorted(transaction))))
                qualifier_set = set()
            else:
                qualifier_set = set()
        if len(re.findall(grep_property_line, item_line)) == 1:
            if len(re.findall(grep_property, item_line)) == 1 and len(re.findall(grep_wds, item_line)) == 1:
                temp_prop = re.findall(grep_property, item_line)[0].upper()
                temp_wds = re.findall(grep_wds, item_line)[0].upper()
    if len(qualifier_set) != 0:
        dict_tuple = tuple([temp_prop] + list(sorted(qualifier_set)))
        if dict_tuple in dict_all_items.keys() and len(dict_tuple) >= 2:
            if transaction.count(dict_all_items[dict_tuple]) == 0:
                transaction.append(dict_all_items[dict_tuple])
            final_transaction = " ".join(list(map(str, sorted(transaction))))
    if temp_wditem != '' and final_transaction != '' and has_instance is True:
        file_wd_transaction.write(final_transaction)
        file_wd_item.write(temp_wditem + "\n")
        file_wd_transaction.write("\n")


def create_horizontal_database(files, item_db_file_path, transaction_db_path):
    """
    Create the horizontal transaction database for frequent itemset mining.
    :param files: the files to process.
    :param item_db_file_path: the path to the item database.
    :param transaction_db_path: the path to where the transaction database will be stored.
    :return:
    """
    file_wd_item = open(transaction_db_path + "\\tid.txt", "w")
    file_wd_transaction = open(transaction_db_path + "\\transaction.dat", "w")
    with open(item_db_file_path, "r") as filehandle:
        obj = json.load(filehandle)
    dict_all_items = {literal_eval(k): v for k, v in obj.items()}
    item_cache = []
    temp_wditem = ''
    for file in files:
        print("File:\t", file)
        stream = bz2.open(file, 'rt')
        for line in stream:
            if len(re.findall(grep_wditem, line)) == 1:
                wditem = re.findall(grep_wditem, line)[0].upper()
                if re.findall(grep_wditem, line)[0].upper() == temp_wditem:
                    item_cache.append(line)
                else:
                    create_transaction(item_cache, temp_wditem, dict_all_items, file_wd_transaction, file_wd_item)
                    item_cache = []
                    temp_wditem = wditem
                    item_cache.append(line)
            else:
                if len(re.findall('<wds?:[Pp]', line)[0]) > 0:
                    pass
                else:
                    print("Line:\t", line)
                    print("Error in dump! No WD-Item could be found!")
        stream.close()
    print("Last item cache length:\t", len(item_cache))
    create_transaction(item_cache, temp_wditem, dict_all_items, file_wd_transaction, file_wd_item)
    print("Finished creating the transaction database")


if __name__ == '__main__':
    """
    The main method reads paths and makes sure they exist before calculations start.
    """
    input_path = input("Enter the directory of the cleaned and splitted .nt.bz2 dump (Example: C:\dump\cleaned_dump):\t")
    input_path = input_path.replace('"', '').replace("'", "")
    assert os.path.exists(input_path), "Path not found at:\t" + str(input_path)
    item_db_file_path = input("Enter the path to the item database (Example: C:\dump\itemdb\items.json):\t")
    item_db_file_path = item_db_file_path.replace('"', '').replace("'", "")
    assert os.path.exists(item_db_file_path), "File not found at:\t" + str(item_db_file_path)
    transaction_db_path = input(
        "Enter the directory to store the transaction database (Example: C:\dump\\transactiondb):\t")
    transaction_db_path = transaction_db_path.replace('"', '').replace("'", "")
    assert os.path.exists(transaction_db_path), "File not found at:\t" + str(transaction_db_path)
    # get filelist of provided path
    file_list = next(os.walk(input_path))[2]
    file_list_fullpath = []
    for file in file_list:
        file_list_fullpath.append(os.path.join(input_path, file))
    create_horizontal_database(file_list_fullpath, item_db_file_path, transaction_db_path)
