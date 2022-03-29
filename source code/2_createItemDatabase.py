import bz2
import csv
import json
import os
import re

# compile for efficient use
grep_property_line = re.compile(
    '(^<wd:[Qq][^>]+>\s<p:[Pp](?!31>|279>)[^>]+>\s<wds:[Qq][^>]+>)')  # to cache property p and wds node name to find all qualifiers, do not cache P31 or P279 lines, P31*** or P279*** will be matched :).
grep_wds = re.compile(
    '((?<=wds:)[Qq][^>]+)')  # we only want wds-Nodes from Wikidata Items, not from Wikidata Propertys like wds:P* as Subject
grep_property = re.compile('((?<=p:)[Pp][^>]+)')
grep_qualifier = re.compile('((?<=pq:)[Pp][^>]+)')
grep_wditem = re.compile('^<wds?:([Qq][^->]+)')


def extractinformation(files, item_db_file_path, ext_ident_path):
    """
    Constructs the item database for frequent itemset mining utilizing regexes.
    :param files: The files to process.
    :param item_db_file_path: The path to where the item database will be stored.
    :param ext_ident_path: The path to the external identifiers file.
    :return:
    """
    # reading external identifiers
    ext_identifiers = []
    with open(ext_ident_path, newline='') as external_identifier_csv:
        for row in csv.reader(external_identifier_csv):
            ext_identifiers.append(row[0])
    temp_prop = ''
    temp_wds = ''
    qualifier_set = set()
    item_dict = dict()
    item_counter = 0
    fileindex = 0
    for filename in files:
        print("File:\t", filename)
        stream = bz2.open(filename, 'rt')
        for line in stream:
            if len(re.findall(grep_wds, line)) != 0 and re.findall(grep_wds, line)[0].upper() == temp_wds:
                if len(re.findall(grep_qualifier, line)) == 1:
                    qualifier = re.findall(grep_qualifier, line)[0].upper()
                    qualifier_set.add(qualifier)
                else:
                    pass
                    # print("Line not needed, ps: or <wd:Q> <p:P> <wds:Q> handled in next operation!")
            else:
                # Use the FIMI as key, value as FIMI Counter, check if there is at least one qualifier
                fimi_tuple = tuple([temp_prop] + list(sorted(qualifier_set)))
                # properties P3921 and P4316 contain sparql queries as values and destroy the database format, they are skipped
                if len(fimi_tuple) < 2 or fimi_tuple in item_dict.keys() or ext_identifiers.count(
                        temp_prop) == 1 or temp_prop == 'P3921' or temp_prop == 'P4316':
                    pass
                    # print("Item of previous statement already present or is too short or external identifier, skipping item")
                else:
                    item_dict[fimi_tuple] = item_counter
                    item_counter += 1
                qualifier_set = set()
            if len(re.findall(grep_property_line, line)) == 1:
                if len(re.findall(grep_property, line)) == 1 and len(re.findall(grep_wds, line)) == 1:
                    temp_prop = re.findall(grep_property, line)[0].upper()
                    temp_wds = re.findall(grep_wds, line)[0].upper()
        stream.close()
        with open(item_db_file_path + '\\' + f'{fileindex:04}' + '-items.json', 'w') as file:
            file.write(json.dumps({str(k): v for k, v in item_dict.items()}))
            file.close()
        fileindex += 1

    fimi_tuple = tuple([temp_prop] + list(sorted(qualifier_set)))
    if len(fimi_tuple) < 2 or fimi_tuple in item_dict.keys() or ext_identifiers.count(
            temp_prop) == 1 or temp_prop == 'P3921' or temp_prop == 'P4316':
        pass
        # print("Item of previous statement already present or is too short, skipping this item now")
    else:
        item_dict[fimi_tuple] = item_counter
        item_counter += 1
    print("Writing final Item Database!")
    with open(item_db_file_path + '\\items.json', 'w') as file:
        # convert keys to string before dumping
        file.write(json.dumps({str(k): v for k, v in item_dict.items()}))
        file.close()
    print("Finished creating item database!")


if __name__ == '__main__':
    """
    The main method reads paths and makes sure they exist before calculations start.
    """
    input_path = input(
        "Enter the directory of the cleaned and splitted .nt.bz2 dump directory (Example: C:\dump\cleaned_dump):\t")
    input_path = input_path.replace('"', '').replace("'", "")
    assert os.path.exists(input_path), "Path not found at:\t" + str(input_path)
    item_db_file_path = input(
        "Enter the directory to store the item databases (one each dump shard) (Example: C:\dump\itemdb):\t")
    item_db_file_path = item_db_file_path.replace('"', '').replace("'", "")
    assert os.path.exists(item_db_file_path), "File not found at:\t" + str(item_db_file_path)
    ext_ident_path = input(
        "Enter the path to the external identifiers file (Example: C:\dump\external_identifiers_optimization.csv):\t")
    ext_ident_path = ext_ident_path.replace('"', '').replace("'", "")
    assert os.path.exists(ext_ident_path), "File not found at:\t" + str(ext_ident_path)
    # get filelist of provided path
    file_list = next(os.walk(input_path))[2]
    file_list_fullpath = []
    for file in file_list:
        file_list_fullpath.append(os.path.join(input_path, file))
    # print("Files: ", file_list_fullpath)
    extractinformation(file_list_fullpath, item_db_file_path, ext_ident_path)
