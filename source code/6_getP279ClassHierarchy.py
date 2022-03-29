import bz2
import json
import os
import re


def get_class_hierarchy(dump_file, P279_class_hierarchy_file):
    """
    Extracts the complete P279 class hierarchy from the Wikidata dump.
    Is used to find a common superclass of metadata patterns.
    :param dump_file: the file to the .nt.bz2 dump
    :param P279_class_hierarchy_file: the output file for the P279 class hierarchy
    :return:
    """
    re_class_line = re.compile(
        '^<http:\/\/www\.wikidata\.org\/entity\/[Qq][^>]+> <http:\/\/www\.wikidata\.org\/prop\/direct\/[Pp]279> <http:\/\/www\.wikidata\.org\/entity\/[^>]+> \.')
    re_instance = re.compile('(?<=^<http:\/\/www\.wikidata\.org\/entity\/)[Qq]([^>]+)')
    re_superclass = re.compile('(?<=<http:\/\/www\.wikidata\.org\/entity\/)[Qq]([^>]+)> \.$')
    hierarchy = dict()
    stream = bz2.open(dump_file, 'rt')
    counter = 0
    out_file = open(P279_class_hierarchy_file, 'w')
    for line in stream:
        counter += 1
        if counter % 10000000 == 0 and counter != 0:
            print("Processed 10 Million lines.")
            print("Class-counts:\t", len(hierarchy.keys()))
        if len(re.findall(re_class_line, line)) == 1:
            instance = (re.findall(re_instance, line)[0])
            superclass = (re.findall(re_superclass, line)[0])
            if instance not in hierarchy:
                hierarchy[instance] = list()
                hierarchy[instance].append(superclass)
            else:
                hierarchy[instance].append(superclass)

    json.dump(hierarchy, out_file)
    out_file.close()
    print("Finished!")


if __name__ == '__main__':
    """
    The main method reads paths and makes sure they exist before calculations start.
    """
    dump_file = input("Enter the path the .nt.bz2 dump (Example: C:\dump\latest-all.nt.bz2):\t")
    dump_file = dump_file.replace('"', '').replace("'", "")
    assert os.path.exists(dump_file), "File not found at:\t" + str(dump_file)
    P279_class_hierarchy_file = input(
        "Enter the path to store the P279 class hierarchy .json (Example: C:\dump\\analysis\class_hierarchy\class_hierarchy_P279.json):\t")
    P279_class_hierarchy_file = P279_class_hierarchy_file.replace('"', '').replace("'", "")
    class_hierarchy_dir = os.path.dirname(P279_class_hierarchy_file)
    assert os.path.exists(class_hierarchy_dir), "Path not found at:\t" + str(class_hierarchy_dir)
    get_class_hierarchy(dump_file, P279_class_hierarchy_file)
