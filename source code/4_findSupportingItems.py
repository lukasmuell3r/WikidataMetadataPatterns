import json
import os


def find_items(pattern_file, transaction_db, tid_db, supporting_items_dir):
    """
    For all patterns found with frequent itemset mining, their respecting items which support them are found and stored.
    :param pattern_file: output file from frequent itemset mining containing all metadata patterns
    :param transaction_db: the transaction database
    :param tid_db: the Wikidata item names for the transaction database
    :param supporting_items_dir: the directory to store the results
    :return:
    """
    patterns = open(file=pattern_file, mode='r')
    print("Converting Dist-Eclat patterns to sets, not containing the support count.")
    d_patterns = dict()
    for pattern in patterns:
        d_patterns[frozenset(set(pattern.split()[:-1]))] = []
    print("Finished converting Dist-Eclat patterns.")
    print("Starting to find TIDs which are supporting the patterns")
    transactions = open(file=transaction_db, mode='r')
    tids = open(file=tid_db, mode='r')
    line_counter = 1
    for tid, transaction in zip(tids, transactions):
        tid = tid[:-1]
        transaction = transaction[:-1].split()
        for key in d_patterns:
            if key.issubset(set(transaction)):
                d_patterns.get(key).append(tid)
        line_counter += 1
    d_export = dict()
    for key in d_patterns:
        d_export[' '.join(sorted(key)) + " (" + str(len(d_patterns.get(key))) + ")"] = d_patterns.get(key)
    export(d_export, supporting_items_dir)


def export(object, supporting_items_dir):
    """
    Results will be saved to disk.
    :param object: the object to save
    :param supporting_items_dir: the path for saving the object
    :return:
    """
    with open(supporting_items_dir + "\\supportingItemsPerPattern.json", 'w') as filehandle:
        json.dump(object, filehandle)


if __name__ == '__main__':
    """
    The main method reads paths and makes sure they exist before calculations start.
    """
    dist_eclat_out = input("Enter the path of the output of Dist-Eclat (Example: C:\dump\dist-eclat\out.txt):\t")
    dist_eclat_out = dist_eclat_out.replace('"', '').replace("'", "")
    assert os.path.exists(dist_eclat_out), "Path not found at:\t" + str(dist_eclat_out)
    transaction_dat_file = input(
        "Enter the path of transaction.dat (Example: C:\dump\\transactiondb\\transaction.dat):\t")
    transaction_dat_file = transaction_dat_file.replace('"', '').replace("'", "")
    assert os.path.exists(transaction_dat_file), "File not found at:\t" + str(transaction_dat_file)
    tid_file = input("Enter the path of tid.txt (Example: C:\dump\\transactiondb\\tid.txt):\t")
    tid_file = tid_file.replace('"', '').replace("'", "")
    assert os.path.exists(tid_file), "File not found at:\t" + str(tid_file)
    supporting_items_dir = input(
        "Enter the output directory for supportingItemsPerPattern.json (Example: C:\dump\\analysis):\t")
    supporting_items_dir = supporting_items_dir.replace('"', '').replace("'", "")
    assert os.path.exists(supporting_items_dir), "File not found at:\t" + str(supporting_items_dir)
    find_items(dist_eclat_out, transaction_dat_file, tid_file, supporting_items_dir)
