import json
import os


def construct_error_list(file):
    """
    Reads the modeling_errors.json from of the analysis output and creates a list of all found modeling errors.
    :param file: the modeling_errors.json file
    :return: list of distinct modeling errors
    """
    file = open(file, 'r')
    d = json.load(file)
    errors = list(d.values())
    errors_string_list = [error_item for error_list in errors for error_item in error_list]
    distinct_errors = set(errors_string_list)
    l_distinct_errors = list(distinct_errors)
    print("Errors:\t", l_distinct_errors)
    return l_distinct_errors


def correct_errors(modeling_errors_file, class_memberships_dir, class_memberships_corrected_dir, error_items):
    """
    Removes modelling errors from the class memberships of the supporting items. This step has to be performed to
    be able to find a common superclass, otherwise in some cases we cannot move up in the class hierarchy.
    :param modeling_errors_file: the modeling_errors.json file
    :param class_memberships_dir: the directory containing the patterns with their supporting items with their
     respective class memberships
    :param class_memberships_corrected_dir: the directory to store the corrected patterns without modeling errors
    :param error_items: list of distinct modeling errors
    :return:
    """
    modeling_error_patterns = open(modeling_errors_file, 'r')
    error_patterns = list(json.load(modeling_error_patterns).keys())
    modeling_error_patterns.close()
    path = next(os.walk(class_memberships_dir))[0]
    file_list = list()
    for pattern in error_patterns:
        file_list.append(path + "\\" + '_'.join(pattern.split()) + ".json")
    print("Error patterns file list:\t", file_list)
    print("Processing", len(file_list), "patterns!")
    for error_pattern in file_list:
        print(error_pattern)
        file = open(error_pattern)
        pattern = json.load(file)
        file.close()
        print('Old pattern dict length:\t', len(pattern))
        # print('Pattern items:\t', pattern.values())
        corrected_pattern = dict()
        for item_pair in list(pattern.items()):
            # print('Old superclasses pair:\t', item_pair)
            errors = set()
            for superclass in set(item_pair[1]):
                # print(superclass)
                if superclass not in error_items:
                    # print("Not in error items")
                    pass
                else:
                    print('Found error:\t', superclass)
                    errors.add(superclass)
            if len(errors) == 0:
                corrected_pattern[item_pair[0]] = item_pair[1]
            elif len(errors) == len(set(item_pair[1])):
                print('Key can be skipped completely because superclasses of it are only errors')
            else:
                new_superclasses = list(set(item_pair[1]).difference(errors))
                corrected_pattern[item_pair[0]] = new_superclasses
        print('New pattern dict length:\t', len(corrected_pattern))
        out_file = open(class_memberships_corrected_dir + "\\" + error_pattern.split('\\')[-1], 'w')
        json.dump(corrected_pattern, out_file)
        out_file.close()


if __name__ == '__main__':
    """
    The main method reads paths and makes sure they exist before calculations start.
    """
    modeling_errors_file = input(
        "Enter the path the modeling_errors.json (Example: C:\dump\\analysis\\results\modeling_errors.json):\t")
    modeling_errors_file = modeling_errors_file.replace('"', '').replace("'", "")
    assert os.path.exists(modeling_errors_file), "File not found at:\t" + str(modeling_errors_file)
    class_memberships_dir = input(
        "Enter the path of the class memberships directory (Example: C:\dump\\analysis\class_memberships):\t")
    class_memberships_dir = class_memberships_dir.replace('"', '').replace("'", "")
    assert os.path.exists(class_memberships_dir), "Path not found at:\t" + str(class_memberships_dir)
    class_memberships_corrected_dir = input(
        "Enter the path of the result directory (Example: C:\dump\\analysis\class_memberships_corrected_errors):\t")
    class_memberships_corrected_dir = class_memberships_corrected_dir.replace('"', '').replace("'", "")
    assert os.path.exists(class_memberships_corrected_dir), "Path not found at:\t" + str(
        class_memberships_corrected_dir)
    wd_error_items = construct_error_list(modeling_errors_file)
    print(wd_error_items)
    correct_errors(modeling_errors_file, class_memberships_dir, class_memberships_corrected_dir, wd_error_items)
