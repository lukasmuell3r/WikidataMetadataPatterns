import bz2
import os
import re

# wikidata prefixes for the shortening of the data
prefixes = {'http://www.wikidata.org/entity/': 'wd:',
            'http://www.wikidata.org/entity/statement/': 'wds:',
            'http://www.wikidata.org/prop/qualifier/': 'pq:',
            'http://www.wikidata.org/prop/': 'p:',
            'http://www.wikidata.org/prop/statement/': 'ps:',
            'http://www.wikidata.org/prop/statement/value-normalized/': 'psn:'}

# regular expressions to filter relevant triples
pattern_filter = '(<http:\/\/www\.wikidata\.org\/entity\/statement\/[^>]+> <http:\/\/www\.wikidata\.org\/prop\/statement\/(?!v)[^>]+> <[^>]+> \.)|(<http:\/\/www\.wikidata\.org\/entity\/statement\/[^>]+> <http:\/\/www\.wikidata\.org\/prop\/statement\/[^>]+> ".*\.)|(<http:\/\/www\.wikidata\.org\/entity\/statement\/[^\/]+> <http:\/\/www\.wikidata\.org\/prop\/qualifier\/[^\/]+> ".*\.)|(<http:\/\/www\.wikidata\.org\/entity\/statement\/[^\/]+> <http:\/\/www\.wikidata\.org\/prop\/qualifier\/[^\/]+> <.*\.)|(<http:\/\/www\.wikidata\.org\/entity\/[^>]+> <http:\/\/www\.wikidata\.org\/prop\/[^>]+> <http:\/\/www\.wikidata\.org\/entity\/statement\/.*\.)'
pattern_replace = '(http:\/\/www.wikidata.org\/)(entity\/|prop\/)(statement\/|qualifier\/|direct\/|)(value-normalized\/|)'


def work(source_file, dest_folder, dest_file):
    """
    Opens the .nt.bz2 dump as a stream for processing and calls construct_result.
    :param source_file: File Path of .bz2 dump
    :param dest_folder: Directory for computation results
    :param dest_file: File name of results
    :return:
    """
    # read sourcefile as stream
    stream = bz2.open(source_file, 'rt')
    # open a file for writing relevant lines of the source file
    construct_result(stream, dest_folder, dest_file)


def clean(line):
    """
    Cleans a line utilizing regex.
    :param line: the line to clean
    :return: cleaned line
    """
    if len(re.findall(pattern_filter, line)) == 1:
        for match in re.finditer(pattern_replace, line):
            line = line.replace(match[0], dict.get(prefixes, match[0]), 1)
    return line


def construct_result(stream, dest_folder, dest_file):
    """
    Reads the stream and constructs the result without unnecessary data.
    :param stream: the .nt.bz2 dump stream
    :param dest_folder: Directory for computation results
    :param dest_file: File name of results
    :return:
    """
    fileindex = 0
    outputline_counter = 0
    fullpath = dest_folder + "\\" + f'{fileindex:04}' + "-" + dest_file
    print("Output file:\t", fullpath)
    file = bz2.open(fullpath, 'wt')
    for line in stream:
        if outputline_counter <= 10000000:
            line_cleaned = clean(line)
            if line_cleaned != line:
                outputline_counter += 1
                file.write(line_cleaned)
        else:
            file.close()
            outputline_counter = 0
            fileindex += 1
            fullpath = dest_folder + "\\" + f'{fileindex:04}' + "-" + dest_file
            print("Output file:\t", fullpath)
            file = bz2.open(fullpath, 'wt')
    file.close()
    return


if __name__ == '__main__':
    """
    The main method reads paths and makes sure they exist before calculations start.
    """
    input_file = input("Enter the path the .nt.bz2 dump (Example: C:\dump\latest-all.nt.bz2):\t")
    input_file = input_file.replace('"', '').replace("'", "")
    assert os.path.exists(input_file), "File not found at:\t" + str(input_file)
    output_path = input("Enter the directory for the output files (Example: C:\dump\cleaned_dump):\t")
    output_path = output_path.replace('"', '').replace("'", "")
    output_file_name_base = os.path.basename(input_file)
    assert os.path.exists(output_path), "Directory not found at:\t" + str(output_path)
    work(input_file, output_path, output_file_name_base)
