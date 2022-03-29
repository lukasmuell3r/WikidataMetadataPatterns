
# Source code
This folder contains the source code in the form of Python scripts. The order of execution of the scripts is marked with the leading number.

The following folder structure is recommended for processing:
```
.
├──     latest_all.nt.bz2 (Wikidata dump)
├──     external_identifiers_optimization.csv
├──     cleaned_dump\
├──     itemdb\
├──     transactiondb\
├──     dist-eclat\
└──     analysis\
        ├──     class_memberships\
        ├──     class_hierarchy\
        ├──     results\
        └──     class_memberships_corrected_errors\
```

Prerequisites for the analysis are the Wikidata dump in `.nt.bz2` format and the external identifiers as `.csv` file. The scripts were executed with `Python 3.9` on `Windows 11`. For an execution under Linux the path structure must be adapted somewhat.

### Python-Packages
```
- bz2
- os
- re
- csv
- json
- ast
- logging
- numpy
- pandas
- SPARQLWrapper
- itertools
- time
- sys
- traceback
```

## cleanAndSplitDump
**Input**
```
- latest_all.nt.bz2
- output directory path
```
**Output**
```
- 10 million lines long .nt.bz2 archives
```
**Summary**
Cleans up the `.nt.bz2` dump by omitting irrelevant information and replaces IRIs with prefixes. This generates part files that are each 10 million lines long. These files can now be opened in programs such as `Notepad++`.

## createItemDatabase
**Input**
```
- directory of splitted and cleaned .nt.bz2 dump
- external_identifiers_optimization.csv
- output directory path
```
**Output**
```
- items.json
```
**Summary**
Generates the FIM item database based on the cleaned wikidata dump and respecting the external identifiers.


## createTransactionDatabase
**Input**
```
- directory of splitted and cleaned .nt.bz2 dump
- items.json
- output directory
```
**Output**
```
- transaction.dat
- tid.txt
```
**Summary**
Generates the transaction database based on the cleaned wikidata dump and the FIM item database.

## Dist-Eclat [external]
**Input**
```
- transaction.dat
```
**Output**
```
- out.txt
```
**Summary**
External algorithm: Dist-Eclat, introduced in 2013, generates patterns based on the transaction database.
[Dist-Eclat on ResearchGate](https://www.researchgate.net/publication/261151539_Frequent_Itemset_Mining_for_Big_Data)

## findSupportingItems
**Input**
```
- out.txt
- transaction.dat
- tid.txt
- output directory
```
**Output**
```
- supportingItemsPerPattern.json
```
**Summary**
Searches with the found patterns from Dist-Eclat and the FIM transaction database for the Wikidata items that support the respective pattern and returns a `.json` with the informations.

## getClassMembership
**Input**
```
- supportingItemsPerPattern.json
- output directory
```
**Output**
```
- [patternname_(support)].json
```
**Summary**
Finds the class memberships of the items that support a pattern via SPARQL. Creates a `.json` file in the output directory for each pattern.

## getP279ClassHierarchy
**Input**
```
- latest_all.nt.bz2
- output directory
```
**Output**
```
- class_hierarchy_P279.json
```
**Summary**
Extracts the `subclass of` (P279) class hierarchy from the dump.

## getP31Objects
**Input**
```
- latest_all.nt.bz2
- output directory
```
**Output**
```
- class_hierarchy_P31.json
```
**Summary**
Extracts the `instance of` (P31) objects from the dump. This is the second part of the class hierarchy as mentioned in Chapter 4 of the thesis.

## analysis
**Input**
```
- P279 class hierarchy
- P31 class hierarchy
- class_membership directory -> [patternname_(support)].json
- output directory
```
**Output**
```
- results.json
- modeling_errors.json
- too_new_patterns.json
- skipped_patterns.json
```
**Summary**
Performs the analysis described in Chapter 4. In addition to the results (`results.json`), detected modeling errors are also output (`modeling_errors.json`). Furthermore, the output of patterns that are too new takes place if a class hierarchy is used that does not yet contain new classes (`too_new_patterns.json`). In this case the class hierarchy can be created again with a newer dump, then the evaluation of the patterns listed there is also possible. In `skipped_patterns.json` patterns are contained, for which after 50 hierarchy levels no common superclass could be found, which should not occur.

## removeModelingErrors
**Input**
```
- modeling_errors.json
- class_membership directory -> [patternname_(support)].json
- output directory
```
**Output**
```
- output directory
```
**Summary**
Removes modeling errors from patterns and places the new `.json` files in an output folder.
These files can then overwrite the faulty data in the `class_membership` directory. The analysis can then be repeated.

