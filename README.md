# CODAC POC

A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want 
to collate to starting interfacing more 
with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their 
financial details to starting reaching out to them for a new marketing push.

## Getting Started

### Executing program

* How to run the program
```
python src\main.py --dataset_one "[FILE_PATH]" --dataset_two "[FILE_PATH]" --countries "COUNTRY_1", "COUNTRY_2", ..., "COUNTRY_N"
```
Examples:
python src\main.py --dataset_one "C:\Users\Bartek\PycharmProjects\CODAC\dataset_one.csv" --dataset_two "C:\Users\Bartek\PycharmProjects\CODAC\dataset_two.csv" --countries "United Kingdom", "Poland", "France"

python src\main.py --dataset_one "C:\Users\Bartek\PycharmProjects\CODAC\dataset_one.csv" --dataset_two "C:\Users\Bartek\PycharmProjects\CODAC\dataset_two.csv" --countries "Netherlands"


## Authors

Contributors names and contact info

ex. Bartek Radoszkiewicz

## Version History

* 1.0
    * Initial Release

