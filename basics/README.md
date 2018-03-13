Contract Risk Analysis
================

Introduction
------------

The objective of the project is looking for risks in a contract. The first version is based on risk keyword search. The main steps of method is listed as follows:

1.  Define a list of risks. The contract contains all the risks before analysis. (Empty document full of risks)
2.  Read and preprocess the contract
3.  Analyse the contract text line by line against the risk list. Pop one risk item from the risk list when it has been addressed in the paragraph
4.  After going through the document, the risks remaining in the list are returned as the risks contained in the contract

Usage
-----

Use the code as below after changing directory to contract\_risk folder.

    pip install -r requirements.txt
    python3 contract_risk.py sample_contract

A jupyter notebook is also provided.

Sample output
-------------

![sample\_output](https://raw.githubusercontent.com/wenxiumatrix/ContractRisk/master/sample_output.jpg?token=AHu_BCfNFezetXNDwVeNdG0YKk_a_t25ks5arLv0wA%3D%3D)

Requirements
------------

-   Python &gt;= 3.6
-   docx2txt &gt;= 0.7
-   jieba &gt;= 0.39
-   numpy &gt;= 1.13.3
-   pandas &gt;= 0.21.1
-   python-dateutil &gt;= 2.6.1
-   pytz &gt;= 2017.3
-   six &gt;= 1.11.0

Licence
-------

© Copy right owned by Wenxiu Matrix 2018
