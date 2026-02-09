# corporation-wiki
web scraping


# CSV Data for Company name

csv file containing two columns "cik_str", "title":
cik_str : unique id of each company
title : names of the companies

https://cdn.gov-cloud.ai/_ENC(4+j2JOgE1QQdq6yO427Uztql2TlqlMwKUOg5QJcVQ5XUgB/GP4/J5WLrrqWMDU3q)/bottle/limka/soda/6a3ea029-285e-4d18-b3d4-a03ca47116fe_$$_V1_sec_companies.csv

# Steps to use

1. download the data from the cdn url
2. make a account in (https://www.corporationwiki.com/)
3. run the script corpwikiscrap.py with your credentials of corporation-wiki
4. enter the path of the downloaded data(from cdn url) in the terminal when asked*(Enter path to companies CSV file:)
5. enter "y" when asked(Start scraping? (y/n):)



CorporationWiki Bulk Scraper
============================

A Python script for automated bulk scraping of company data from [CorporationWiki.com](https://corporationwiki.com/) with unlimited pagination support.


Prerequisites
-------------

1. **CorporationWiki Account**: Create a free account at [corporationwiki.com](https://www.corporationwiki.com/)
    

Setup
-----

### 1\. Clone/Download the Script`

### 2\. Install Dependencies

Plain`   pip install -r requirements.txt   `


### 3\. Configure Credentials

Create a .env file in the project root:

env
`   CORPORATIONWIKI_EMAIL=your_email@example.com  CORPORATIONWIKI_PASSWORD=your_password   `

### 5\. Download Company Data

Download the CSV file from:\[[https://cdn.gov-cloud.ai/](https://cdn.gov-cloud.ai/)_ENC(4+j2JOgE1QQdq6yO427Uztql2TlqlMwKUOg5QJcVQ5XUgB/GP4/J5WLrrqWMDU3q)/bottle/limka/soda/6a3ea029-285e-4d18-b3d4-a03ca47116fe_\_V1\_sec\_companies.csv\](https://cdn.gov-cloud.ai/\_ENC(4+j2JOgE1QQdq6yO427Uztql2TlqlMwKUOg5QJcVQ5XUgB/GP4/J5WLrrqWMDU3q)/bottle/limka/soda/6a3ea029-285e-4d18-b3d4-a03ca47116fe\_\_V1\_sec\_companies.csv)

Usage
-----

### Run the Script

`   python corpwikiscrap.py   `

### Interactive Steps:

1.  **Enter path to companies CSV file:**Provide the full path to the downloaded CSV 
    
2.  **Start scraping? (y/n):**Type y and press Enter to begin
    

### Output

*   All scraped data is saved in the corporationwiki\_output/ directory
    
*   Each company gets its own CSV file named after the company
    
*   Progress is displayed in real-time.
    

CSV File Format
---------------

The input CSV should contain at least these columns:

*   cik\_str: Unique identifier for each company
    
*   title: Company names to search for
    

Example CSV structure:

csv

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   cik_str,title  0000320193,Apple Inc.  0000789019,Microsoft Corporation  0001018724,Amazon.com Inc.   `

Data Scraped
------------

For each company, the script extracts:

*   Company name
    
*   Location
    
*   Company URL
    
*   Officers/Directors (name, URL, entity ID)
    
*   Total officers count
    
*   Pagination information
    
