(Simple) Python Media Crawler
==============================
Crawler starts with provided starting url and is searching for links with simple regular expression.  It is faster than parsing with library but not as precise as using dedicated parsing library (I recommend lxml for production use). 

Crawler analyzes http response header and makes decision based on content-type header. Supported media files are then analyzed with python Mutagen library (it is considered as best audio metadata python lib but is based on GPL license).

Crawler is using simple SQLite database for handling queue data and crawling history. It does not check if it leaves current domain as I didn't know if it is needed.

How to use:
-------------------------------
python media_crawler.py site_url output.csv
Optional arguments:

    -v --verbose                 :    Prints some verbose data
    -d --depth max_crawl_depth   :    Sets max crawling depth
    -b --database file_name      :    SQLite database filename

output.csv will contain metadata saved as csv

Prereqs:
----------
Included in repo so you don't have to manually download it. Just for informational purposes.
- http://code.google.com/p/mutagen/ - Lib for getting metadata from media files

Future improvements
----------------------
- multithreading, so crawler can crawl site and analyze files simultaneously
- do not download file, analyze header only if this is sufficient (analyze mutagen lib source or find another lib)
- more advanced tests
- extract links not based on regular expressions, use lxml 

Test
-------
All files included in test suite are available on CC license (Free).