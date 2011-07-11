#!/usr/bin/python2.6
"""Python crawler indexing media files (mp3, aac, ogg, wma,.. etc.).

Usage: media_crawler.py site_url output.csv
Optional arguments:

    -v --verbose                 :    Prints some verbose data
    -d --depth max_crawl_depth   :    Sets max crawling depth
    -b --database file_name      :    SQLite database filename
"""

import sys
import re
import urllib2
import urlparse
import sqlite3 as sqlite
from datetime import datetime
import tempfile
from mutagen import File
import csv
import traceback


CONNECTION_TIMEOUT = 20
SUPPORTED_CONTENT_TYPES = ['audio/mpeg', 'audio/mp3', 'audio/x-ms-wma', 'audio/ogg', 'audio/aac ']
QUEUE_ID = 0
QUEUE_PARENT = 1
QUEUE_DEPTH = 2
QUEUE_URL = 3


def create_csv_list(audio):
    '''
    Creates list for CSV writer from audio file
    @param audio:
    '''
    artist = title = album = None
    length = audio.info.length
    if 'artist' in audio:
        artist = " ".join(audio['artist'])

    if 'title' in audio:
        title = " ".join(audio['title'])
    elif 'Title' in audio:
        title = " ".join(audio['Title'])

    if 'album' in audio:
        album = " ".join(audio['album'])

    return utf_8_encoder([artist, title, album, length])


def get_file_from_response(response):
    '''
    Get response file and pack it into temporary file
    @param response: urllib repsone
    '''
    fileobj = tempfile.NamedTemporaryFile()
    fileobj.write(response.read())
    fileobj.seek(0)
    return fileobj


def utf_8_encoder(unicode_list):
    '''
    Covert list that may contain unicode data to utf8
    as csv writer doesn't handle unicode
    @param unicode_list: list with strings
    '''
    for i in xrange(len(unicode_list)):
        if isinstance(unicode_list[i], unicode):
            unicode_list[i] = unicode_list[i].encode('utf8')
    return unicode_list


class MediaCrawler(object):
    '''
    Crawler that starts from URL and searches for media files.
    Uses response header to dermine if encountered valid file type.
    '''
    # Precompile link regex expression
    linkregex = re.compile('<a.*?\shref=[\'"](.*?)[\'"].*?>', re.MULTILINE | re.DOTALL)

    def __init__(self, verbose=False):
        self._crawl_depth = -1
        self.db_name = "media_crawler.db"
        self.__crawled = []
        self.verbose = verbose
        self.__limit_depth = False
        self.__connection = self.__cursor = None
        self.__limit_depth = False
        self.files_found = 0
        self.__csv_file = self.__csv_writer = None
        self.print_messages = True

    def _set_crawl_depth(self, crawl_depth):
        self._crawl_depth = crawl_depth
        self.__limit_depth = True

    def _get_crawl_depth(self):
        return self._crawl_depth

    crawl_depth = property(_get_crawl_depth, _set_crawl_depth)

    def prepare_output(self, csv_filename):
        '''
        Create CSV file and open it for appending
        @param csv_filename: csv file to write to
        '''
        self.__csv_file = open(csv_filename, "a")
        self.__csv_writer = csv.writer(self.__csv_file)

    def prepare_db(self):
        '''
        Create SQLite database file and tables
        '''
        # Connect to the DB and create the tables if they don't exist
        self.__connection = sqlite.connect(self.db_name)
        self.__cursor = self.__connection.cursor()

        # crawl_index: holds information of the urls that have been crawled
        # url, timestamp
        # url can't be unique becouse user can force to visit url again by
        # providing already crawled start url
        self.__cursor.execute('CREATE TABLE IF NOT EXISTS crawl_index (crawlid INTEGER, parentid INTEGER, url VARCHAR(256), timestamp INTEGER )')
        # create index for urls
        self.__cursor.execute('CREATE INDEX IF NOT EXISTS crawl_index_url_idx ON crawl_index(url)')

        # queue: holds information of the urls that need to be crawled
        self.__cursor.execute('CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY, parent INTEGER, depth INTEGER, url VARCHAR(256))')
        # create index for urls
        self.__cursor.execute('CREATE INDEX IF NOT EXISTS queue_url_idx ON queue(url)')
        self.__connection.commit()

    def add_starting_url(self, url):
        '''
        Add starting URL into database
        @param url: string with url
        '''
        self.__cursor.execute("INSERT INTO queue VALUES ((?), (?), (?), (?))", (None, 0, 0, url))

    def clean_up(self):
        '''
        Clean up all stuff
        Commit everything, end connections etc.
        '''
        self.__connection.commit()
        self.__connection.close()
        self.__csv_file.close()

    def start(self, csv_file, start_url=None):
        '''
        Start crawling and write output to CSV file
        If no starting url is provided start, get last one from DB
        @param csv_file: Output CSV file
        @param start_url: string with starting URL
        '''
        self.prepare_db()
        self.prepare_output(csv_file)
        if start_url is not None:
            self.add_starting_url(start_url)
        work = True
        try:
            while work:
                # Get the first item from the queue
                self.__cursor.execute("SELECT * FROM queue LIMIT 1")
                crawling = self.__cursor.fetchone()
                if crawling is None:
                    if self.print_messages:
                        print "No URLs to fetch!"
                    work = False
                    continue
                if self.verbose:
                    print "Crawling: ", crawling[3]

                # Start crawling URL from queue
                self.crawl(crawling)
                # Crawling completed, remove the item from the queue
                self.__cursor.execute("DELETE FROM queue WHERE id = (?)", (crawling[0], ))

        except KeyboardInterrupt:
            print "User abort - exiting.."
        except Exception:
            traceback.print_exc()
        finally:
            #  Closing files etc..
            self.clean_up()
            if self.print_messages:
                print "Total new files found: ", self.files_found

    def exract_media(self, response, url):
        '''
        Extract media data from file contained in response
        @param response: urllib response
        @param url: url string
        '''
        # Create temporary file as mutagen needs to execute some
        # disk-file specific functions on it
        fileobj = get_file_from_response(response)
        audio = File(fileobj.name, easy=True)
        # Print some data for verbose mode
        if self.verbose:
            print audio
        # Write new row into CSV
        csv_list = create_csv_list(audio)
        csv_list.append(url)
        self.__csv_writer.writerow(csv_list)
        self.files_found += 1
        # Close temporary file
        fileobj.close()

    def crawl(self, crawling):
        '''
        Main method that extracts links from url
        and gets data from found media files
        @param crawling: DB object representing queue entry
        '''
        # crawler id
        cid = crawling[QUEUE_ID]
        # parent id. 0 if start url
        parent_id = crawling[QUEUE_PARENT]
        # current depth
        cur_depth = crawling[QUEUE_DEPTH]
        # crawling Url
        curl = crawling[QUEUE_URL]
        # Split the link into its sections
        url = urlparse.urlparse(curl)

        try:
            # Add the link to the already __crawled list
            self.__crawled.append(curl)
        except MemoryError:
            # If the __crawled array is too big, delete it and start over
            del self.__crawled[:]
        try:
            # Create a Request object
            request = urllib2.Request(curl)
            # Add user-agent header to the request
            request.add_header("User-Agent", "MediaCrawler")
            # Build the url opener, open the link and read it into msg
            opener = urllib2.build_opener()
            response = opener.open(request, timeout=CONNECTION_TIMEOUT)
            content_type = response.info().getheader('Content-Type')
            charset = content_type.split('charset=')
            encoding = charset[-1] if len(charset) > 1 else None
            # Put new __crawled link into the db
            self.add_to_crawl_index(cid, parent_id, curl)

            # check if is media file using header
            if any(x in content_type for x in SUPPORTED_CONTENT_TYPES):
                # extract media data
                self.exract_media(response, curl)
            else: # probably an HTML page
                # convert response to unicode and extract links from html
                html = response.read()
                if encoding is not None:
                    html = unicode(html, encoding)
                links = MediaCrawler.linkregex.findall(html)
                # add found links to queue
                self.add_to_queue(url, links, cid, cur_depth)

        except Exception:#(BadStatusLine, HTTPError, IOError):
            # skip URL if there are problems with it
            return

    def add_to_crawl_index(self, cid, parent_id, absolute_url):
        '''
        Add data into crawl_index that represents crawler history
        @param cid: crawler_id
        @param parent_id: parent entry id
        @param absolute_url: string with absolute url
        '''
        self.__cursor.execute("INSERT INTO crawl_index VALUES( (?), (?), (?), (?))", (cid, parent_id, absolute_url, datetime.now()))

    def was_crawled_or_planned(self, url):
        '''
        Check if url was already crawled
        @param url: url to be checked
        '''
        if url in self.__crawled:
            return True
        # if not in memory, check queue
        self.__cursor.execute("SELECT COUNT(*) FROM queue WHERE url=? LIMIT 1", [url])
        rows_num = self.__cursor.fetchone()[0]
        if rows_num > 0:
            return True
        # if not in queue, then check in history
        self.__cursor.execute("SELECT COUNT(*) FROM crawl_index WHERE url=? LIMIT 1", [url])
        rows_num = self.__cursor.fetchone()[0]
        if rows_num > 0:
            return True
        return False

    def add_to_queue(self, url, links, cid, cur_depth):
        '''
        Add links found at url into database queue
        @param url: Url at which links where found
        @param links: list of string with urls
        @param cid: crawler id
        @param cur_depth: crawling depth from starting url
        '''
        if not self.__limit_depth or cur_depth < self.crawl_depth:
            # Read the links and insert them into the queue
            for link in links:
                if link.startswith('/'):
                    link = 'http://' + url[1] + link
                elif link.startswith('#'):
                    continue
                elif not link.startswith('http'):
                    link = urlparse.urljoin(url.geturl(), link)

                if not self.was_crawled_or_planned(link):
                    self.__cursor.execute("INSERT INTO queue VALUES ( (?), (?), (?), (?) )", (None, cid, cur_depth + 1, link))


import getopt


def main():
    '''
    Command line for testing purposes
    '''
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvd:b:", ["help"])
    except getopt.error, msg:
        print msg
        print "for help use --help"
        sys.exit(2)

    crawler = MediaCrawler()

    if len(sys.argv) < 2:
        print __doc__
        sys.exit("Not enough arguments!")

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print __doc__
            sys.exit(0)
        elif opt in ("-v", "--verbose"):
            crawler.verbose = True
        elif opt in ("-d", "--depth"):
            crawler.crawl_depth = int(arg)
        elif opt in ("-b", "--database"):
            crawler.db_name = arg

    crawler.start(args[1], args[0])


if __name__ == "__main__":
    main()
