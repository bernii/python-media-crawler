#!/usr/bin/python2.6
'''
Created on 2011-06-11

@author: Bernard Kobos / berni
'''
import os

TEST_DIR = 'test' # directory with tests
SEP = '/' # it's url so use slash

from media_crawler import MediaCrawler
import unittest
from os.path import abspath, dirname, sep

ABS_PATH = abspath(dirname(__file__)).replace(sep, SEP)
PATH = "file://" + ABS_PATH + SEP + TEST_DIR + SEP
CSV_TEMP_NAME = "test_tmp.csv"


class MediaCrawlerTestLinks(MediaCrawler):
    '''
    Simple class that inhertis MediaCrawler and adds some
    counters that help in making tests.
    '''

    def __init__(self):
        MediaCrawler.__init__(self)
        self.count_adds = 0
        self.count_media = 0
        self.db_name = "test_tmp.db"
        self.print_messages = False

    def add_to_queue(self, url, links, cid, cur_depth):
        '''
        Count found links (how many were added to queue).
        '''
        self.count_adds += len(links)
        MediaCrawler.add_to_queue(self, url, links, cid, cur_depth)

    def exract_media(self, response, url):
        '''
        Count extracted media - don't really have to do it actually
        '''
        self.count_media += 1

    def reset(self):
        '''
        Reset counters & delete files
        '''
        self.count_adds = 0
        self.count_media = 0
        # remove files
        os.remove(self.db_name)
        os.remove(CSV_TEMP_NAME)


class KnownValues(unittest.TestCase):
    '''
    Test output for known input
    '''
    known_nums = ( # (filename, number of links, number of media files )
                    ("index.htm", 8, 6),
                    ("in1.htm", 4, 3),
                    ("in2.htm", 3, 2),
                    ("in1_1.htm", 3, 2),
                    ("in1_1_1.htm", 4, 2),
                  )

    def test_num_links(self):
        """crawler should find known link count with known input"""
        mc_mod = MediaCrawlerTestLinks()
        mc_mod.crawl_depth = 0
        for filename, num_links, _ in self.known_nums:
            mc_mod.start(CSV_TEMP_NAME, PATH + filename)
            self.assertEqual(mc_mod.count_adds, num_links)
            mc_mod.reset()

    def test_num_classified(self):
        """crawler should find known media count with known input"""
        for filename, _, num_media in self.known_nums:
            # create new crawler for each file cause we need some
            # class internal cunters to reset (they are private)
            mc_mod = MediaCrawlerTestLinks()
            mc_mod.crawl_depth = 1
            mc_mod.start(CSV_TEMP_NAME, PATH + filename)
            self.assertEqual(mc_mod.count_media, num_media)
            mc_mod.reset()

    def test_crawling(self):
        '''
        Crawler should find 22 urls 
        and 14 media files for provided test case
        '''
        test_urls = 22
        test_media_files = 14
        mc_mod = MediaCrawlerTestLinks()
        mc_mod.start(CSV_TEMP_NAME, PATH + self.known_nums[0][0])
        self.assertEqual(mc_mod.count_adds, test_urls)
        self.assertEqual(mc_mod.count_media, test_media_files)
        mc_mod.reset()


if __name__ == "__main__":
    unittest.main()
