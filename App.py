from selenium import webdriver
from bs4 import BeautifulSoup
import os
import pandas as pd
import time
import datetime


class  QuizApp(object):

    def __init__(self):
        while True:
             start = datetime.datetime.now()
             file_index = str(start)
             self.id = file_index
             print 'Now start to craw data...'
             self.crawl_data()
             print 'Now bag of words...'
             self.word_count()
            ## let's sleep an hour
             time.sleep(216000)


    def crawl_data(self):

        url = 'https://www.verizonwireless.com/smartphones/samsung-galaxy-s7/'
        chromedriver = "/Users/Miya/Downloads/chromedriver.exe"
        os.environ["webdriver.chrome.driver"] = chromedriver
        driver = webdriver.Chrome(chromedriver)

        data_dict = dict()
        data_dict['Title'] = list()
        data_dict['ReviewText'] = list()
        data_dict['SubmissionTime'] = list()
        data_dict['UserNickname'] = list()

        driver.get(url)
        n = 0
        while True:
            html_source = driver.page_source
            soup = BeautifulSoup(html_source, 'html.parser')

            ## only look at reviews, exclude questions

            section = soup.findAll('div', attrs={"class": "bv-content-list-container"})
            ## title
            data_dict['Title'] += [i.text.strip() for i in
                                   section[0].find_all('h4', attrs={"class": "bv-content-title"})]
            print(
                [i.text.strip() for i in section[0].find_all('h4', attrs={"class": "bv-content-title"})][0])  # to check
            ## text
            data_dict['ReviewText'] += [i.text.strip() for i in
                                        section[0].find_all('div', attrs={"class": "bv-content-summary-body-text"})]
            ## time
            data_dict['SubmissionTime'] += [i['content'].strip() for i in
                                            section[0].find_all('meta', attrs={"itemprop": "datePublished"})]
            ## user
            data_dict['UserNickname'] += [i.text.strip() for i in
                                          section[0].find_all('h3', attrs={"class": "bv-author"})]

            ## click till the very last page page
            try:
                driver.find_element_by_partial_link_text('►').click()
                time.sleep(10)
                n += 1
            except:
                print(n)
                try:
                    driver.find_element_by_partial_link_text('Next').click()
                    time.sleep(10)
                    n += 1
                except:
                    break

        ## save data file into csv
        filename = 'data'+self.id+'csv'
        pd.DataFrame.from_dict(data_dict).to_csv(filename)
        ## put files to hadoop
        os.system('hadoop fs -put '+filename)

        # print write review file into txt
        reviewfilename = 'reviews'+self.id+'.txt'
        with open(reviewfilename, 'w') as file:
            for review in data_dict['ReviewText']:
                file.write(review + '\n')
        ## put files to hadoop
        os.system('hadoop fs -put '+reviewfilename)



    def word_count(self):
        print('Now bag of words....')
        ## run word count and generate keywords.csv
        os.system("spark-submit spark.py "+'reviews'+self.id+'.txt '+'localhost 9999')
        ## put keywords to hadoop
        os.system('hadoop fs -put keywords.csv')
        ## save keywords to hive table
        os.system('hive -f table.sql')