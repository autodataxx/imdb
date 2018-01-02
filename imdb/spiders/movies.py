# -*- coding: utf-8 -*-
import os
import csv
import glob
import MySQLdb

import scrapy


class MoviesSpider(scrapy.Spider):
    name = 'movies'
    allowed_domains = ['imdb.com']
    #start_urls = ['http://www.imdb.com/search/title?count=100&release_date=2005,2017']
    start_urls = ['http://www.imdb.com/search/title?count=100&release_date=2005,2017&page=1&ref_=adv_nxt']
    
    def parse(self, response):
		
		#title = response.xpath('//*[@class="lister-list"]/*[@class="lister-item mode-advanced"]/*[@class="lister-item-content"]/*[@class="lister-item-header"]/a/text()').extract_first()
        
		movies = response.xpath('//*[@class="lister-item mode-advanced"]')
		#content_path = './/*[@class="lister-item mode-advanced"]/*[@class="lister-item-content"]'
		for movie in movies:
			#num = movie.xpath('//*[@class="lister-item-index unbold text-primary"]/text()').extract_first()
			#print '------------------- this is con- --------------------'
			title = movie.xpath('.//*[@class="lister-item-header"]/a/text()').extract_first()
			year = movie.xpath('.//*[@class="lister-item-header"]/span[2]/text()').extract_first()[1:5]
			certificate = movie.xpath('.//p[1]/*[@class="certificate"]/text()').extract_first()
			rtime = movie.xpath('.//p[1]/span[3]/text()').extract_first()
			runtime=0
			if rtime:
				runtime = rtime.replace(' min','')
				
			#runtime = movie.xpath('.//p[1]/span[3]/text()').extract_first().replace(' min','')
			#genre = movie.xpath('.//p[1]/*[@class="genre"]/text()').extract_first().strip().split(",")[0]
			genres = movie.xpath('.//p[1]/*[@class="genre"]/text()').extract_first().rstrip().strip()
			desc = movie.xpath('.//p[2]/text()').extract_first().rstrip().strip()
			votes = movie.xpath('.//*[@class="sort-num_votes-visible"]/*[@name="nv"]/@data-value').extract_first()
			rate = movie.xpath('.//*[@class="ratings-bar"]/*[@name="ir"]/@data-value').extract_first()
			imgurl = movie.xpath('.//*[@class="lister-item-image float-left"]/a/*[@class="loadlate"]/@loadlate').extract_first()
			url = movie.xpath('.//*[@class="lister-item-header"]/a/@href').extract_first()
			absolute_url = response.urljoin(url)
			
			yield {'title':title,
					'year': year,
					'certificate':certificate ,
					'runtime':runtime,
					'genres':genres,
					'votes':votes,
					'rate':rate,
					'imgurl':imgurl,
					'desc':desc
					}

		next_page_url =  response.xpath('//*[@class="lister-page-next next-page"]/@href').extract_first()
		absolute_text_page_url = response.urljoin(next_page_url)
		yield scrapy.Request(absolute_text_page_url)

    def close(self,reason):
        csv_file = max(glob.iglob('*.csv'), key=os.path.getctime)
        print csv_file
        mydb = MySQLdb.connect(host="207.246.126.155",port=3306,user="lester", passwd="123456xl",db="imdb")
        cursor = mydb.cursor()
        csv_data = csv.reader(file(csv_file))
        
        row_count =0
        for row in csv_data:
			if row_count !=0:
				cursor.execute('INSERT IGNORE INTO movie(genres,runtime,certificate,title,rate,year,votes,imgurl,description) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)', row)
			row_count += 1
        mydb.commit()
        cursor.close()
