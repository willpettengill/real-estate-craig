import json
import requests
from bs4 import BeautifulSoup as bs4
import craigslist_scraper
from craigslist_scraper import scraper
import numpy as np
import psycopg2
import time
import datetime as dt
import logging
from dateutil.parser import parse
#dbname = scraper, table = re_data

#TO DO:


#Extend basic scraper function to include additional meta data from CL listing
## this should include posting date to find listings that keep getting re-listed
## add other Geo's

def AddMeta(x):
	if x.soup:
		
		try:
			x.address = x.soup.findAll(attrs={'class': 'mapaddress'})[0].text
		except:
			x.address = None	
		try:	
			x.latitude = float(re_data.soup.findAll(attrs={'class': 'mapbox'})[0].findChildren()[0].get('data-latitude'))
		except:
			x.latitude = None	
		try:	
			x.longitude = float(re_data.soup.findAll(attrs={'class': 'mapbox'})[0].findChildren()[0].get('data-longitude'))
		except:
			x.longitude = None
		try:	
			x.sqfeet = int(x.soup.findAll(attrs={'class': 'housing'})[0].text.replace('/','').replace('ft2','').replace('-','').strip())
		except:
			x.sqfeet = None
		try:	
			x.fulltitle = x.soup.findAll(attrs={'class': 'postingtitletext'})[0].text.replace(' \n\n\nhide this posting\n\n\n\n    unhide\n  \n\n','').replace('\n','')
		except:
			x.fulltitle = None
		try:	
			x.neighborhood = x.fulltitle[x.fulltitle.find('(')+1:x.fulltitle.find(')')]
		except:
			x.neighborhood = None
		try:
			x.post_date = parse(re_data.soup.findAll(attrs={'class': 'date timeago'})[0]['datetime'])
		except:
			x.post_date = None	
	else:
		pass

#Parse a CL result page into a list of links to pass to scraper and AddMeta
def getResults(page=1):
	link_list = []
	base_url = 'https://newyork.craigslist.org/search/off?bundleDuplicates=1'
	#rsp = requests.get('https://newyork.craigslist.org/search/off?bundleDuplicates=1&min_price=1&max_price=1000000&minSqft=1&maxSqft=1000000&availabilityMode=0')
	rsp = requests.get(base_url, params = {'min_price':1,'max_price':1000000, 'minSqft':1, 'maxSqft':1000000, 's':page})
	if rsp.status_code == 403:
		print rsp.text
		exit()
	else:		
		html = bs4(rsp.text, 'html.parser')
		listings = html.find_all(attrs={'class': 'result-row'})
		for listing in listings:
				detail =  listing.find_all(attrs={'class': 'result-title hdrlnk'})
				link = 'https://newyork.craigslist.org/' + detail[0]['href']
				link_list.append(link)
	return link_list

def DBbuild():
	con = psycopg2.connect("dbname='scraper' user='wpettengill' password='wpettengill' host='craigslistdb.cnc0ky2ic2hk.us-east-1.rds.amazonaws.com' port='5432'")
	#cur.execute('drop table if exists re_data; Create table re_data (address text, latitude double precision, longitude double precision, sqfeet int, fulltitle text, neighborhood text, price int, title text);')
	return con

def DBwrite(re_data, con, url):
	
	try:
		cur = con.cursor()
		qry = '''
		insert into re_data (address, latitude, longitude, sqfeet, fulltitle, neighborhood, price, title, url, date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
		''' 
		cur.execute(qry, (re_data.address, re_data.latitude, re_data.longitude, re_data.sqfeet, re_data.fulltitle, re_data.neighborhood, re_data.price, re_data.title, url, re_data.post_date.strftime('%Y/%m/%d')))
		print 'insert success'
		con.commit()
		cur.close()
	except:
		con.rollback()

def checkdb(con):
	cur = con.cursor()
	qry = 'select * from re_data'
	cur.execute(qry, con)

	print '%s total records' % (cur.rowcount)	

def main(con):
	url_list = json.load(open('cl_listings.json'))
	date_list = [dt.datetime.utcnow()]
	for i in np.arange(0,2500,100):
		print 'index is %s' % i
		urls = getResults(i)
		print 'got %s results' % (len(urls))
		for url in urls[50:60]:
			time.sleep(3)
			if url not in url_list:
				url_list.append(url)
				try:
					re_data = scraper.scrape_url(url)
				except:
					print 'problem scraping'
					print 'url is %s' % (url)
					continue
				try:
					AddMeta(re_data)
				except:
					print 'problem adding additional meta'
					print 'url is %s' % (url)
					continue
				try:
					DBwrite(re_data, con, url) #Build this function
					print 'wrote to db'
				except:
					print 'problem writing to db'
					print 'url is %s' % (url)
					continue
				try:
					date_list.append(re_data.post_date.replace(tzinfo=None))
				except:
					continue	
		if min(date_list) < (dt.datetime.utcnow() - dt.timedelta(3)):
			break
		else:
			time.sleep(100)

			
	with open('cl_listings.json', 'w') as f:
	        json.dump(list(set(url_list)), f)	


if __name__ == "__main__":
	con = DBbuild()
	print 'db connected'
	main(con)
	print 'main function'
	checkdb(con,cur)
	print 'checkdb done'