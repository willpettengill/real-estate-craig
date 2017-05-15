import pandas as pd
import json
import requests
from bs4 import BeautifulSoup as bs4
import craigslist_scraper
from craigslist_scraper import scraper
import numpy as np
import psycopg2
import time
#dbname = scraper, table = re_data

#TO DO:

#Set up database functionality to write to AWS
#create text file of all urls to prevent duplicate scraping
#iterate to get all CL listings, rather than first 160
#use: http://chrisholdgraf.com/querying-craigslist-with-python/
#

#Extend basic scraper function to include additional meta data from CL listing
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
	else:
		pass

#Parse a CL result page into a list of links to pass to scraper and AddMeta
def getResults(page=1):
	link_list = []
	base_url = 'https://newyork.craigslist.org/search/off?bundleDuplicates=1'
	#rsp = requests.get('https://newyork.craigslist.org/search/off?bundleDuplicates=1&min_price=1&max_price=1000000&minSqft=1&maxSqft=1000000&availabilityMode=0')
	rsp = requests.get(base_url, params = {'min_price':1,'max_price':1000000, 'minSqft':1, 'maxSqft':1000000, 's':page})
	html = bs4(rsp.text, 'html.parser')
	listings = html.find_all(attrs={'class': 'result-row'})
	for listing in listings:
			detail =  listing.find_all(attrs={'class': 'result-title hdrlnk'})
			link = 'https://newyork.craigslist.org/' + detail[0]['href']
			link_list.append(link)
	return link_list

def DBbuild():
	con = psycopg2.connect("dbname='scraper' user='wpettengill' password='wpettengill' host='craigslistdb.cnc0ky2ic2hk.us-east-1.rds.amazonaws.com' port='5432'")
	cur = con.cursor()
	#cur.execute('drop table if exists re_data; Create table re_data (address text, latitude double precision, longitude double precision, sqfeet int, fulltitle text, neighborhood text, price int, title text);')
	return con, cur

def DBwrite(re_data, con, cur):
	today = datetime.date.today().strftime("%D")
	try:
		qry = '''
		insert into re_data (address, latitude, longitude, sqfeet, fulltitle, neighborhood, price, title, url, date) values (%s, %s, %s, %s, %s, %s, %s, %s)
		''' 
		cur.execute(qry, (re_data.address, re_data.latitude, re_data.longitude, re_data.sqfeet, re_data.fulltitle, re_data.neighborhood, re_data.price, re_data.title, url, today))
		print 'insert success'
		con.commit()
	except:
		con.rollback()

def checkdb():
	qry = 'select * from re_data'
	cur.execute(qry, con)
	
	for record in cur:
		print record
	print '%s records added'.format(len(cur))	

def main():
	url_list = json.load(open('cl_listings.json'))
	con, cur = DBbuild()
	for i in np.arange(0,2500,100):
		print 'index is %s'.format(i)
		urls = getResults(i)
		for url in urls:
			if url not in url_list:
				url_list.append(url)
				re_data = scraper.scrape_url(url)
				AddMeta(re_data)
				DBwrite(re_data, con, cur) #Build this function
				print 'record added'
				time.sleep(2)
		with open('cl_listings.json', 'w') as f:
		        json.dump(list(set(url_list)), f)	


if __name__ == "__main__":
	main()
	checkdb()