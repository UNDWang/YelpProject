# *-*coding:utf-8 *-*

# crawl from yelp with one certain keyword input
import urllib.request  
import os
import re, random
import time
from bs4 import BeautifulSoup
import lxml
from html5print import HTMLBeautifier
import json
from multiprocessing.dummy import Pool as ThreadPool
from threading import Lock 


class suckyelp:

	#search keyword and waiting time after each visit
	def __init__(self, keyword):
		self.keyword = keyword
		self.webheader = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}   
		self.proxies = []  					#proxy pool
		#for test
		self.proxies = ["165.138.65.233:3128","74.118.0.16:80","115.110.118.62:3128","165.138.79.68:8080","202.40.3.153:80"]
		self.myreq = urllib.request
		self.url = "https://www.yelp.com/search?find_loc=" + self.keyword.replace(" ", "+")
		self.biz_page = 0                        
		self.file_seq = 1                       #index for file storing 
		self.lock = Lock()
		self.biz_list = []
		self.reviewurls = []
		self.hidden_reviewurls = []
		self.hidden_reviews = []
		self.reviews = []
		

	def get_num(self,string):
		return re.compile('\d+').findall(string)

	#return html file after pasrsing by beautiful soup
	def get_soup(self,pageurl,rest=7):
		self.change_proxy_randomly()

		try:
			req = urllib.request.Request(url= pageurl, headers= self.webheader)
			time.sleep(rest)
			htmldoc = urllib.request.urlopen(req).read()
			return BeautifulSoup(htmldoc,"lxml")
		except Exception as e:
			print (str(e))
			return 0

	#save as json file
	def write_json(self, data, data_name):
		print("saving%d..." %file_seq)
		
		file = data_name+str(self.file_seq)+'.json'
		self.file_seq += 1
		if not os.path.exists(data_name):
			os.makedirs(data_name)
		
		with open(data_name+'/'+file,'w') as ff:
			ff.write(json.dumps(data, indent=2))

		return

	#get biz pages of each business
	def Into_page(self, url):
		soup = self.get_soup(url)
		if soup == 0:
			print("error, plz wait or change proxy...")
			time.sleep(20)
			return
		return self.get_num(soup.find("div",{"class", "page-of-pages"}).string)[1]


	#For map function, each time parse one page
	def parse_biz(self, pagecount):
		page_url = self.url + "?start=%d" %(pagecount*10)
		soup = self.get_soup(page_url,10)
		if soup is 0:
			print("error, plz wait or change proxy...")
			time.sleep(20)
			return
			
		tags = soup.findAll("div",{"class", "search-result"})

		for tag in tags:
			biz_data = {
				"biz-id" : tag["data-biz-id"],
				"url" : "https://www.yelp.com" + tag.find("a",{"class", "biz-name"})["href"],
				#"review-count" : self.get_num(tag.find("span",{"class", "review-count"}).string)[0]
				}
			with self.lock:
				self.biz_list.append(biz_data)
		return


	#Parse url childpages in each bizpage 
	def get_reviewurls(self, biz):
		review_page_num = int(self.Into_page(biz["url"]))
		if review_page_num == 0: return
		
		for i in range(0, review_page_num):
			url_data = {
				"biz-id" : biz["biz-id"],
				"url" : biz["url"] + "?start=%d" %(i*20)
			}
			with self.lock:
				self.reviewurls.append(url_data)

		return

	def get_hidden_reviewurls(self, biz):
		review_page_num = int(self.Into_page(biz["url"]))
		if review_page_num == 0: return
		
		for i in range(0, review_page_num):
			url_data = {
				"biz-id" : biz["biz-id"],
				"url" : biz["url"].replace("biz","not_recommended_reviews") + "?start=%d" %(i*20)
			}
			with self.lock:
				self.hidden_reviewurls.append(url_data)

		return


	#Paser review in each review url childpage
	def parse_review(self, urldata):
		soup = self.get_soup(urldata["url"],10)
		if soup == 0:
			print("error, plz wait or change proxy...")
			time.sleep(20)
			return
		tags = soup.findAll("div",{"class", "review review--with-sidebar"})
		reviewtype = "recommend"
		for tag in tags:
			#get basic info in userstats		
			userstats = []
			for i in tag.find("ul",{"class", "user-passport-stats"}).findAll("b"):
				userstats.append(i.string)
			while len(userstats)<3: userstats.append("0")

			data = {
				"user-id" :tag.find("a",{"class", "user-display-name"})["data-hovercard-id"] if reviewtype == "recommend" else tag.find("span",{"class", "user-display-name"})["data-hovercard-id"],
				"review-id" : tag["data-review-id"] if reviewtype == "recommend" else 0,
				"business-id" : urldata["biz-id"],
				"basic-info": "friends:" + userstats[0] + "  reviews:" + userstats[1] + "  photos:" + userstats[2],
				"page" : "https://www.yelp.com" + tag.find("a",{"class", "user-display-name"})["href"] if reviewtype == "recommend" else 0,
				"star" : self.get_num(tag.find("div",{"class", "i-stars"})["title"])[0],
				"date" : str(tag.find("span",{"class", "rating-qualifier"}).string).strip(),
				"text" : tag.p.text,
				"type" : reviewtype+" review"
				}
		
			#print(data)
			#print("Keep sucking...")

			with self.lock: self.reviews.append(data)
		print('one review page done,going 2 next one')

		if len(self.reviews) > 500: 
			with self.lock:
				self.write_json(self.reviews, "reviewdata")
				del self.reviews[:]
				self.reviews = []


		return

	#def get_user:


	def use_MulitProcess(self):
		#destribute crawling task
		#test ip
		#print(self.get_soup("http://www.ip138.com/ips1388.asp").prettify())
		self.biz_page = self.Into_page(self.url)
		print("Totally %s pages of business to be parse" %self.biz_page)
		print("Job1:Returning all page url")
		pool1 = ThreadPool(processes=8)
		pool1.map(self.parse_biz, list(range(0 , int(self.biz_page))))
		pool1.close() 
		pool1.join()
		print("Job1 done, Job2:Returning all business url")
		pool2 = ThreadPool(processes=8)
		pool2.map(self.get_reviewurls, self.biz_list)
		pool2.close() 
		pool2.join()
		print("Job2 done, Job3:Returning all reviews in all business url")
		pool3 = ThreadPool(processes=8)
		pool3.map(self.parse_review, self.reviewurls)
		pool3.close() 
		pool3.join()
		print("Finished all reviews under input keyword")




	#crawl proxy from xicidaili
	def get_proxy(self):
		'''
		header = {
			"Host": "www.xicidaili.com",
			"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:47.0) Gecko/20100101 Firefox/47.0",
			"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
			"Referer": "http://www.xicidaili.com/wt/1",
		}
		'''
		try:
			req = urllib.request.Request('http://www.xicidaili.com/',None,self.webheader)
		except:
			print('failed getting proxy')
			return

		response = urllib.request.urlopen(req)
		html = response.read().decode('utf-8')

		p = re.compile(r'''<tr[^>]*?>\s+
									<td[^>]*?>(.*?)</td>\s+
									<td[^>]*?>(.*?)</td>\s+
									<td[^>]*?>(.*?)</td>\s+
									<td[^>]*?>(.*?)</td>\s+
									<td[^>]*?>(.*?)</td>\s+
									<td[^>]*?>(.*?)</td>\s+
									<td[^>]*?>(.*?)</td>\s+
									<td[^>]*?>(.*?)</td>\s+
							</tr>''',re.VERBOSE) 

		proxy_list = p.findall(html)

		for each_proxy in proxy_list:
			if each_proxy[5] == 'HTTP':
				self.proxies.append(each_proxy[1]+':'+each_proxy[2])

		#cleaning invalid proxy
		print('valid porxies : %d' % len(self.proxies))
		for i in range(len(self.proxies) - 1, -1, -1):
			self.change_proxy(self.proxies[i])
			try:
				response = urllib.request.urlopen('http://www.baidu.com',timeout = 5)  #http://www.ip138.com/ips1388.asp
				html = response.read()

			except:
				print('URL error!')
				#print("delete invalid proxy")
				#self.proxies[i] = None                         #while proxy not working use local
				self.proxies.pop(i)                      #delete invalid proxy
				pass
		#print('valid porxies : %d' % len(self.proxies))
		print(self.proxies)

	#change
	def change_proxy(self, pro):
		if pro == None:
			proxy_support = urllib.request.ProxyHandler({})
		else:
			proxy_support = urllib.request.ProxyHandler({'http':pro})

		opener = urllib.request.build_opener(proxy_support)
		opener.addheaders = [('User-Agent',self.webheader['User-Agent'])]
		urllib.request.install_opener(opener)
		print('change proxy：%s' % ('localhost' if pro==None else pro)+'                          \r',end='')



	def change_proxy_randomly(self):
		self.change_proxy(random.choice(self.proxies))


	
suck = suckyelp("Las Vegas")
suck.get_proxy()
suck.use_MulitProcess()
#suck.Parse()