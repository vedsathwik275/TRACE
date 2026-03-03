# news_scraper.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
import re
from newspaper import Article
import urllib.parse

class TRACENewsScraper:
    def __init__(self):
        """Initialize scraper focused on NBA injury news"""
        self.articles = []
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # Keywords related to injuries and players (expand as needed)
        self.injury_keywords = [
            'injury', 'injured', 'injuries', 'injury report', 'out for season',
            'ACL', 'MCL', 'meniscus', 'torn', 'fracture', 'break', 'sprain',
            'strain', 'surgery', 'rehabilitation', 'recovery', 'healing',
            'miss', 'game out', 'status update', 'play status', 'return date',
            'achilles', 'achillies', 'achilles tendon', 'foot injury', 'heel',
            'plantar fasciitis', 'plantar', 'fascia', 'tarsal tunnel', 'stress fracture',
            'calf strain', 'hamstring', 'quad', 'groin', 'hip flexor', 'back injury',
            'spine', 'vertebrae', 'disc', 'herniated', 'bulging', 'concussion',
            'head injury', 'neck', 'shoulder separation', 'rotator cuff', 'labrum',
            'elbow', 'wrist', 'hand', 'finger', 'thumb', 'ankle sprain', 'ankle',
            'knee surgery', 'knee pain', 'cartilage', 'ligament', 'tendon',
            'degenerative', 'arthritic', 'arthritis', 'soreness', 'tightness',
            'sore', 'tight', 'pain', 'discomfort', 'ailment', 'condition',
            'ailment', 'ailments', 'health', 'medical', 'diagnosis', 'prognosis',
            'rest', 'load management', 'management', 'conservative treatment',
            'non-operative', 'operative', 'procedure', 'operation', 'surgical',
            'post-op', 'post operation', 'post surgery', 'post-surgery',
            'return to play', 'RTP', 'play again', 'back in action', 'active',
            'inactive', 'DTD', 'DNP', 'probable', 'questionable', 'doubtful',
            'likely', 'status', 'update', 'news', 'report', 'information',
            'latest', 'breaking', 'developing', 'announcement', 'statement',
            'team statement', 'official', 'confirmed', 'rumors', 'speculation',
            'Kyrie Irving', 'Irving', 'Jayson Tatum', 'Tatum', 'Jaylen Brown', 'Brown',
            'Giannis Antetokounmpo', 'Giannis', 'Damian Lillard', 'Lillard',
            'Nikola Jokić', 'Jokić', 'Jamal Murray', 'Murray', 'Michael Porter Jr.', 'MPJ',
            'Luka Dončić', 'Luka', 'Kevin Durant', 'Durant',
            'Devin Booker', 'Booker', 'Bradley Beal', 'Beal', 'Joel Embiid', 'Embiid',
            'James Harden', 'Harden', 'Tyrese Haliburton', 'Haliburton', 'Shai Gilgeous-Alexander', 'SGA',
            'Victor Wembanyama', 'Wemby', 'Scottie Barnes', 'Barnes', 'Paolo Banchero', 'Banchero',
            'Cade Cunningham', 'Cunningham', 'Zion Williamson', 'Zion', 'Brandon Miller', 'Miller',
            'LaMelo Ball', 'LaMelo', 'Jalen Brunson', 'Brunson', 'Anthony Edwards', 'AE',
            'Franz Wagner', 'Franz', 'Chet Holmgren', 'Chet', 'Victor Oladipo', 'Olly',
            'Terrence Shannon Jr.', 'T-Shaq', 'Steph Curry', 'Curry', 'Klay Thompson', 'Thompson',
            'Draymond Green', 'Green', 'Andrew Wiggins', 'Wiggins', 'Jordan Poole', 'Poole',
            'Klay Thompson', 'Thompson', 'Stephen Curry', 'Curry', 'LeBron James', 'LeBron',
            'Anthony Davis', 'AD', 'Russell Westbrook', 'Westbrook', 'Austin Reaves', 'Reaves',
            "D'Angelo Russell", 'DLo', 'Malik Monk', 'Monk', 'Jarred Vanderbilt', 'JV',
            'Thomas Bryant', 'Bryant', 'Rui Hachimura', 'Rui', 'Max Christie', 'Christie',
            'Skylar Mays', 'Mays', 'Juan Toscano-Anderson', 'JT', 'Jaxson Hayes', 'Hayes',
            'Derrick Rose', 'Rose', 'Alex Caruso', 'Caruso', 'Lonzo Ball', 'Lonzo',
            'DeMar DeRozan', 'DeRozan', 'Zach LaVine', 'LaVine', 'Nikola Vučević', 'Vucevic',
            'Pat Williams', 'Williams', 'Ayo Dosunmu', 'Ayo', 'Torrey Craig', 'Craig',
            'Justin Holiday', 'Holiday', 'Javonte Green', 'Green', 'Dalen Terry', 'Terry',
            'Carlik Jones', 'Jones', 'Adama Sanogo', 'Sanogo', 'Coby White', 'White',
            'Jordi Williams', 'Jordi', 'Drew Timme', 'Timme', 'Lindy Waters III', 'Waters',
            'Vlatko Čančar', 'Canchar', 'Bones Hyland', 'Bones', 'Christian Braun', 'Braun',
            'Reggie Jackson', 'Reggie', 'Jeff Dowtin', 'Dowtin', 'Collin Gillespie', 'Gillespie',
            'Marcus Morris Sr.', 'Mook', 'Peyton Watson', 'Watson', 'Hunter Tyson', 'Tyson',
            'Boogie Cousins', 'Boogie', 'Kentavious Caldwell-Pope', 'KCP', 'Troy Brown Jr.', 'Troy',
            'Aaron Gordon', 'Gordon', 'Christian Wood', 'Wood', 'DeAndre Ayton', 'Ayton',
            'Devin Booker', 'Book', 'Kevin Durant', 'KD', 'Mikal Bridges', 'Bridges',
            'Tyrese Maxey', 'Maxey', 'Tobias Harris', 'Harris', 'Joel Embiid', 'The Process',
            'Tyrese Maxey', 'Ty', 'Georges Niang', 'Niang', 'Paul Reed', 'Reed',
            'Charles Bassey', 'Bassey', 'Jaden Springer', 'Springer', 'Luka Garza', 'Garza',
            'Isaiah Joe', 'Isaiah', 'Marcus Morris Sr.', 'Marcus', 'P.J. Tucker', 'PJT',
            'Cam Thomas', 'Cam', 'Nic Claxton', 'Nic', 'Ben Simmons', 'Simmons',
            'Mikal Bridges', 'Mik', 'Timothee Chalamet', 'Timo', 'Dorian Finney-Smith', 'DFS',
            'Royce O''Neale', 'RON', 'Yuta Watanabe', 'Watanabe', 'Keon Johnson', 'Keon',
            'Day\'Ron Sharpe', 'Sharpe', 'Miles Bridges', 'Miles', 'Scottie Barnes', 'Scottie',
            'Pascal Siakam', 'Pascal', 'Jakob Poeltl', 'Jakob', 'OG Anunoby', 'OG',
            'Gary Trent Jr.', 'GT', 'Malachi Flynn', 'Flynn', 'Precious Achiuwa', 'Precious',
            'Chris Boucher', 'Boucher', 'Dalano Banton', 'Banton', 'Khem Birch', 'Birch',
            'Scottie Barnes', 'Scoot', 'RJ Barrett', 'RJ', 'Quentin Grimes', 'Quentin',
            'Miles McBride', 'Miles', 'Jericho Sims', 'Sims', 'Mitchell Robinson', 'Mitch',
            'Taj Gibson', 'Taj', 'Cameron Thomas', 'CJ', 'Landry Shamet', 'Landry',
            'Day\'Ron Sharpe', 'Day', 'Jalen Wilson', 'Wilson', 'Dennis Smith Jr.', 'DSJ',
            'Mason Plumlee', 'Mason', 'Ochai Agbaji', 'Ochai', 'Javonte Smart', 'Smart',
            'Drew Eubanks', 'Eubanks', 'Keita Bates-Diop', 'KBD', 'Alperen Sengun', 'Alperen',
            'Jalen Green', 'Jalen', 'Kevin Porter Jr.', 'KPJ', 'Tari Eason', 'Eason',
            'Amen Thompson', 'Amen', 'Cam Whitmore', 'Whitmore', 'Dillon Brooks', 'Dillon',
            'Fred VanVleet', 'Fred', 'Jae\'Sean Tate', 'Tate', 'Dereck Lively II', 'Lively',
            'Vince Williams Jr.', 'Vince', 'Jock Landale', 'Jock', 'A.J. Griffin', 'AJ',
            'Kelly Oubre Jr.', 'KO', 'Clint Capela', 'Capela', 'Dejounte Murray', 'Dejounte',
            'Trae Young', 'Trae', 'Bogdan Bogdanović', 'Bogdan', 'De\'Andre Hunter', 'Hunter',
            'Onyeka Okongwu', 'Onyeka', 'AJ Griffin', 'AJ', 'Saddiq Bey', 'Saddiq',
            'Killian Hayes', 'Killian', 'Jalen Duren', 'Duren', 'Ausar Thompson', 'Ausar',
            'Marcus Sasser', 'Sasser', 'Isaiah Stewart', 'Stewart', 'Jamaree Bouyea', 'Bouyea',
            'Isaiah Livers', 'Livers', 'Jaden Ivey', 'Ivey', 'Marcus Morris Sr.', 'Markieff',
            'Danilo Gallinari', 'Gallinari', 'T.J. Warren', 'Warren'
        ]

    def fetch_article_body(self, url: str, source: str):
        """Fetch the full body of an article from its URL."""
        try:
            article = Article(url)
            article.download()
            article.parse()
            body_text = article.text
            word_count = len(body_text.split())
            return {
                'body': body_text,
                'word_count': word_count,
                'fetch_success': True
            }
        except Exception as e:
            print(f"⚠️ Could not fetch full article from {source}: {e}")
            return {
                'body': '', # Fallback to empty string
                'word_count': 0,
                'fetch_success': False
            }

    def scrape_espn_rss(self, fetch_full_articles: bool = True):
        """Scrape ESPN NBA RSS feed with full article bodies"""
        print("🔍 Searching ESPN NBA RSS...")
        url = "http://www.espn.com/espn/rss/nba/news"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'xml')
                items = soup.find_all('item')
                for item in items:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pub_date_elem = item.find('pubdate')
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text(strip=True)
                        article_url = link_elem.get_text(strip=True)
                        
                        # Check for relevance
                        relevance_score = sum(1 for keyword in self.injury_keywords if keyword.lower() in title.lower())
                        if relevance_score > 0: # Only process if relevant
                            article_data = {
                                'title': title,
                                'url': article_url,
                                'source': 'ESPN',
                                'pub_date': pub_date_elem.get_text(strip=True) if pub_date_elem else 'Unknown',
                                'mentioned_players': [],
                                'relevance_score': relevance_score,
                                'body': '',
                                'body_word_count': 0,
                                'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            
                            # Find mentioned players in title
                            for player in [kw for kw in self.injury_keywords if len(kw.split()) == 1 or kw.count(' ') == 1]: # Simple check
                                if player.lower() in title.lower() and player not in article_data['mentioned_players']:
                                    article_data['mentioned_players'].append(player)

                            # Fetch full article body
                            if fetch_full_articles and article_url:
                                print(f"🌐 Fetching full article from URL...")
                                body_data = self.fetch_article_body(article_url, 'ESPN')
                                if not body_data['fetch_success']:
                                    print(f"🔄 Trying fallback method...")
                                    # Fallback could involve direct requests/bs4 parsing
                                    try:
                                        inner_response = requests.get(article_url, headers=self.headers, timeout=10)
                                        if inner_response.status_code == 200:
                                            inner_soup = BeautifulSoup(inner_response.content, 'html.parser')
                                            # This is a generic attempt; ESPN might require specific selectors
                                            paragraphs = inner_soup.find_all('p')
                                            fallback_body = ' '.join([p.get_text(strip=True) for p in paragraphs])
                                            body_data['body'] = fallback_body
                                            body_data['word_count'] = len(fallback_body.split())
                                            body_data['fetch_success'] = True
                                    except:
                                         pass # If fallback fails, body remains empty

                            article_data['body'] = body_data['body']
                            article_data['body_word_count'] = body_data['word_count']

                            # Avoid duplicates
                            if not any(existing['title'] == title for existing in self.articles):
                                self.articles.append(article_data)
                                print(f"✅ Found: {title[:70]}...")
                            time.sleep(1) # Rate limiting
        except Exception as e:
            print(f"❌ ESPN RSS error: {e}")

    def scrape_cbs_sports_rss(self, fetch_full_articles: bool = True):
        """Scrape CBS Sports NBA RSS feed with full article bodies"""
        print("🔍 Searching CBS Sports NBA RSS...")
        url = "https://www.cbssports.com/rss/nba/"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'xml')
                items = soup.find_all('item')
                for item in items:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pub_date_elem = item.find('pubdate')
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text(strip=True)
                        article_url = link_elem.get_text(strip=True)
                        
                        # Check for relevance
                        relevance_score = sum(1 for keyword in self.injury_keywords if keyword.lower() in title.lower())
                        if relevance_score > 0:
                            article_data = {
                                'title': title,
                                'url': article_url,
                                'source': 'CBS Sports',
                                'pub_date': pub_date_elem.get_text(strip=True) if pub_date_elem else 'Unknown',
                                'mentioned_players': [],
                                'relevance_score': relevance_score,
                                'body': '',
                                'body_word_count': 0,
                                'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            
                            # Find mentioned players in title
                            for player in [kw for kw in self.injury_keywords if len(kw.split()) == 1 or kw.count(' ') == 1]: 
                                if player.lower() in title.lower() and player not in article_data['mentioned_players']:
                                    article_data['mentioned_players'].append(player)

                            # Fetch full article body
                            if fetch_full_articles and article_url:
                                print(f"🌐 Fetching full article from URL...")
                                body_data = self.fetch_article_body(article_url, 'CBS Sports')
                                if not body_data['fetch_success']:
                                    print(f"🔄 Trying fallback method...")
                                    try:
                                        inner_response = requests.get(article_url, headers=self.headers, timeout=10)
                                        if inner_response.status_code == 200:
                                            inner_soup = BeautifulSoup(inner_response.content, 'html.parser')
                                            # Generic fallback selector
                                            paragraphs = inner_soup.find_all('p')
                                            fallback_body = ' '.join([p.get_text(strip=True) for p in paragraphs])
                                            body_data['body'] = fallback_body
                                            body_data['word_count'] = len(fallback_body.split())
                                            body_data['fetch_success'] = True
                                    except:
                                         pass

                            article_data['body'] = body_data['body']
                            article_data['body_word_count'] = body_data['word_count']

                            # Avoid duplicates
                            if not any(existing['title'] == title for existing in self.articles):
                                self.articles.append(article_data)
                                print(f"✅ Found: {title[:70]}...")
                            time.sleep(1) # Rate limiting
        except Exception as e:
            print(f"❌ CBS Sports RSS error: {e}")

    def scrape_bleacher_report_rss(self, fetch_full_articles: bool = True):
        """Scrape Bleacher Report NBA RSS feed with full article bodies"""
        print("🔍 Searching Bleacher Report NBA RSS...")
        url = "https://bleacherreport.com/articles/feed?tag_id=20"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'xml')
                items = soup.find_all('item')
                for item in items:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pub_date_elem = item.find('pubdate')
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text(strip=True)
                        article_url = link_elem.get_text(strip=True)
                        
                        # Check for relevance
                        relevance_score = sum(1 for keyword in self.injury_keywords if keyword.lower() in title.lower())
                        if relevance_score > 0:
                            article_data = {
                                'title': title,
                                'url': article_url,
                                'source': 'Bleacher Report',
                                'pub_date': pub_date_elem.get_text(strip=True) if pub_date_elem else 'Unknown',
                                'mentioned_players': [],
                                'relevance_score': relevance_score,
                                'body': '',
                                'body_word_count': 0,
                                'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            
                            # Find mentioned players in title
                            for player in [kw for kw in self.injury_keywords if len(kw.split()) == 1 or kw.count(' ') == 1]: 
                                if player.lower() in title.lower() and player not in article_data['mentioned_players']:
                                    article_data['mentioned_players'].append(player)

                            # Fetch full article body
                            if fetch_full_articles and article_url:
                                print(f"🌐 Fetching full article from URL...")
                                body_data = self.fetch_article_body(article_url, 'Bleacher Report')
                                if not body_data['fetch_success']:
                                    print(f"🔄 Trying fallback method...")
                                    try:
                                        inner_response = requests.get(article_url, headers=self.headers, timeout=10)
                                        if inner_response.status_code == 200:
                                            inner_soup = BeautifulSoup(inner_response.content, 'html.parser')
                                            paragraphs = inner_soup.find_all('p')
                                            fallback_body = ' '.join([p.get_text(strip=True) for p in paragraphs])
                                            body_data['body'] = fallback_body
                                            body_data['word_count'] = len(fallback_body.split())
                                            body_data['fetch_success'] = True
                                    except:
                                         pass

                            article_data['body'] = body_data['body']
                            article_data['body_word_count'] = body_data['word_count']

                            # Avoid duplicates
                            if not any(existing['title'] == title for existing in self.articles):
                                self.articles.append(article_data)
                                print(f"✅ Found: {title[:70]}...")
                            time.sleep(1) # Rate limiting
        except Exception as e:
            print(f"❌ Bleacher Report RSS error: {e}")

    def scrape_yahoo_sports_rss(self):
        """Scrape Yahoo Sports NBA RSS feed"""
        print("🔍 Searching Yahoo Sports NBA RSS...")
        url = "https://sports.yahoo.com/nba/rss.xml"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'xml')
                items = soup.find_all('item')
                for item in items:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pub_date_elem = item.find('pubdate')
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text(strip=True)
                        article_url = link_elem.get_text(strip=True)
                        
                        # Check for relevance
                        relevance_score = sum(1 for keyword in self.injury_keywords if keyword.lower() in title.lower())
                        if relevance_score > 0:
                            article_data = {
                                'title': title,
                                'url': article_url,
                                'source': 'Yahoo Sports',
                                'pub_date': pub_date_elem.get_text(strip=True) if pub_date_elem else 'Unknown',
                                'mentioned_players': [],
                                'relevance_score': relevance_score,
                                'body': '', # Body fetching not implemented for Yahoo in original
                                'body_word_count': 0,
                                'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            
                            # Find mentioned players in title
                            for player in [kw for kw in self.injury_keywords if len(kw.split()) == 1 or kw.count(' ') == 1]: 
                                if player.lower() in title.lower() and player not in article_data['mentioned_players']:
                                    article_data['mentioned_players'].append(player)

                            # Avoid duplicates
                            if not any(existing['title'] == title for existing in self.articles):
                                self.articles.append(article_data)
                                print(f"✅ Found: {title[:70]}...")
        except Exception as e:
            print(f"❌ Yahoo Sports error: {e}")

    def scrape_nba_official_news(self):
        """Scrape NBA.com official news"""
        print("🔍 Searching NBA.com Official News...")
        url = "https://www.nba.com/news/rss.xml"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'xml')
                items = soup.find_all('item')
                for item in items:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pub_date_elem = item.find('pubdate')
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text(strip=True)
                        article_url = link_elem.get_text(strip=True)
                        
                        # Check for relevance
                        relevance_score = sum(1 for keyword in self.injury_keywords if keyword.lower() in title.lower())
                        if relevance_score > 0:
                            article_data = {
                                'title': title,
                                'url': article_url,
                                'source': 'NBA.com',
                                'pub_date': pub_date_elem.get_text(strip=True) if pub_date_elem else 'Unknown',
                                'mentioned_players': [],
                                'relevance_score': relevance_score,
                                'body': '', # Body fetching not implemented for NBA.com in original
                                'body_word_count': 0,
                                'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            
                            # Find mentioned players in title
                            for player in [kw for kw in self.injury_keywords if len(kw.split()) == 1 or kw.count(' ') == 1]: 
                                if player.lower() in title.lower() and player not in article_data['mentioned_players']:
                                    article_data['mentioned_players'].append(player)

                            # Avoid duplicates
                            if not any(existing['title'] == title for existing in self.articles):
                                self.articles.append(article_data)
                                print(f"✅ Found: {title[:70]}...")
        except Exception as e:
            print(f"❌ NBA.com error: {e}")

    def scrape_sporting_news_rss(self):
        """Scrape Sporting News NBA RSS feed"""
        print("🔍 Searching Sporting News NBA RSS...")
        url = "https://www.sportingnews.com/us/rss/nba"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'xml')
                items = soup.find_all('item')
                for item in items:
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pub_date_elem = item.find('pubdate')
                    
                    if title_elem and link_elem:
                        title = title_elem.get_text(strip=True)
                        article_url = link_elem.get_text(strip=True)
                        
                        # Check for relevance
                        relevance_score = sum(1 for keyword in self.injury_keywords if keyword.lower() in title.lower())
                        if relevance_score > 0:
                            article_data = {
                                'title': title,
                                'url': article_url,
                                'source': 'Sporting News',
                                'pub_date': pub_date_elem.get_text(strip=True) if pub_date_elem else 'Unknown',
                                'mentioned_players': [],
                                'relevance_score': relevance_score,
                                'body': '', # Body fetching not implemented for Sporting News in original
                                'body_word_count': 0,
                                'scraped_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            
                            # Find mentioned players in title
                            for player in [kw for kw in self.injury_keywords if len(kw.split()) == 1 or kw.count(' ') == 1]: 
                                if player.lower() in title.lower() and player not in article_data['mentioned_players']:
                                    article_data['mentioned_players'].append(player)

                            # Avoid duplicates
                            if not any(existing['title'] == title for existing in self.articles):
                                self.articles.append(article_data)
                                print(f"✅ Found: {title[:70]}...")
        except Exception as e:
            print(f"❌ Sporting News error: {e}")

    def run_trace_scrape(self, fetch_full_articles: bool = True):
        """Run COMPREHENSIVE TRACE-focused scraping from ALL sources"""
        print("🏀 TRACE Project: Comprehensive NBA Injury News Scraper")
        print("=" * 60)
        
        # Multiple scraping methods
        self.scrape_espn_rss(fetch_full_articles=fetch_full_articles)
        time.sleep(2)
        self.scrape_cbs_sports_rss(fetch_full_articles=fetch_full_articles)
        time.sleep(2)
        # self.scrape_bleacher_report_rss(fetch_full_articles=fetch_full_articles)
        # time.sleep(2)
        # self.search_google_news() # Assuming this was commented out in original
        # time.sleep(2)
        self.scrape_yahoo_sports_rss()
        time.sleep(2)
        self.scrape_nba_official_news()
        time.sleep(2)
        self.scrape_sporting_news_rss()
        time.sleep(2)
        
        return self.articles

    def analyze_results(self):
        """Analyze collected articles."""
        if not self.articles:
            print("❌ No articles collected.")
            return

        df = pd.DataFrame(self.articles)
        print(f"\n📊 Collected {len(df)} articles.")
        print(f"Sources breakdown:\n{df['source'].value_counts()}")
        print(f"Average relevance score: {df['relevance_score'].mean():.2f}")
        # Example: Print top 5 most relevant
        top_relevant = df.nlargest(5, 'relevance_score')
        print(f"\n📋 Top 5 Most Relevant Articles:")
        for _, row in top_relevant.iterrows():
            print(f"  [{row['relevance_score']:.1f}] ({row['source']}) {row['title'][:80]}...")

    def save_for_trace(self):
        """Save collected articles to a CSV file."""
        if not self.articles:
            print("❌ No articles to save.")
            return pd.DataFrame()

        df = pd.DataFrame(self.articles)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'data/trace_news_data_{timestamp}.csv'
        df.to_csv(filename, index=False)
        print(f"\n💾 News data saved to: {filename}")
        return df