# bluesky_scraper.py
import requests
import pandas as pd
from datetime import datetime
import time
import json

class TRACEBlueskyScraper:
    def __init__(self):
        self.base_url = "https://bsky.social/xrpc"
        self.session = requests.Session()
        self.access_token = None
        self.did = None # Decentralized ID

    def login(self, handle, password):
        """Login to Bluesky using app password."""
        print(f"🔐 Logging in as {handle}...")
        url = f"{self.base_url}/com.atproto.server.createSession"
        payload = {
            "identifier": handle,
            "password": password
        }
        response = self.session.post(url, json=payload)
        if response.status_code == 200:
            data = response.json()
            self.access_token = data["accessJwt"]
            self.did = data["did"]
            self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})
            print("✅ Login successful!")
        else:
            print(f"❌ Login failed: {response.text}")
            raise Exception(f"Bluesky login failed: {response.text}")

    def search_posts(self, query, limit=50):
        """Search for posts containing the query."""
        print(f"🔍 Searching Bluesky for: {query}")
        url = f"{self.base_url}/app.bsky.feed.searchPosts"
        params = {
            "q": query,
            "limit": limit
        }
        response = self.session.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            return data.get("posts", [])
        else:
            print(f"❌ Search failed: {response.text}")
            return []

    def get_post_thread(self, uri, depth=2):
        """Get a post and its replies up to a certain depth."""
        print(f"🧵 Fetching thread for URI: {uri}")
        url = f"{self.base_url}/app.bsky.feed.getPostThread"
        params = {
            "uri": uri,
            "depth": depth
        }
        response = self.session.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Thread fetch failed: {response.text}")
            return {}

    def process_post(self, post_record, fetch_replies=False, max_replies=10):
        """Process a single post record and output standardized Supabase schema."""
        # Extract core post data
        uri = post_record['uri']
        cid = post_record['cid']
        author = post_record['author']
        record = post_record['record']

        text = record.get('text', '')
        created_at = record.get('createdAt', '')
        reply_count = record.get('replyCount', 0)
        repost_count = record.get('repostCount', 0)
        like_count = record.get('likeCount', 0)

        # Parse created_at to ISO format and extract year/month
        try:
            created_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            created_date_iso = created_dt.isoformat()
            year = created_dt.year
            month = created_dt.month
            year_month = created_dt.strftime('%Y-%m')
        except:
            created_dt = datetime.now()
            created_date_iso = created_dt.isoformat()
            year = created_dt.year
            month = created_dt.month
            year_month = created_dt.strftime('%Y-%m')
        
        # Build combined text (post + replies if fetched)
        combined_text = text

        # Fetch replies if requested
        if fetch_replies and reply_count > 0:
            thread_data = self.get_post_thread(uri)
            if 'thread' in thread_data and 'replies' in thread_data['thread']:
                replies = thread_data['thread']['replies'][:max_replies]
                for reply in replies:
                    reply_record = reply.get('post', {})
                    if 'record' in reply_record:
                        reply_text = reply_record['record'].get('text', '')
                        combined_text += f"\n---Reply---\n{reply_text}\n---End Reply---\n"

        # Calculate engagement tier
        total_engagement = like_count + repost_count
        engagement_tier = 'high' if total_engagement > 100 else ('medium' if total_engagement > 20 else 'low')
        
        # Check if achilles related
        is_achilles_related = 'achilles' in text.lower() or 'achillies' in text.lower()
        
        # Extract mentioned players (simple keyword matching)
        mentioned_players = []
        player_keywords = ['LeBron', 'Curry', 'Durant', 'Giannis', 'Luka', 'Jokic', 'Embiid', 'Tatum', 'AD']
        for player in player_keywords:
            if player.lower() in text.lower():
                mentioned_players.append(player)

        post_data = {
            # === STANDARD SUPABASE SCHEMA COLUMNS ===
            'source_platform': 'Bluesky',
            'source_detail': 'Post Search',
            'author': author.get('handle', 'Unknown'),
            'url': f"https://bsky.app/profile/{author['handle']}/post/{uri.split('/')[-1]}",
            'text_content': combined_text,
            'created_date': created_date_iso,
            'engagement_score': float(like_count),
            'engagement_secondary': float(repost_count),
            'engagement_tier': engagement_tier,
            'relevance_score': 0.0,  # Could be calculated based on search query match
            'recovery_phase': 'fan_discussion',
            'mentioned_players': json.dumps(mentioned_players),
            'is_achilles_related': is_achilles_related,
            'is_quality_content': True,
            'uploaded_at': datetime.now().isoformat(),
            'text_length': len(combined_text),
            'year': year,
            'month': month,
            'year_month': year_month,
            # === BLUESKY-SPECIFIC COLUMNS ===
            'num_replies_extracted': reply_count if fetch_replies else 0,
            'avg_reply_likes': 0.0,  # Would need to fetch individual reply data
            'total_reply_words': len(combined_text.split()) - len(text.split()) if fetch_replies else 0,
            # === UNUSED COLUMNS (set to defaults) ===
            'num_comments_extracted': 0,
            'avg_comment_score': 0.0,
            'total_comment_words': 0,
            'body_word_count': 0,
            'fetch_success': False,
        }
        return post_data

    def run_comprehensive_collection(self, posts_per_query=50, fetch_replies=True, max_replies=30):
        """Run comprehensive collection based on injury-related queries."""
        print("🦋 Running comprehensive Bluesky collection...")
        all_posts = []
        
        # Define search queries related to NBA injuries
        queries = [
            "nba injury", "injured nba player", "nba injury report", "achilles injury nba",
            "nba recovery", "nba rehab", "nba surgery", "nba return date", "nba status update"
        ]
        
        for query in queries:
            print(f"\n--- Searching for: {query} ---")
            raw_posts = self.search_posts(query, limit=posts_per_query)
            for raw_post in raw_posts:
                processed_post = self.process_post(raw_post, fetch_replies=fetch_replies, max_replies=max_replies)
                all_posts.append(processed_post)
            time.sleep(2) # Rate limiting

        df = pd.DataFrame(all_posts)
        print(f"\n📊 Collected {len(df)} posts from Bluesky.")
        return df

# Example usage (requires valid credentials)
# scraper = TRACEBlueskyScraper()
# scraper.login('your_handle.bsky.social', 'your_app_password')
# df_bluesky = scraper.run_comprehensive_collection(posts_per_query=50, fetch_replies=True, max_replies=30)