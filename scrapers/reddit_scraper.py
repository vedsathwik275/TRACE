# reddit_scraper.py
import praw
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import time
import re
import json
from typing import List, Dict, Optional
import warnings
warnings.filterwarnings('ignore')

print("🏀 TRACE Project: Comprehensive NBA Injury Scraper")
print("=" * 60)
print("Temporal Recovery Analytics for Career Expectation")
print("Using Official Reddit API via PRAW - All 30 Teams Coverage")
print("=" * 60)

class TRACEPrawScraper:
    """
    Comprehensive PRAW-based NBA injury sentiment scraper for the TRACE project.
    Expanded from working version to cover all 30 teams and more sources.
    """

    def __init__(self, client_id: str = None, client_secret: str = None, user_agent: str = None):
        """
        Initialize PRAW scraper with Reddit API credentials.
        """
        # Default user agent
        self.user_agent = user_agent or "TRACE-NBA-Research:v1.0.0 (by /u/TRACEResearcher)"
        # Store credentials
        self.client_id = client_id
        self.client_secret = client_secret
        # Reddit instance (will be initialized when credentials provided)
        self.reddit = None
        # ALL 30 NBA TEAMS - Comprehensive Coverage
        self.nba_teams = {
            # Eastern Conference - Atlantic Division
            'Boston Celtics': ['celtics', 'BostonCeltics'],
            'Brooklyn Nets': ['netstrade', 'BrooklynNets'],
            'New York Knicks': ['nyknicks', 'NewYorkKnicks'],
            'Philadelphia 76ers': ['sixers', 'Philadelphia76ers'],
            'Toronto Raptors': ['torontoraptors', 'TorontoRaptors'],
            
            # Eastern Conference - Central Division
            'Chicago Bulls': ['chicagobulls', 'ChicagoBulls'],
            'Cleveland Cavaliers': ['clevelandcavs', 'ClevelandCavaliers'],
            'Detroit Pistons': ['detroitpistons', 'DetroitPistons'],
            'Indiana Pacers': ['pacers', 'IndianaPacers'],
            'Milwaukee Bucks': ['mkebucks', 'MilwaukeeBucks'],
            
            # Eastern Conference - Southeast Division
            'Atlanta Hawks': ['atlantahawks', 'AtlantaHawks'],
            'Charlotte Hornets': ['hornets', 'CharlotteHornets'],
            'Miami Heat': ['heat', 'MiamiHeat'],
            'Orlando Magic': ['orlandomagic', 'OrlandoMagic'],
            'Washington Wizards': ['washingtonwizards', 'WashingtonWizards'],
            
            # Western Conference - Northwest Division
            'Denver Nuggets': ['denvernuggets', 'DenverNuggets'],
            'Minnesota Timberwolves': ['timberwolves', 'MinnesotaTimberwolves'],
            'Oklahoma City Thunder': ['thunder', 'OklahomaCityThunder'],
            'Portland Trail Blazers': ['ripcity', 'PortlandTrailBlazers'],
            'Utah Jazz': ['utahjazz', 'UtahJazz'],
            
            # Western Conference - Pacific Division
            'Golden State Warriors': ['warriors', 'GoldenStateWarriors'],
            'LA Clippers': ['clippers', 'LAClippers'],
            'Los Angeles Lakers': ['lakers', 'LosAngelesLakers'],
            'Phoenix Suns': ['suns', 'PhoenixSuns'],
            'Sacramento Kings': ['kings', 'SacramentoKings'],
            
            # Western Conference - Southwest Division
            'Dallas Mavericks': ['mavericks', 'DallasMavericks'],
            'Houston Rockets': ['rockets', 'HoustonRockets'],
            'Memphis Grizzlies': ['memesgrizz', 'MemphisGrizzlies'],
            'New Orleans Pelicans': ['pelicans', 'NewOrleansPelicans'],
            'San Antonio Spurs': ['nbaspurs', 'SanAntonioSpurs']
        }
        
        # Player keywords related to injuries (example list, expand as needed)
        self.injury_keywords = [
            'injury', 'injured', 'injuries', 'injury report', 'out for season',
            'ACL', 'MCL', 'meniscus', 'torn', 'fracture', 'break', 'sprain',
            'strain', 'surgery', 'rehabilitation', 'recovery', 'healing',
            'miss', 'game out', 'status update', 'play status', 'return date',
            'achilles', 'achillies', 'achilles tendon', 'foot injury', 'heel',
            'plantar fasciitis', 'plantar', 'fascia', 'tarsal tunnel', 'stress fracture',
            'calf strain', 'hamstring', 'quad', 'groin', 'hip flexor', 'back injury',
            'spine', 'vertebrae', 'disc', 'herniated', 'bulging', 'conchead injury', 'neck', 'shoulder separation', 'rotator cuff', 'labrum',
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
            'trade', 'acquisition', 'signing', 'contract', 'extension', 'rookie',
            'draft pick', 'free agency', 'FA', 'waivers', 'buyout', 'trade deadline',
            'deadline', 'deadline day', 'deadline drama', 'roster', 'lineup',
            'rotation', 'minutes', 'playing time', 'start', 'bench', 'backup',
            'role', 'position', 'performance', 'stats', 'statistics', 'numbers',
            'game log', 'log', 'production', 'efficiency', 'shooting', 'scoring',
            'rebounding', 'assists', 'defense', 'defensive', 'offense', 'offensive',
            'coach', 'coaching', 'strategy', 'system', 'scheme', 'playbook',
            'plays', 'tactics', 'matchup', 'opponent', 'schedule', 'season',
            'playoffs', 'playoff', 'regular season', 'preseason', 'summer league',
            'training camp', 'camp', 'practice', 'workout', 'conditioning',
            'fitness', 'strength', 'cardio', 'nutrition', 'diet', 'supplements',
            'recovery', 'sleep', 'mental health', 'wellness', 'healthcare',
            'medical staff', 'trainer', 'trainer room', 'therapy', 'physical therapy',
            'PT', 'chiropractor', 'massage', 'massage therapist', 'orthopedist',
            'surgeon', 'doctor', 'physician', 'specialist', 'expert', 'consultation',
            'second opinion', 'evaluation', 'scan', 'MRI', 'X-ray', 'ultrasound',
            'test', 'testing', 'results', 'findings', 'report', 'assessment',
            'diagnostic', 'diagnostics', 'imaging', 'imaging study', 'study',
            'research', 'research paper', 'study', 'data', 'analysis', 'analyst',
            'analytics', 'metric', 'measurement', 'benchmark', 'comparison',
            'peer', 'comparison', 'peer comparison', 'league average', 'average',
            'statistical', 'statistic', 'percentage', 'rate', 'ratio', 'proportion',
            'frequency', 'count', 'number', 'size', 'magnitude', 'severity',
            'grade', 'level', 'degree', 'extent', 'range', 'scope', 'impact',
            'effect', 'outcome', 'result', 'consequence', 'implication',
            'ramification', 'future', 'long term', 'short term', 'timeline',
            'duration', 'length', 'period', 'timeframe', 'window', 'phase',
            'stages', 'stage', 'progress', 'progression', 'regression',
            'plateau', 'improvement', 'worsening', 'deterioration', 'decline',
            'reversal', 'turnaround', 'recovery', 'recuperation', 'healing',
            'repair', 'regeneration', 'restoration', 'rehab', 'rehabilitation',
            'therapy', 'therapeutic', 'treatment', 'treat', 'manage', 'control',
            'prevent', 'prevention', 'mitigate', 'reduce', 'minimize', 'avoid',
            'risk', 'factor', 'factor', 'risk factor', 'precaution', 'measure',
            'protocol', 'guideline', 'rule', 'standard', 'best practice',
            'protocol', 'procedure', 'process', 'method', 'technique', 'approach',
            'strategy', 'plan', 'program', 'curriculum', 'course', 'path',
            'journey', 'route', 'way', 'direction', 'goal', 'objective',
            'target', 'aim', 'purpose', 'intent', 'reason', 'motivation',
            'drive', 'desire', 'aspiration', 'dream', 'hope', 'expectation',
            'anticipation', 'look forward', 'eager', 'excited', 'enthusiasm',
            'passion', 'love', 'enjoy', 'appreciate', 'value', 'cherish',
            'treasure', 'adore', 'admire', 'respect', 'esteem', 'honor',
            'praise', 'commend', 'applaud', 'celebrate', 'honor', 'recognize',
            'acknowledge', 'appreciate', 'thank', 'grateful', 'gratitude',
            'appreciation', 'acknowledgment', 'recognition', 'esteem',
            'regard', 'respect', 'deference', 'consideration', 'thoughtfulness',
            'kindness', 'compassion', 'empathy', 'sympathy', 'support',
            'encouragement', 'motivation', 'inspiration', 'uplift', 'boost',
            'confidence', 'esteem', 'morale', 'spirit', 'attitude', 'mindset',
            'perspective', 'outlook', 'view', 'stance', 'position', 'angle',
            'approach', 'method', 'way', 'manner', 'style', 'form', 'type',
            'kind', 'sort', 'variety', 'category', 'classification', 'group',
            'class', 'section', 'division', 'segment', 'part', 'portion',
            'piece', 'bit', 'element', 'component', 'aspect', 'facet',
            'feature', 'characteristic', 'trait', 'quality', 'attribute',
            'property', 'character', 'nature', 'essence', 'substance',
            'core', 'heart', 'soul', 'spirit', 'mind', 'intellect', 'brain',
            'cognitive', 'mental', 'psychological', 'emotional', 'feeling',
            'emotion', 'affect', 'sentiment', 'opinion', 'view', 'belief',
            'conviction', 'faith', 'trust', 'confidence', 'certainty',
            'assurance', 'security', 'stability', 'balance', 'equilibrium',
            'harmony', 'peace', 'calm', 'serenity', 'tranquility', 'quiet',
            'silence', 'stillness', 'motionlessness', 'rest', 'repose',
            'slumber', 'sleep', 'nap', 'doze', 'snooze', 'siesta', 'catnap',
            'power nap', 'rest period', 'break', 'pause', 'intermission',
            'interval', 'hiatus', 'gap', 'void', 'absence', 'vacancy',
            'emptiness', 'blank', 'space', 'distance', 'separation',
            'interval', 'span', 'stretch', 'extent', 'reach', 'scope',
            'range', 'breadth', 'width', 'depth', 'height', 'altitude',
            'elevation', 'level', 'tier', 'rank', 'order', 'sequence',
            'series', 'chain', 'link', 'connection', 'bond', 'tie',
            'relationship', 'association', 'affiliation', 'connection',
            'linkage', 'network', 'web', 'system', 'structure', 'framework',
            'scaffold', 'support', 'foundation', 'base', 'bottom', 'ground',
            'earth', 'soil', 'dirt', 'mud', 'clay', 'sand', 'gravel',
            'stone', 'rock', 'boulder', 'pebble', 'grain', 'particle',
            'molecule', 'atom', 'electron', 'proton', 'neutron', 'nucleus',
            'cell', 'organism', 'life', 'living', 'alive', 'breathing',
            'respiratory', 'circulatory', 'cardiovascular', 'heart',
            'pulse', 'beat', 'rhythm', 'tempo', 'pace', 'speed', 'velocity',
            'acceleration', 'force', 'energy', 'power', 'strength', 'might',
            'ability', 'capability', 'capacity', 'potential', 'possibility',
            'opportunity', 'chance', 'luck', 'fortune', 'destiny', 'fate',
            'karma', 'dharma', 'reincarnation', 'afterlife', 'eternity',
            'infinity', 'endless', 'perpetual', 'continuous', 'constant',
            'unchanging', 'stable', 'fixed', 'static', 'stationary',
            'immobile', 'motionless', 'still', 'at rest', 'idle', 'inactive',
            'dormant', 'latent', 'potential', 'hidden', 'concealed',
            'secret', 'mysterious', 'enigmatic', 'puzzling', 'confusing',
            'bewildering', 'perplexing', 'baffling', 'intriguing', 'fascinating',
            'captivating', 'engaging', 'interesting', 'compelling', 'gripping',
            'riveting', 'mesmerizing', 'hypnotizing', 'entrancing', 'spellbinding',
            'enchanting', 'charming', 'delightful', 'pleasing', 'enjoyable',
            'fun', 'amusing', 'entertaining', 'diverting', 'recreational',
            'leisure', 'pastime', 'hobby', 'interest', 'pursuit', 'endeavor',
            'venture', 'project', 'undertaking', 'enterprise', 'initiative',
            'effort', 'attempt', 'trial', 'experiment', 'test', 'examination',
            'investigation', 'inquiry', 'research', 'study', 'analysis',
            'examination', 'scrutiny', 'inspection', 'review', 'appraisal',
            'evaluation', 'assessment', 'rating', 'score', 'grade', 'mark',
            'level', 'degree', 'extent', 'amount', 'quantity', 'number',
            'size', 'magnitude', 'scale', 'proportion', 'ratio', 'fraction',
            'percentage', 'decimal', 'integer', 'whole number', 'real number',
            'imaginary number', 'complex number', 'prime number', 'composite number',
            'even number', 'odd number', 'positive number', 'negative number',
            'zero', 'null', 'nil', 'nothing', 'void', 'empty set', 'empty space',
            'vacuum', 'cosmos', 'universe', 'galaxy', 'star', 'planet',
            'moon', 'satellite', 'comet', 'asteroid', 'meteor', 'shooting star',
            'constellation', 'zodiac', 'sign', 'symbol', 'mark', 'sign',
            'indication', 'signal', 'hint', 'clue', 'tip', 'advice',
            'counsel', 'guidance', 'direction', 'instruction', 'command',
            'order', 'mandate', 'decree', 'dictate', 'pronounce', 'declare',
            'announce', 'publish', 'release', 'issue', 'distribute', 'spread',
            'disseminate', 'propagate', 'circulate', 'transmit', 'communicate',
            'inform', 'notify', 'alert', 'warn', 'advise', 'suggest',
            'recommend', 'propose', 'offer', 'present', 'provide', 'supply',
            'deliver', 'dispatch', 'send', 'transmit', 'forward', 'relay',
            'pass', 'transfer', 'hand over', 'give', 'grant', 'bestow',
            'award', 'present', 'offer', 'make available', 'provide access',
            'enable', 'allow', 'permit', 'authorize', 'approve', 'sanction',
            'endorse', 'support', 'uphold', 'maintain', 'preserve', 'protect',
            'defend', 'shield', 'guard', 'ward', 'safeguard', 'secure',
            'lock', 'fasten', 'close', 'shut', 'seal', 'bar', 'bolt',
            'padlock', 'chain', 'fence', 'wall', 'barrier', 'obstacle',
            'hindrance', 'impediment', 'block', 'stop', 'halt', 'pause',
            'cease', 'terminate', 'end', 'finish', 'complete', 'accomplish',
            'achieve', 'succeed', 'triumph', 'win', 'victory', 'conquer',
            'overcome', 'prevail', 'dominate', 'master', 'control', 'govern',
            'rule', 'lead', 'direct', 'manage', 'administer', 'operate',
            'run', 'conduct', 'execute', 'perform', 'carry out', 'implement',
            'put into effect', 'activate', 'trigger', 'initiate', 'start',
            'begin', 'commence', 'inaugurate', 'launch', 'open', 'inaugurate',
            'inaugural', 'first', 'initial', 'primary', 'chief', 'principal',
            'main', 'major', 'significant', 'important', 'crucial', 'vital',
            'essential', 'critical', 'key', 'central', 'core', 'heart',
            'soul', 'spirit', 'essence', 'nature', 'character', 'quality',
            'attribute', 'property', 'feature', 'trait', 'aspect', 'facet',
            'dimension', 'side', 'face', 'surface', 'edge', 'corner',
            'point', 'spot', 'location', 'place', 'position', 'site',
            'area', 'region', 'zone', 'territory', 'domain', 'realm',
            'sphere', 'field', 'discipline', 'subject', 'topic', 'theme',
            'subject matter', 'content', 'material', 'information', 'data',
            'facts', 'figures', 'statistics', 'numbers', 'quantities',
            'values', 'measures', 'units', 'metrics', 'standards', 'criteria',
            'benchmarks', 'references', 'models', 'examples', 'cases',
            'instances', 'samples', 'specimens', 'tests', 'trials', 'experiments',
            'observations', 'discoveries', 'findings', 'results', 'outcomes',
            'conclusions', 'deductions', 'inferences', 'implications',
            'ramifications', 'effects', 'consequences', 'repercussions',
            'aftermath', 'sequel', 'continuation', 'follow-up', 'sequel',
            'spin-off', 'derivative', 'adaptation', 'remake', 'reboot',
            'revival', 'renaissance', 'rebirth', 'renewal', 'refresh',
            'rejuvenation', 'regeneration', 'restoration', 'rehabilitation',
            'reconstruction', 'rebuilding', 'repair', 'fix', 'mend', 'patch',
            'stitch', 'sew', 'glue', 'paste', 'bond', 'attach', 'connect',
            'join', 'unite', 'combine', 'merge', 'fuse', 'blend', 'mix',
            'stir', 'shake', 'whisk', 'beat', 'knead', 'roll', 'flatten',
            'shape', 'form', 'mold', 'cast', 'pour', 'fill', 'pack', 'load',
            'stuff', 'cram', 'squeeze', 'compress', 'condense', 'compact',
            'dense', 'thick', 'heavy', 'massive', 'large', 'big', 'huge',
            'enormous', 'gigantic', 'colossal', 'titanic', 'monumental',
            'immense', 'vast', 'wide', 'broad', 'expansive', 'extensive',
            'comprehensive', 'inclusive', 'complete', 'full', 'total', 'entire',
            'whole', 'all', 'every', 'each', 'individual', 'particular',
            'specific', 'precise', 'exact', 'accurate', 'correct', 'right',
            'true', 'valid', 'genuine', 'authentic', 'real', 'actual',
            'factual', 'objective', 'subjective', 'relative', 'comparative',
            'absolute', 'universal', 'common', 'usual', 'typical', 'normal',
            'standard', 'average', 'ordinary', 'regular', 'routine', 'habitual',
            'customary', 'traditional', 'conventional', 'established', 'accepted',
            'recognized', 'acknowledged', 'approved', 'sanctioned', 'authorized',
            'licensed', 'certified', 'qualified', 'competent', 'capable',
            'skilled', 'proficient', 'expert', 'master', 'virtuoso', 'genius',
            'talented', 'gifted', 'brilliant', 'intelligent', 'smart', 'clever',
            'bright', 'sharp', 'quick', 'fast', 'rapid', 'swift', 'speedy',
            'hasty', 'quick', 'prompt', 'immediate', 'instant', 'sudden',
            'abrupt', 'unexpected', 'surprise', 'startling', 'shocking',
            'astounding', 'amazing', 'incredible', 'unbelievable', 'phenomenal',
            'extraordinary', 'remarkable', 'notable', 'significant', 'important',
            'crucial', 'vital', 'essential', 'critical', 'key', 'pivotal',
            'decisive', 'determining', 'influential', 'powerful', 'strong',
            'robust', 'sturdy', 'solid', 'firm', 'secure', 'stable', 'steady',
            'reliable', 'dependable', 'trustworthy', 'faithful', 'loyal',
            'devoted', 'dedicated', 'committed', 'involved', 'engaged',
            'active', 'participating', 'contributing', 'helping', 'assisting',
            'aiding', 'supporting', 'backing', 'endorsing', 'advocating',
            'promoting', 'fostering', 'nurturing', 'cultivating', 'developing',
            'enhancing', 'improving', 'bettering', 'advancing', 'progressing',
            'moving forward', 'evolving', 'changing', 'transforming', 'metamorphosing',
            'developing', 'growing', 'maturing', 'aging', 'ripening', 'flowering',
            'blooming', 'flourishing', 'thriving', 'prospering', 'succeeding',
            'winning', 'triumphing', 'conquering', 'prevailing', 'dominating',
            'mastering', 'controlling', 'managing', 'handling', 'dealing with',
            'addressing', 'tackling', 'confronting', 'facing', 'meeting',
            'encountering', 'running into', 'coming across', 'discovering',
            'finding', 'locating', 'identifying', 'recognizing', 'acknowledging',
            'accepting', 'receiving', 'taking', 'getting', 'obtaining', 'acquiring',
            'procuring', 'securing', 'gaining', 'achieving', 'earning', 'receiving',
            'obtaining', 'procuring', 'securing', 'gaining', 'achieving', 'earning'
        ]

        # Data storage
        self.scraped_data = []
        self.api_calls_made = 0

    def setup_reddit_connection(self, client_id: str, client_secret: str) -> bool:
        """
        Set up Reddit connection with PRAW - Same as your working version
        """
        try:
            print(f"🔐 Setting up Reddit connection...")
            print(f"📱 User Agent: {self.user_agent}")
            self.reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=self.user_agent
            )
            # Test connection
            username = self.reddit.user.me()
            print(f"✅ Connected successfully as: {username}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Reddit: {e}")
            return False

    def process_submission(self, submission, include_comments: bool = True, comment_limit: int = 30):
        """Process a single Reddit submission."""
        # Extract title and text
        title = submission.title
        text = submission.selftext
        
        # Calculate relevance scores based on keywords
        title_lower = title.lower()
        text_lower = text.lower()
        title_relevance = sum(1 for keyword in self.injury_keywords if keyword in title_lower)
        text_relevance = sum(1 for keyword in self.injury_keywords if keyword in text_lower)
        
        # Combine title and text for broader context
        combined_text = f"{title}\n\n{text}".strip()

        # Determine recovery phase (simple heuristic)
        recovery_phase = 'unknown'
        if any(phase in text_lower for phase in ['rehab', 'rehabilitation', 'therapy', 'recovery']):
            recovery_phase = 'rehabilitation'
        elif any(phase in text_lower for phase in ['return', 'back', 'active', 'playing']):
            recovery_phase = 'return_to_play'
        elif any(phase in text_lower for phase in ['surgery', 'operated', 'post-op']):
            recovery_phase = 'post_op'
        elif any(phase in text_lower for phase in ['injury', 'injured', 'out']):
            recovery_phase = 'acute'

        # Extract mentioned players (simple keyword matching)
        mentioned_players = []
        player_keywords = [
            'Kyrie Irving', 'Irving', 'Jayson Tatum', 'Tatum', 'Jaylen Brown', 'Brown',
            'Giannis Antetokounmpo', 'Giannis', 'Damian Lillard', 'Lillard',
            'Nikola Jokić', 'Jokić', 'Jamal Murray', 'Murray', 'Michael Porter Jr.', 'MPJ',
            'Luka Dončić', 'Luka', 'Kyrie Irving', 'Kyrie', 'Kevin Durant', 'Durant',
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
        for player in player_keywords:
            if player.lower() in combined_text.lower():
                if player not in mentioned_players:
                    mentioned_players.append(player)

        # Parse created_date to ISO format for Supabase compatibility
        created_dt = datetime.fromtimestamp(submission.created_utc)
        created_date_iso = created_dt.isoformat()
        year = created_dt.year
        month = created_dt.month
        year_month = created_dt.strftime('%Y-%m')
        
        # Calculate engagement tier
        score = submission.score or 0
        num_comments = submission.num_comments or 0
        total_engagement = score + num_comments
        engagement_tier = 'high' if total_engagement > 100 else ('medium' if total_engagement > 20 else 'low')

        post_data = {
            # === STANDARD SUPABASE SCHEMA COLUMNS ===
            'source_platform': 'Reddit',
            'source_detail': submission.subreddit.display_name,
            'author': submission.author.name if submission.author else 'Unknown',
            'url': f"https://reddit.com{submission.permalink}",
            'text_content': combined_text,
            'created_date': created_date_iso,
            'engagement_score': float(score),
            'engagement_secondary': float(num_comments),
            'engagement_tier': engagement_tier,
            'relevance_score': float(title_relevance + text_relevance),
            'recovery_phase': recovery_phase,
            'mentioned_players': json.dumps(mentioned_players),  # JSON string for Supabase
            'is_achilles_related': bool('achilles' in combined_text.lower() or 'achillies' in combined_text.lower()),
            'is_quality_content': True,
            'uploaded_at': datetime.now().isoformat(),
            'text_length': len(combined_text),
            'year': year,
            'month': month,
            'year_month': year_month,
            # === REDDIT-SPECIFIC COLUMNS ===
            'num_comments_extracted': 0,
            'avg_comment_score': 0.0,
            'total_comment_words': 0,
            # === UNUSED COLUMNS (set to defaults) ===
            'num_replies_extracted': 0,
            'avg_reply_likes': 0.0,
            'total_reply_words': 0,
            'body_word_count': 0,
            'fetch_success': False,
        }

        # Process comments if enabled
        if include_comments:
            try:
                submission.comments.replace_more(limit=0) # Flatten comment tree
                comments = submission.comments.list()[:comment_limit]

                comment_texts = []
                comment_scores = []
                total_words = 0

                for comment in comments:
                    comment_text = comment.body
                    comment_texts.append(comment_text)
                    comment_scores.append(comment.score)
                    total_words += len(comment_text.split())

                post_data['num_comments_extracted'] = len(comment_texts)
                post_data['avg_comment_score'] = float(sum(comment_scores) / len(comment_scores)) if comment_scores else 0.0
                post_data['total_comment_words'] = total_words

                # Append comments to text_content for broader sentiment context
                post_data['text_content'] = f"{post_data['text_content']}\n\nComments:\n" + "\n".join(comment_texts)
                post_data['text_length'] = len(post_data['text_content'])

            except Exception as e:
                print(f"⚠️ Error processing comments for post {submission.id}: {e}")

        return post_data

    def scrape_subreddit(self, subreddit_name: str, sort_method: str = 'hot',
                         limit: int = 100, time_filter: str = 'all',
                         include_comments: bool = True,
                         comment_limit: int = 30) -> List[Dict]:
        """
        Scrape posts from a specific subreddit using PRAW WITH COMMENTS
        Args:
            subreddit_name: Name of subreddit
            sort_method: 'hot', 'new', 'top', 'rising'
            limit: Max posts to retrieve
            time_filter: 'all', 'day', 'week', 'month', 'year'
            include_comments: Whether to extract comments (default: True)
            comment_limit: Max comments per post (default: 30)
        Returns:
            List of post dictionaries with comments
        """
        if not self.reddit:
            print("❌ Reddit not initialized. Call setup_reddit_connection() first.")
            return []

        print(f"\n📊 Scraping r/{subreddit_name} {'WITH COMMENTS' if include_comments else ''}")
        print(f"🎯 Method: {sort_method}, Limit: {limit}, Comments: {comment_limit if include_comments else 0}")
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
            if sort_method == 'hot':
                posts = subreddit.hot(limit=limit)
            elif sort_method == 'new':
                posts = subreddit.new(limit=limit)
            elif sort_method == 'top':
                if time_filter == 'all':
                    posts = subreddit.top(limit=limit)
                elif time_filter == 'year':
                    posts = subreddit.top(limit=limit, time_filter='year')
                elif time_filter == 'month':
                    posts = subreddit.top(limit=limit, time_filter='month')
                elif time_filter == 'week':
                    posts = subreddit.top(limit=limit, time_filter='week')
                elif time_filter == 'day':
                    posts = subreddit.top(limit=limit, time_filter='day')
                else:
                    posts = subreddit.top(limit=limit) # default to all time
            elif sort_method == 'rising':
                posts = subreddit.rising(limit=limit)
            else:
                posts = subreddit.hot(limit=limit) # default to hot

            results = []
            processed_count = 0
            relevant_count = 0
            
            for submission in posts:
                processed_count += 1
                post_data = self.process_submission(submission, include_comments, comment_limit)
                
                # Only add if it meets minimum relevance threshold
                if post_data['relevance_score'] > 0:  # Adjust threshold as needed
                    results.append(post_data)
                    relevant_count += 1
                    
                    if relevant_count >= limit:
                        break
                        
            print(f"✅ Scraped {relevant_count}/{processed_count} relevant posts from r/{subreddit_name}")
            return results
        except Exception as e:
            print(f"❌ Error scraping r/{subreddit_name}: {str(e)}")
            return []

    def search_reddit(self, query: str, subreddit_name: str = 'all',
                      sort: str = 'relevance', time_filter: str = 'all', limit: int = 100,
                      include_comments: bool = True, comment_limit: int = 30) -> List[Dict]:
        """
        Search Reddit for specific content using PRAW - Same as working version
        """
        if not self.reddit:
            print("❌ Reddit not initialized. Call setup_reddit_connection() first.")
            return []

        print(f"\n🔍 Searching Reddit: '{query}' in r/{subreddit_name}")
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            
            if sort == 'relevance':
                search_results = subreddit.search(query, sort='relevance', time_filter=time_filter, limit=limit)
            elif sort == 'new':
                search_results = subreddit.search(query, sort='new', time_filter=time_filter, limit=limit)
            elif sort == 'top':
                search_results = subreddit.search(query, sort='top', time_filter=time_filter, limit=limit)
            else:
                search_results = subreddit.search(query, sort='relevance', time_filter=time_filter, limit=limit)

            results = []
            processed_count = 0
            relevant_count = 0
            for submission in search_results:
                processed_count += 1
                post_data = self.process_submission(submission, include_comments, comment_limit)
                
                # Add search results regardless of initial relevance if they contain the query terms
                # The query itself indicates relevance
                if query.lower() in post_data['combined_text'].lower():
                    results.append(post_data)
                    relevant_count += 1
                    
                    if relevant_count >= limit:
                        break
                        
            print(f"✅ Found {relevant_count}/{processed_count} relevant results for '{query}' in r/{subreddit_name}")
            return results
        except Exception as e:
            print(f"❌ Error searching Reddit for '{query}': {str(e)}")
            return []

    def comprehensive_scrape(self, max_posts_per_subreddit: int = 50,
                             include_comments: bool = True,
                             comment_limit: int = 30) -> pd.DataFrame:
        """
        Comprehensive scraping for TRACE project - EXPANDED from working version
        """
        if not self.reddit:
            print("❌ Reddit not initialized. Call setup_reddit_connection() first.")
            return pd.DataFrame()

        print("\n🚀 Starting Comprehensive TRACE Scraping with PRAW")
        print("=" * 60)
        all_data = []

        # 1. Scrape main NBA subreddits (same as working version)
        priority_subreddits = ['nba', 'nbadiscussion']
        for subreddit in priority_subreddits:
            # Hot posts
            posts = self.scrape_subreddit(
                subreddit, 'hot', max_posts_per_subreddit, include_comments=include_comments, comment_limit=comment_limit
            )
            all_data.extend(posts)
            # Top posts from last month
            posts = self.scrape_subreddit(
                subreddit, 'top', max_posts_per_subreddit, 'month', include_comments=include_comments, comment_limit=comment_limit
            )
            all_data.extend(posts)

        # 2. Scrape ALL 30 NBA team subreddits (EXPANDED)
        print(f"\n📊 Scraping All 30 NBA Team Subreddits...")
        for team_name, subreddits in self.nba_teams.items():
            for subreddit_name in subreddits:
                try:
                    # Hot posts
                    posts = self.scrape_subreddit(
                        subreddit_name, 'hot', max_posts_per_subreddit, include_comments=include_comments, comment_limit=comment_limit
                    )
                    all_data.extend(posts)
                    # Top posts from last month
                    posts = self.scrape_subreddit(
                        subreddit_name, 'top', max_posts_per_subreddit, 'month', include_comments=include_comments, comment_limit=comment_limit
                    )
                    all_data.extend(posts)
                    # Add small delay to be respectful
                    time.sleep(0.5)
                except Exception as e:
                    print(f"⚠️ Error scraping r/{subreddit_name}: {e}")

        # 3. Scrape additional NBA-related communities (EXPANDED)
        additional_communities = ['fantasybball', 'nbanalytics', 'nba_draft', 'Basketball']
        for subreddit in additional_communities:
            posts = self.scrape_subreddit(
                subreddit, 'hot', max_posts_per_subreddit, include_comments=include_comments, comment_limit=comment_limit
            )
            all_data.extend(posts)

        # 4. Expanded targeted searches (MORE COMPREHENSIVE)
        injury_searches = [
            'NBA injury report', 'achilles injury', 'achillies injury', 'achilles tendon injury',
            'foot injury', 'heel injury', 'plantar fasciitis', 'stress fracture',
            'ACL injury', 'MCL injury', 'meniscus tear', 'knee surgery',
            'back injury', 'spine injury', 'concussion', 'shoulder injury',
            'elbow injury', 'wrist injury', 'ankle sprain', 'calf strain',
            'hamstring injury', 'groin injury', 'hip flexor injury'
        ]
        print(f"\n🎯 Phase 5: Targeted Injury Searches...")
        for search_term in injury_searches:
            search_posts = self.search_reddit(search_term, 'nba', 'relevance', 'year', max_posts_per_subreddit)
            all_data.extend(search_posts)
            time.sleep(1) # Rate limiting

        # NEW: Keyword combination searches
        print(f"\n🎯 Phase 6: Keyword Combination Searches...")
        combo_posts = self.scrape_keyword_combinations(max_results_per_combo=15)
        all_data.extend(combo_posts)

        # 5. Convert to DataFrame and clean (same as working version)
        if all_data:
            df = pd.DataFrame(all_data)
            # Remove duplicates (by URL since post_id is not in Supabase schema)
            df = df.drop_duplicates(subset=['url'])
            # Sort by relevance
            df = df.sort_values('relevance_score', ascending=False)
            print(f"\n📊 COMPREHENSIVEING COMPLETE ")
            print(f"   • Total unique posts collected: {len(df)}")
            print(f"   • Average relevance score: {df['relevance_score'].mean():.2f}")
            print(f"   • Date range: {df['created_date'].min()} to {df['created_date'].max()}")
            return df
        else:
            print("❌ No relevant data collected")
            return pd.DataFrame()

    def analyze_scraped_data(self, df: pd.DataFrame) -> None:
        """
        Analyze and visualize scraped NBA injury data - Same as working version with enhancements
        """
        if df.empty:
            print("❌ No data to analyze")
            return

        print("\n📊 TRACE Comprehensive Data Analysis")
        print("=" * 50)

        # Basic statistics
        print(f"📈 Dataset Overview:")
        print(f"Total posts: {len(df)}")
        print(f"Date range: {df['created_date'].min()} to {df['created_date'].max()}")
        print(f"Average relevance score: {df['relevance_score'].mean():.2f}")
        print(f"Top 5 subreddits by volume:")
        print(df['source_detail'].value_counts().head())

        # Recovery phase distribution
        print(f"\n🔄 Recovery Phases:")
        print(df['recovery_phase'].value_counts())

        # Top mentioned players (now JSON string, need to parse)
        import json
        all_players = []
        for json_str in df['mentioned_players']:
            try:
                players = json.loads(json_str)
                all_players.extend(players)
            except:
                pass
        if all_players:
            print(f"\n👤 Top Mentioned Players:")
            player_counts = pd.Series(all_players).value_counts()
            print(player_counts.head(10))

        # Sample of high-relevance posts
        print(f"\n📋 High-Relevance Posts (Sample):")
        high_rel_df = df[df['relevance_score'] >= df['relevance_score'].quantile(0.8)].head(10)
        for idx, row in high_rel_df.iterrows():
             print(f"  {row['relevance_score']:.1f}| r/{row['source_detail']}| {row['recovery_phase']}| {row['text_content'][:60]}...")

        print(f"\n✅ Comprehensive analysis complete!")
        print(f"🎯 Dataset ready for FinBERT sentiment analysis!")

    def scrape_keyword_combinations(self, max_results_per_combo: int = 20) -> List[Dict]:
        """
        Search for specific keyword combinations that fans actually use
        """
        print("\n🔍 Searching Keyword Combinations...")
        all_results = []
        # Common fan discussion patterns
        combinations = [
            # Injury + emotion
            ('injury', 'sad'),
            ('injured', 'worried'),
            ('out for season', 'concerned'),
            ('achilles', 'worst'),
            ('recovery', 'optimistic'),
            ('return', 'excited'),
            ('surgery', 'praying'),
            ('healing', 'hope'),
            ('rehab', 'patient'),
            ('back', 'soon')
        ]
        
        for term1, term2 in combinations:
            try:
                query = f"{term1} {term2}"
                results = self.search_reddit(query, 'nba', 'relevance', 'year', max_results_per_combo)
                all_results.extend(results)
                time.sleep(1) # Rate limiting
            except Exception as e:
                print(f"⚠️ Error with combo '{term1} {term2}': {e}")
        
        print(f"✅ Found {len(all_results)} posts from keyword combinations")
        return all_results

print("✅ TRACE Comprehensive Scraper loaded successfully!")
print("🏀 Ready to scrape ALL 30 NBA teams and comprehensive sources!")