# relevance_scorer.py
"""
Relevance scoring and classification for NBA Achilles injury content.

This module contains only scoring and classification logic — no scraping, no file I/O, no network calls.
"""

from scrapers.reddit_config import (
    KEYWORD_WEIGHTS,
    HYPER_RELEVANCE_THRESHOLD,
    TARGET_PLAYERS,
    PLAYER_ALIASES,
)


class TRACERelevanceScorer:
    """
    Relevance scorer for NBA Achilles injury content.

    Computes relevance scores based on keyword matching and player mentions,
    classifies content into recovery phases, and extracts player mentions.
    """

    def __init__(self) -> None:
        """
        Initialize the scorer with configuration constants.
        """
        self.keyword_weights = KEYWORD_WEIGHTS
        self.hyper_relevance_threshold = HYPER_RELEVANCE_THRESHOLD
        self.target_players = TARGET_PLAYERS
        self.player_aliases = PLAYER_ALIASES

    def compute_score(self, title: str, body: str) -> tuple[float, list[str]]:
        """
        Compute relevance score for a post based on keyword and player matches.

        Args:
            title: The post title string.
            body: The post body/selftext string.

        Returns:
            A tuple of (total_score, matched_keywords) where:
            - total_score: Float sum of all matched term weights and player bonuses.
            - matched_keywords: List of all matched keyword strings.
        """
        combined_text = f"{title} {body}".lower()
        total_score = 0.0
        matched_keywords: list[str] = []
        matched_players: set[str] = set()

        # Iterate through keyword categories
        for category, config in self.keyword_weights.items():
            weight = config["weight"]
            terms = config["terms"]

            for term in terms:
                if term.lower() in combined_text and term not in matched_keywords:
                    total_score += weight
                    matched_keywords.append(term)

        # Check for player mentions (full names)
        for player_name in self.target_players.keys():
            if player_name.lower() in combined_text and player_name not in matched_players:
                total_score += 5.0
                matched_players.add(player_name)

        # Check for player aliases
        for alias, full_name in self.player_aliases.items():
            if alias.lower() in combined_text and full_name not in matched_players:
                total_score += 5.0
                matched_players.add(full_name)

        # Bonus: Player + injury combination (catches headlines like "Durant injury update")
        injury_words = {"injury", "injured", "out", "hurt", "sidelined", "achilles", "calf", "ankle", "foot", "tendon"}
        has_injury_word = any(word in combined_text for word in injury_words)
        has_player = len(matched_players) > 0
        
        if has_player and has_injury_word:
            total_score += 3.0  # Bonus for player + injury combo

        return total_score, matched_keywords

    def is_hyper_relevant(self, title: str, body: str) -> bool:
        """
        Determine if content meets the hyper-relevance threshold.

        Args:
            title: The post title string.
            body: The post body/selftext string.

        Returns:
            True if the computed score meets or exceeds HYPER_RELEVANCE_THRESHOLD.
        """
        score, _ = self.compute_score(title, body)
        return score >= self.hyper_relevance_threshold

    def extract_players(self, text: str) -> list[str]:
        """
        Extract mentioned player names from text.

        Args:
            text: The text content to search for player mentions.

        Returns:
            Deduplicated list of full player name strings found in the text.
            If both an alias and full name appear, only the full name is returned.
        """
        text_lower = text.lower()
        found_players: set[str] = set()

        # First, check for full player names
        for player_name in self.target_players.keys():
            if player_name.lower() in text_lower:
                found_players.add(player_name)

        # Then check aliases, but only add if the full name wasn't already found
        for alias, full_name in self.player_aliases.items():
            if alias.lower() in text_lower and full_name not in found_players:
                found_players.add(full_name)

        return list(found_players)

    def detect_recovery_phase(self, text: str) -> str:
        """
        Classify text into a recovery phase category.

        Args:
            text: The text content to classify.

        Returns:
            One of five strings:
            - "immediate_post_injury": Moment of injury confirmation.
            - "surgery_treatment": Surgical intervention phase.
            - "rehabilitation": Recovery and rehab phase.
            - "return_anticipation": Preparing to return to play.
            - "general": Default if no phase indicators match.
        """
        text_lower = text.lower()

        # Priority 1: Immediate post-injury
        immediate_phrases = [
            "went down",
            "confirmed tear",
            "rupture confirmed",
            "season over",
            "career ending",
            "just injured",
        ]
        for phrase in immediate_phrases:
            if phrase in text_lower:
                return "immediate_post_injury"

        # Priority 2: Surgery/treatment
        surgery_phrases = [
            "surgery scheduled",
            "post surgery",
            "post-op",
            "surgical repair",
            "operation",
            "medical team",
        ]
        for phrase in surgery_phrases:
            if phrase in text_lower:
                return "surgery_treatment"

        # Priority 3: Rehabilitation
        rehab_phrases = [
            "ahead of schedule",
            "rehab",
            "physical therapy",
            "working out",
            "training",
            "setback",
            "progressing",
            "milestone",
        ]
        for phrase in rehab_phrases:
            if phrase in text_lower:
                return "rehabilitation"

        # Priority 4: Return anticipation
        return_phrases = [
            "return date",
            "cleared to play",
            "game time decision",
            "minutes restriction",
            "comeback",
            "first game back",
            "return to court",
        ]
        for phrase in return_phrases:
            if phrase in text_lower:
                return "return_anticipation"

        # Default
        return "general"


# =============================================================================
# Test Cases
# =============================================================================

if __name__ == "__main__":
    scorer = TRACERelevanceScorer()

    # Test Case 1: High-relevance text (should score > 15)
    # Realistic fan post about Achilles rupture and surgery
    test1_title = "Kevin Durant suffers torn Achilles in Finals Game 5"
    test1_body = """
    Devastating news. Kevin Durant went down with a ruptured achilles tendon.
    The achilles tear was confirmed by the medical team. Surgery is scheduled
    for next week. This is a career-ending injury for many players. Achilles
    rupture recovery timeline is typically 12-18 months. Prayers up for KD.
    """

    score1, keywords1 = scorer.compute_score(test1_title, test1_body)
    print("=" * 60)
    print("TEST CASE 1: High-relevance (Achilles rupture + surgery)")
    print("=" * 60)
    print(f"Score: {score1}")
    print(f"Is Hyper-Relevant: {scorer.is_hyper_relevant(test1_title, test1_body)}")
    print(f"Matched Keywords: {keywords1}")
    print(f"Players Mentioned: {scorer.extract_players(test1_title + ' ' + test1_body)}")
    print(f"Recovery Phase: {scorer.detect_recovery_phase(test1_body)}")
    print()

    # Test Case 2: Borderline text (may or may not reach 15)
    # General injury discussion with some relevant terms
    test2_title = "Injury report: Multiple players out tonight"
    test2_body = """
    The injury report shows several players sidelined. One player has a calf strain
    and is undergoing physical therapy. Another is working on recovery and rehab.
    The team says he's progressing well and should return soon. Timeline is unclear.
    """

    score2, keywords2 = scorer.compute_score(test2_title, test2_body)
    print("=" * 60)
    print("TEST CASE 2: Borderline (General injury discussion)")
    print("=" * 60)
    print(f"Score: {score2}")
    print(f"Is Hyper-Relevant: {scorer.is_hyper_relevant(test2_title, test2_body)}")
    print(f"Matched Keywords: {keywords2}")
    print(f"Players Mentioned: {scorer.extract_players(test2_title + ' ' + test2_body)}")
    print(f"Recovery Phase: {scorer.detect_recovery_phase(test2_body)}")
    print()

    # Test Case 3: Irrelevant text (clearly not injury-related)
    # Game highlights and stats discussion
    test3_title = "Game Thread: Lakers vs Celtics - Championship rematch!"
    test3_body = """
    What a game! The Lakers are dominating tonight. LeBron with 35 points,
    12 rebounds, and 8 assists. The Celtics can't stop him. This rivalry
    never gets old. Who do you think wins the championship this year?
    """

    score3, keywords3 = scorer.compute_score(test3_title, test3_body)
    print("=" * 60)
    print("TEST CASE 3: Irrelevant (Game discussion, no injury)")
    print("=" * 60)
    print(f"Score: {score3}")
    print(f"Is Hyper-Relevant: {scorer.is_hyper_relevant(test3_title, test3_body)}")
    print(f"Matched Keywords: {keywords3}")
    print(f"Players Mentioned: {scorer.extract_players(test3_title + ' ' + test3_body)}")
    print(f"Recovery Phase: {scorer.detect_recovery_phase(test3_body)}")
    print()
