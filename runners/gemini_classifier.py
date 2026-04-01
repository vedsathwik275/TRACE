import json
import time
from typing import Dict, Any
import pandas as pd
import google.generativeai as genai
import os
from dotenv import load_dotenv
from text_sanitizer import sanitize_text

# Configure Gemini API with API key from .env
load_dotenv()
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
genai.configure(api_key=GEMINI_API_KEY)

SYSTEM_PROMPT = """You are an expert classifier for NBA Achilles injury recovery posts and discussions.

Your task is to classify whether a post is SUITABLE or UNSUITABLE for research on Achilles injury recovery narratives.

SUITABLE posts include:
- Personal recovery experiences and timelines
- Detailed injury descriptions and rehabilitation progress
- Medical updates or treatment discussions
- Player comeback stories and performance analysis
- Community discussions about recovery expectations
- Comparative analysis of different recovery trajectories

UNSUITABLE posts include:
- Generic injury news without recovery context
- Trade rumors or contract discussions
- Off-topic sports commentary
- Duplicate or spam content
- Low-quality or uninformative posts
- Posts primarily about other injuries

Respond ONLY with a valid JSON object in this exact format:
{
  "classification": "SUITABLE" or "UNSUITABLE",
  "confidence": 0.0 to 1.0,
  "reasoning": "brief explanation of classification decision",
  "recovery_phase": "one of: pre-injury, acute, early-recovery, mid-recovery, late-recovery, return-to-play, post-return, unknown",
  "key_entities": ["list", "of", "relevant", "players", "teams", "or", "concepts"]
}"""


def classify_record(row: pd.Series, max_retries: int = 3) -> Dict[str, Any]:
    """
    Classify an NBA Achilles injury post using Gemini 2.5 Flash.

    Args:
        row: pandas Series containing the post data
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        Dict with classification results or error information
    """
    # Extract required fields from the row
    fields = {
        'text_content': sanitize_text(row.get('text_content', '')),
        'source_platform': row.get('source_platform', ''),
        'recovery_phase': row.get('recovery_phase', ''),
        'mentioned_players': row.get('mentioned_players', ''),
        'is_achilles_related': row.get('is_achilles_related', ''),
        'engagement_score': row.get('engagement_score', ''),
        'created_date': row.get('created_date', '')
    }

    # Build user message with the data
    user_message = f"""Classify the following post:

Text Content: {fields['text_content']}
Source Platform: {fields['source_platform']}
Recovery Phase (if detected): {fields['recovery_phase']}
Mentioned Players: {fields['mentioned_players']}
Is Achilles Related: {fields['is_achilles_related']}
Engagement Score: {fields['engagement_score']}
Created Date: {fields['created_date']}

Provide your classification as a JSON object."""

    # Initialize the model
    model = genai.GenerativeModel(
        model_name='gemini-2.5-flash',
        generation_config={
            'temperature': 0.1,
            'top_p': 0.95,
            'max_output_tokens': 1024,
        },
        system_instruction=SYSTEM_PROMPT
    )

    # Retry loop
    for attempt in range(max_retries):
        try:
            # Generate content with system instruction
            response = model.generate_content(user_message)

            # Extract and parse JSON response
            response_text = response.text.strip()

            # Remove markdown code blocks if present
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            if response_text.startswith('```'):
                response_text = response_text[3:]
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            response_text = response_text.strip()

            # Parse JSON
            result = json.loads(response_text)

            # Validate required fields
            required_fields = ['classification', 'confidence', 'reasoning', 'recovery_phase', 'key_entities']
            if not all(field in result for field in required_fields):
                raise ValueError(f"Missing required fields in response. Got: {list(result.keys())}")

            # Validate classification value
            if result['classification'] not in ['SUITABLE', 'UNSUITABLE']:
                raise ValueError(f"Invalid classification: {result['classification']}")

            # Validate confidence range
            if not (0.0 <= result['confidence'] <= 1.0):
                raise ValueError(f"Confidence out of range: {result['confidence']}")

            return result

        except json.JSONDecodeError as e:
            error_msg = f"JSON parsing error on attempt {attempt + 1}: {str(e)}"
            if attempt == max_retries - 1:
                return {
                    'classification': 'ERROR',
                    'confidence': 0.0,
                    'reasoning': error_msg,
                    'recovery_phase': 'unknown',
                    'key_entities': [],
                    'error': error_msg,
                    'raw_response': response_text if 'response_text' in locals() else None
                }
            time.sleep(1 * (attempt + 1))  # Exponential backoff

        except Exception as e:
            error_msg = f"Error on attempt {attempt + 1}: {type(e).__name__}: {str(e)}"
            if attempt == max_retries - 1:
                return {
                    'classification': 'ERROR',
                    'confidence': 0.0,
                    'reasoning': error_msg,
                    'recovery_phase': 'unknown',
                    'key_entities': [],
                    'error': error_msg
                }
            time.sleep(1 * (attempt + 1))  # Exponential backoff

    # This should never be reached, but just in case
    return {
        'classification': 'ERROR',
        'confidence': 0.0,
        'reasoning': 'All retry attempts failed',
        'recovery_phase': 'unknown',
        'key_entities': [],
        'error': 'Maximum retries exceeded'
    }
