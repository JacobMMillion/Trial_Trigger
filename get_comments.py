from apify_client import ApifyClient
import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime, timezone
import json
from openai import OpenAI
import tiktoken



"""

This module provides utilities to fetch and process comments from social media posts
using Apify actors for Instagram and TikTok, and leverages OpenAI's API to filter comments 
that are relevant to our apps. It also includes helper functions for counting tokens and 
batching comments to respect token limits.

"""


# ----------------------------
# Environment & Credentials Setup
# ----------------------------
load_dotenv()  # This loads the environment variables from the .env file into os.environ

APIFY_API_KEY = os.environ.get("APIFY_API_KEY")
APIFY_CLIENT = ApifyClient(APIFY_API_KEY)

CONN_STR = os.getenv('DATABASE_URL')


# ----------------------------
# FETCH COMMENTS FROM A SOCIAL MEDIA POST.
# - SELECTS THE APPROPRIATE APIFY ACTOR BASED ON THE URL (INSTAGRAM OR TIKTOK).
# - RETURNS A LIST OF COMMENT STRINGS.
# ----------------------------
def get_comments(url):

    """
    For Instagram:
      - Input is a JSON with "directUrls" and "resultsLimit".
    For TikTok:
      - Input is a JSON with "postURLs", "commentsPerPost", and "maxRepliesPerComment".
    """

    # Determine which actor and input to use based on the URL
    # Return an empty array if Youtube or something else
    if "instagram.com" in url:
        actor_id = "apify/instagram-comment-scraper"
        run_input = {
            "directUrls": [url],
            "resultsLimit": 15,  # Number of comments to retrieve
        }
    elif "tiktok.com" in url:
        actor_id = "clockworks/tiktok-comments-scraper"
        run_input = {
            "postURLs": [url],
            "commentsPerPost": 15,       # Number of comments to retrieve
            "maxRepliesPerComment": 2     # Maximum number of replies per comment
        }
    else:
        return []
    

    try:
        # Call the actor
        run = APIFY_CLIENT.actor(actor_id).call(run_input=run_input)
        
        # Retrieve the default dataset ID from the run metadata.
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            raise ValueError("No dataset ID found in the run response.")
        
        # Fetch the dataset items using Apify API v2 endpoint.
        dataset_response = APIFY_CLIENT.dataset(dataset_id).list_items()
        data = dataset_response.items

        comments = []
        for item in data:
            comments.append(item["text"])
        
        return comments

    except Exception as e:
        print(f"Error retrieving comments for url {url}: {str(e)}")
        return None


# ----------------------------
# COUNT TOKENS IN A GIVEN TEXT.
# - USES TIKTOKEN WITH GPT-4 (cl100k_base ENCODING).
# - RETURNS THE NUMBER OF TOKENS.
# ----------------------------
def count_tokens(text):
    # Setup tiktoken encoding for GPT-4 (using the "cl100k_base" encoding)
    encoding = tiktoken.get_encoding("cl100k_base")
    """Returns the number of tokens in the provided text."""
    return len(encoding.encode(text))


# ----------------------------
# SPLIT COMMENTS INTO BATCHES BASED ON TOKEN LIMIT.
# - ENSURES EACH BATCH, INCLUDING A FIXED PROMPT OVERHEAD, DOES NOT EXCEED THE MAX TOKEN LIMIT.
# - RETURNS A LIST OF COMMENT BATCHES.
# ----------------------------
def split_into_batches(comments, max_tokens_per_batch=30000, prompt_overhead=200):
    """
    Splits a list of comment strings into batches where each batch's token count,
    including a fixed prompt overhead, stays below max_tokens_per_batch.
    
    Parameters:
        comments (list): A list of comment strings.
        max_tokens_per_batch (int): Maximum token count per batch (default set to 30,000).
        prompt_overhead (int): Fixed token count for the static prompt text.
        
    Returns:
        list: A list of batches (each batch is a list of comments).
    """
    batches = []
    current_batch = []
    current_tokens = prompt_overhead  # Account for the tokens in the prompt
    for comment in comments:
        tokens_in_comment = count_tokens(comment)
        # If adding this comment exceeds the max token count, start a new batch.
        if current_tokens + tokens_in_comment > max_tokens_per_batch:
            batches.append(current_batch)
            current_batch = [comment]
            current_tokens = prompt_overhead + tokens_in_comment
        else:
            current_batch.append(comment)
            current_tokens += tokens_in_comment
    if current_batch:
        batches.append(current_batch)
    return batches


# ----------------------------
# FILTER COMMENTS RELATED TO THE APP USING THE OPENAI API.
# - SPLITS COMMENTS INTO BATCHES TO STAY WITHIN TOKEN LIMITS.
# - USES CHATGPT (GPT-4) TO FILTER COMMENTS THAT ARE RELEVANT TO ENGAGEMENT WITH THE APP.
# - RETURNS THE FILTERED COMMENTS AS A LIST.
# ----------------------------
def get_comments_about_app(comments):

    # Instantiate the client using API key from environment variables.
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    # Split comments into batches based on token limits.
    batches = split_into_batches(comments, max_tokens_per_batch=30000, prompt_overhead=300)
    all_filtered_comments = []
    
    for batch in batches:

        # NOTE: This prompt is maybe not great, and maybe TODO: we want a different one for each app. But maybe we move this anyways.
        prompt = (
            "Below is a list of comments from influencer posts promoting our apps: Astra (an astrology app), Haven (a bible app), "
            "Saga (a generative writing app), and Berry (a women's health app). Your task is to filter and return only those comments that "
            "demonstrate a user action or intent related to engaging with one of our apps. Even a mention of the app is sufficient. This includes comments suggesting that the user "
            "downloaded, installed, signed up for a trial, expressed direct interest, or mentioned a specific action prompted by the app or its features. "
            "Do not include comments that only mention general topics (e.g., astrology, biblical themes, writing, or women's health) unless they also "
            "reference a direct engagement with the app. Err on the side of inclusion when in doubt. "
            "IMPORTANT: The output should be a JSON array of strings (each string should be one comment). Nothing more, including markdown. \n\n"
            "Comments:\n" + json.dumps(batch, indent=2)
        )
        
        # Call the ChatCompletion endpoint.
        chat_completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=500,
        )
        
        # Extract the text from the response.
        result_text = chat_completion.choices[0].message.content.strip()
        try:
            # Parse the response text as JSON to get the list of filtered comments.
            filtered_batch = json.loads(result_text)
            if isinstance(filtered_batch, list):
                all_filtered_comments.extend(filtered_batch)
            else:
                print("Unexpected JSON format, expected a list but got:", type(filtered_batch))
        except Exception as e:
            print("Error parsing JSON response:", e)
            print("Raw response:", result_text)
    
    return all_filtered_comments




# ----------------------------
# EXAMPLE USAGE (MAIN)
# - FETCH COMMENTS FROM A GIVEN POST URL (INSTAGRAM OR TIKTOK),
#   FILTER THEM FOR APP-RELATED CONTENT, AND OPTIONALLY LOG THE RESULTS TO THE DATABASE.
# ----------------------------
if __name__ == "__main__":
    # Replace with an Instagram or TikTok post URL

    # # Example for Tiktok
    # post_url = "https://www.tiktok.com/@andrea.bookster/video/7464988020970769695"

    # # Example for Instagram
    # # post_url = "https://www.instagram.com/reel/DDSO9TCvA75/"

    # # Fetch comments from the post
    # comments = ["Astra is a cool app!", "This should not be picked up", "I tried this app and liked it!", "It is not free, only a 7 day trial", "Astrology is cool", "Your account is a haven of safety for me"]

    # # Filter comments to those that only relate to the app
    # filtered_comments = get_comments_about_app(comments)

    # # Print
    # for comment in filtered_comments:
    #     print(comment)
    pass