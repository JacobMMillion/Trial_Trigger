from apify_client import ApifyClient
import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime, timezone
import json
from openai import OpenAI
import tiktoken

"""
get_comments returns a json object where "text" is the list of comments

log_comments logs the url and comments to the scraped_comments database, along with a timestamp
"""



load_dotenv()  # This loads the environment variables from the .env file into os.environ

APIFY_API_KEY = os.environ.get("APIFY_API_KEY")
APIFY_CLIENT = ApifyClient(APIFY_API_KEY)

CONN_STR = os.getenv('DATABASE_URL')


def get_comments(url):
    """
    Fetches comments from a social media post using the appropriate Apify actor
    and retrieves the output data from the default dataset.
    
    For Instagram:
      - Input is a JSON with "directUrls" and "resultsLimit".
    For TikTok:
      - Input is a JSON with "postURLs", "commentsPerPost", and "maxRepliesPerComment".
    
    After the run, it fetches the scraped comments from the dataset.
    
    Parameters:
      url (str): The URL of the Instagram or TikTok post.
    
    Returns:
      A list of comments
    """
    # Determine which actor and input to use based on the URL
    if "instagram.com" in url:
        actor_id = "apify/instagram-comment-scraper"
        run_input = {
            "directUrls": [url],
            "resultsLimit": 50,  # Number of comments to retrieve
        }
    elif "tiktok.com" in url:
        actor_id = "clockworks/tiktok-comments-scraper"
        run_input = {
            "postURLs": [url],
            "commentsPerPost": 50,       # Number of comments to retrieve
            "maxRepliesPerComment": 2     # Maximum number of replies per comment
        }
    else:
        raise ValueError("Unsupported URL. Please provide an Instagram or TikTok URL.")
    

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



def log_comments(url, comments):
    """
    Logs scraped comments into the scraped_comments table.

    Parameters:
      url (str): The URL for which comments were scraped.
      comments (list): The list of comment objects.
    """
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(CONN_STR)
        cursor = conn.cursor()

        # Get the current UTC time in ISO format
        event_time = datetime.now(timezone.utc).isoformat()

        # Convert the list of comments into a JSON string
        comments_json = json.dumps(comments)

        # Define the SQL to insert a new row
        insert_event = """
            INSERT INTO scraped_comments (
                url,
                comments,
                log_date
            ) VALUES (%s, %s, %s)
            RETURNING id;
        """

        # Execute the insert statement
        cursor.execute(insert_event, (url, comments_json, event_time))

        # Fetch the auto-generated ID of the inserted row
        event_id = cursor.fetchone()[0]

        # Commit changes and close the connection
        conn.commit()
        cursor.close()
        conn.close()

        print(f"Scraped comments logged with ID: {event_id}")
    except Exception as e:
        print(f"Error logging scraped comments: {str(e)}")
        return False
    


def count_tokens(text):
    # Setup tiktoken encoding for GPT-4 (using the "cl100k_base" encoding)
    encoding = tiktoken.get_encoding("cl100k_base")
    """Returns the number of tokens in the provided text."""
    return len(encoding.encode(text))


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


"""
Uses ChatGPT API to filter the comments to only those that may relate to the app, trial, or download
"""
def get_comments_about_app(comments):
    """
    Uses the new OpenAI SDK to filter a list of comment strings,
    returning only those comments that mention or relate to an app,
    a trial, or a download. This function splits the input comments
    into batches to stay within token limits before calling the API.

    Parameters:
        comments (list): A list of comment strings
        
    Returns:
        list: Filtered comments as a list of strings.
    """
    # Instantiate the client using your API key from environment variables.
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    # Split comments into batches based on token limits.
    batches = split_into_batches(comments, max_tokens_per_batch=30000, prompt_overhead=300)
    all_filtered_comments = []
    
    for batch in batches:
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
            model="gpt-4o",  # Use your desired model name.
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




# Example usage
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

    # # # Log the comments to the database
    # # log_comments(post_url, filtered_comments)

    # # Print
    # for comment in filtered_comments:
    #     print(comment)
    pass