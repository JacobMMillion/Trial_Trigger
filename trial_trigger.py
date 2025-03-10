import psycopg2
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import re
from apify_client import ApifyClient

from get_comments import get_comments
from get_comments import get_comments_about_app

# Load environment variables and set connection string.
load_dotenv()
CONN_STR = os.getenv('DATABASE_URL')

APIFY_API_KEY = os.environ.get("APIFY_API_KEY")
APIFY_CLIENT = ApifyClient(APIFY_API_KEY)


def trial_trigger(app_name):
    """
    Check the last 14 days of trial counts (grouped by date) from the NewTrials table
    for the specified app. Calculate the average delta between consecutive days.
    If the increase (i.e. current delta) between the last two days exceeds twice the average delta,
    trigger the view scraper.

    Logged in UTC. This is important as we use it as a check to see if there has been a log for
    the day already.

    Returns True if the trigger is fired; otherwise, False.
    """
    # Determine our date range (last 14 days, including today)
    now = datetime.now(timezone.utc).date()  # current UTC date
    start_date = now - timedelta(days=13)    # 14 days total

    # Connect to the database.
    conn = psycopg2.connect(CONN_STR)
    cursor = conn.cursor()

    # Query the NewTrials table to get daily trial counts.
    # We group by the date portion of original_purchase_date_dt.
    upper_bound = now + timedelta(days=1)  # include the entire current day
    query = """
        SELECT DATE(original_purchase_date_dt) AS trial_date, COUNT(*) AS trial_count
        FROM NewTrials
        WHERE app_name = %s
          AND original_purchase_date_dt >= %s
          AND original_purchase_date_dt < %s
        GROUP BY trial_date
        ORDER BY trial_date;
    """
    cursor.execute(query, (app_name, start_date, upper_bound))
    rows = cursor.fetchall()

    # Build a dictionary with one entry per day over the 14-day period, initializing counts to 0.
    daily_counts = {}
    for i in range(14):
        day = start_date + timedelta(days=i)
        daily_counts[day] = 0

    # Update our dictionary with the actual counts returned by the query.
    for row in rows:
        trial_date, trial_count = row
        daily_counts[trial_date] = trial_count

    cursor.close()
    conn.close()

    # Create a sorted list of dates and corresponding counts.
    sorted_dates = sorted(daily_counts.keys())
    counts = [daily_counts[dt] for dt in sorted_dates]

    if len(counts) < 2:
        print("Not enough data to compute deltas.")
        return False

    # Compute the absolute differences between each consecutive day.
    deltas = [abs(counts[i] - counts[i-1]) for i in range(1, len(counts))]
    average_delta = sum(deltas) / len(deltas) if deltas else 0

    # make negative if decreasing
    curr_delta = counts[-1] - counts[-2]

    threshold = average_delta  # Trigger if the increase is greater than threshold

    # # TESTING
    # threshold = -99999

    # Detailed logging for debugging.
    print(f"Date range: {sorted_dates[0]} to {sorted_dates[-1]}")
    print("Daily counts:")
    for dt, count in zip(sorted_dates, counts):
        print(f"  {dt.isoformat()}: {count}")
    print("Deltas between consecutive days:")
    for i in range(1, len(sorted_dates)):
        print(f"  Delta from {sorted_dates[i-1].isoformat()} to {sorted_dates[i].isoformat()}: {deltas[i-1]}")
    
    print(f"Average delta: {average_delta:.2f}")
    print(f"Current delta (last two days): {curr_delta}")
    print(f"Threshold: {threshold:.2f}")

    # Trigger if exceeds threshold.
    if (curr_delta) > threshold:

        # Check if a trigger event has already been logged today.
        try:
            conn = psycopg2.connect(CONN_STR)
            cursor = conn.cursor()
            check_query = """
                SELECT COUNT(*) FROM TrialTriggerEvents
                WHERE app = %s AND event_time::date = %s;
            """
            cursor.execute(check_query, (app_name, now))
            count_already_triggered = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            if count_already_triggered > 0:
                print("Trigger already fired for today. Skipping new trigger.")
                return False
        except Exception as e:
            print(f"Error checking trial trigger events: {str(e)}")
            return False

        # Insert a row into TrialTriggerEvents.
        try:
            conn = psycopg2.connect(CONN_STR)
            cursor = conn.cursor()
            event_time = datetime.now(timezone.utc)
            insert_event = """
                INSERT INTO TrialTriggerEvents (
                    event_time,
                    trial_count,
                    average_delta,
                    current_delta,
                    threshold,
                    app
                ) VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id;
            """
            cursor.execute(insert_event, (
                event_time,
                counts[-1],  # current trial count (latest day)
                average_delta,
                curr_delta,
                threshold,
                app_name
            ))
            event_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Trial trigger event logged with ID: {event_id}")
        except Exception as e:
            print(f"Error logging trial trigger event: {str(e)}")
            return False

        # Call view scraper and pass the event ID.
        trigger_view_scraper(app_name, event_id)
        return True
    else:
        print("No significant upward change detected; no trigger required.")
        return False
    



def trigger_view_scraper(app_name, event_id):
    """
    Iterate over the DailyVideoData table and aggregate all video records
    (using the post_url column) from the past two weeks for the given app.
    For each post_url, select only the most recent log (based on log_time)
    along with additional columns (view_count, comment_count, caption,
    create_time, log_time, num_likes). Then print the results sorted by
    log_time descending.
    """

    # capitalize first letter
    app_name = app_name.capitalize()

    # Calculate threshold: two weeks ago (using UTC)
    threshold_date = datetime.now(timezone.utc) - timedelta(days=14)

    # Connect to the database
    conn = psycopg2.connect(CONN_STR)
    cursor = conn.cursor()

    # Use DISTINCT ON to get, for each post_url posted in last 2 weeks, the row with the latest log_time.
    query = """
        SELECT DISTINCT ON (post_url)
            id,
            post_url,
            creator_username,
            marketing_associate,
            app,
            view_count,
            comment_count,
            caption,
            create_time,
            log_time,
            num_likes
        FROM DailyVideoData
        WHERE create_time >= %s AND app = %s
        ORDER BY post_url, log_time DESC;
    """

    cursor.execute(query, (threshold_date, app_name))
    rows = cursor.fetchall()

    # Optional: sort the rows by create_time descending (most recent first)
    rows = [row for row in rows if row[8] is not None]
    rows.sort(key=lambda x: x[8], reverse=True)

    cursor.close()
    conn.close()

    print("Processing videos from the past 2 weeks:")
    for row in rows:
        # Unpack all columns (id, post_url, creator_username, marketing_associate,
        # app, view_count, comment_count, caption, create_time, log_time, num_likes)
        _, post_url, creator_username, marketing_associate, _, old_view_count, old_comment_count, caption, create_time, log_time, old_num_likes = row
        print(f"Processing URL: {post_url}")
        print(f"  Previous Metrics -> Views: {old_view_count}, Comments: {old_comment_count}, Likes: {old_num_likes}")

        # Get new metrics from Apify.
        result = hit_apify(post_url)
        if result is None:
            print(f"  Skipping URL {post_url} due to API failure.")
            continue

        username, new_view_count, new_comment_count, new_likes = result

        # Compute deltas.
        delta_views = new_view_count - old_view_count if new_view_count is not None else None
        delta_comments = new_comment_count - old_comment_count if new_comment_count is not None else None
        delta_likes = new_likes - old_num_likes if new_likes is not None else None

        # Get the app comments
        comments = get_comments(post_url)
        app_comments = get_comments_about_app(comments)
        print("Got comments, and filtered to those about the app.")

        print(f"  Updated Metrics -> Views: {new_view_count}, Comments: {new_comment_count}, Likes: {new_likes}")
        print(f"  Deltas          -> ΔViews: {delta_views}, ΔComments: {delta_comments}, ΔLikes: {delta_likes}\n")

        # Log these values in VideoMetricDeltas.
        try:
            conn = psycopg2.connect(CONN_STR)
            cursor = conn.cursor()
            insert_query = """
                INSERT INTO VideoMetricDeltas (
                    trial_trigger_event_id,
                    post_url,
                    creator_username,
                    marketing_associate,
                    old_view_count,
                    new_view_count,
                    delta_views,
                    old_comment_count,
                    new_comment_count,
                    delta_comments,
                    old_likes,
                    new_likes,
                    delta_likes,
                    app_comments
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (
                event_id,
                post_url,
                creator_username,
                marketing_associate,
                old_view_count,
                new_view_count,
                delta_views,
                old_comment_count,
                new_comment_count,
                delta_comments,
                old_num_likes,
                new_likes,
                delta_likes,
                app_comments
            ))
            conn.commit()
            cursor.close()
            conn.close()
            print(f"  Logged metrics delta for URL: {post_url}\n")
        except Exception as e:
            print(f"  Error logging metrics for URL {post_url}: {str(e)}\n")



def hit_apify(url):

    tiktok_regex = r"tiktok"
    instagram_regex = r"instagram"

    if re.search(tiktok_regex, url):
        url_type = "tiktok"
    elif re.search(instagram_regex, url):
        url_type = "instagram"
    else:
        print(f"URL '{url}' does not match TikTok or Instagram. Skipping.")
        return 0    

    try:
        # Prepare the Actor input based on URL type
        if url_type == "tiktok":  # TikTok  
            run_input = {
                "excludePinnedPosts": True,
                "postURLs": [url],
                "resultsPerPage": 1,
                "shouldDownloadCovers": False,
                "shouldDownloadSlideshowImages": False,
                "shouldDownloadSubtitles": False,
                "shouldDownloadVideos": False,
                "searchSection": "",
                "maxProfilesPerQuery": 10
            }
        elif url_type == "instagram":  # Instagram
            run_input = {
                "addParentData": False,
                "directUrls": [url],
                "enhanceUserSearchWithFacebookPage": False,
                "isUserReelFeedURL": False,
                "isUserTaggedFeedURL": False,
                "resultsLimit": 1,
                "resultsType": "details",
                "searchLimit": 1,
                "searchType": "hashtag"
            }

        # Choose the correct actor based on URL type
        if url_type == "tiktok":
            actor_link = "clockworks/free-tiktok-scraper"
        elif url_type == "instagram":
            actor_link = "apify/instagram-scraper"

        # Set keys based on URL type
        if url_type == "tiktok":
            view_key = "playCount"
            timestamp_key = "createTimeISO"
            comment_key = "commentCount"
            caption_key = "text"
        elif url_type == "instagram":
            view_key = "videoPlayCount"
            timestamp_key = "timestamp"
            comment_key = "commentsCount"
            caption_key = "caption"

        # Run the Actor for the single URL and wait for it to finish
        run = APIFY_CLIENT.actor(actor_link).call(run_input=run_input)

        # Retrieve and update the view count for the current URL
        item = next(APIFY_CLIENT.dataset(run["defaultDatasetId"]).iterate_items(), None)

        if item:
            view_count = item.get(view_key, 0)
            comment_count = item.get(comment_key, None)
            
            # Retrieve the likes count based on platform
            if url_type == "tiktok":
                likes_count = item.get("diggCount", 0)
            elif url_type == "instagram":
                likes_count = item.get("likesCount", 0)

            # Extract the username based on platform
            if url_type == "tiktok":
                author_meta = item.get("authorMeta", {})
                username = author_meta.get("name", None)
            elif url_type == "instagram":
                username = item.get("ownerUsername", None)


            return username, view_count, comment_count, likes_count
        
        else:
            return None

    except Exception as e:
        print(f"Error processing url {url}: {str(e)}")
        return None



# MAIN, run for each app
if __name__ == "__main__":
    # Define your app names properly
    APP_NAMES = ["saga", "berry", "haven", "astra"]

    for app in APP_NAMES:
        if trial_trigger(app):
            print(app, ": Threshold exceeded, running scraper")
        else:
            print(app, ": Threshold NOT exceeded, NOT running scraper")