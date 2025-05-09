import psycopg2
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import re
from apify_client import ApifyClient
import smtplib
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from get_comments import get_comments
from get_comments import get_comments_about_app


"""
This program monitors trial sign-up activity for various apps and triggers further data analysis when increased activity is detected.

DEPRECATED:
– Daily Trigger:
  * Looks at the last 30 days of trial counts (UTC), grouped by calendar date.
  * Computes the median of the first 29 days (excluding today).
  * Fires if today's count exceeds 75% of that median.
  * If already fired today, requires an additional Δ of (median_value * .75 + 200) above the prior trigger's count.

THIS IS LIVE:
– Hourly Trigger:
  * Looks at the last 3 full days (72 hours) of trial counts (UTC), grouped by hour.
  * Computes the median of the first 71 hours.
  * Fires if the most recent full hour's count exceeds 1.35× that median plus 4 and is a 2 hour peak.
  * Ensures only one hourly event per 3 UTC hours.

– When either trigger fires:
  1. Logs a new row in `TrialTriggerEvents` with type = 'daily' or 'hourly'.
  2. Runs `trigger_view_scraper` to fetch and delta‐compute video metrics for the past 21 days.
  3. Inserts those deltas into `VideoMetricDeltas` and updates `DailyVideoData`.
  4. Sends an email notification summarizing the top three videos for that event.

Environment, DB connection, and credentials are loaded via `.env`. Uses psycopg2 for Postgres, Apify for scraping video metrics, and SMTP for notifications.
"""

# ----------------------------
# Environment & Credentials Setup
# ----------------------------

# Load environment variables and set connection string.
load_dotenv()
CONN_STR = os.getenv('DATABASE_URL')

APIFY_API_KEY = os.environ.get("APIFY_API_KEY")
APIFY_CLIENT = ApifyClient(APIFY_API_KEY)

# ----------------------------
# DRIVER FUNCTION
# FIRST TRIES TO TRIGGER FOR DAILY
# IF DAILY TRIGGER DOES NOT FIRE, THEN TRIES TO TRIGGER FOR HOURLY
# RETURNS TRUE IF EITHER TRIGGER IS FIRED
# ----------------------------
def trial_trigger(app_name):
    # print(f"---Checking daily trigger for {app_name}...---")
    # # First, check the daily trigger
    # if daily_trigger(app_name):
    #     print(f"---Daily trigger fired for {app_name}.---")
    #     return True
    # If daily trigger did not fire, check hourly trigger
    # print(f"---Daily trigger did not fire for {app_name}. Checking hourly trigger...---")
    
    print("---Checking hourly trigger for", app_name, "...---")
    print("---We no longer check for a daily trigger---")
    if hourly_trigger(app_name):
        print(f"---Hourly trigger fired for {app_name}.---")
        return True
    
    print(f"No significant activity detected for {app_name}. No triggers fired.")
    return False

# ----------------------------
# CHECK 3 DAYS OF TRIAL COUNTS FOR A GIVEN APP (GROUPED BY HOUR) FROM THE `NewTrials` TABLE.
# THIS TABLE LOGGED IN UTC
# IF THE PAST (full) HOUR TRIAL #s EXCEEDS THE (HISTORICAL MEDIAN + 65%, essentially 1.65x) + 4 HOURLY TRIAL COUNT, TRIGGER FIRES 
# ALSO ENSURES THAT THE CURRENT HOUR IS A PEAK VS THE LAST 2 HOURS AND HASN'T BEEN AN EVENT WITHIN THE PAST 3 HOURS
# RETURNS TRUE IF TRIGGER FIRED
# ----------------------------
def hourly_trigger(app_name):
    # 1) Establish our 3-day window, rounded to the current hour
    now = datetime.now(timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    start_time   = current_hour - timedelta(days=3)  # 72 hours back

    # 2) Fetch counts per hour from NewTrials
    upper_bound = current_hour + timedelta(hours=1)
    conn = psycopg2.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
          date_trunc('hour', original_purchase_date_dt) AS trial_hour,
          COUNT(*) AS trial_count
        FROM NewTrials
        WHERE app_name = %s
          AND original_purchase_date_dt >= %s
          AND original_purchase_date_dt <  %s
        GROUP BY trial_hour
        ORDER BY trial_hour
    """, (app_name, start_time, upper_bound))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    # 3) Build a full 72-hour dict, defaulting to 0
    hourly_counts = {}
    for i in range(72):
        slot = start_time + timedelta(hours=i)
        hourly_counts[slot] = 0
    # Fill in actual counts
    for trial_hour, count in rows:
        # Ensure timezone‐aware
        if trial_hour.tzinfo is None:
            trial_hour = trial_hour.replace(tzinfo=timezone.utc)
        
        trial_hour = trial_hour.replace(minute=0, second=0, microsecond=0)

        # Only overwrite if we pre-seeded that slot
        # CRITICAL: This ensures we only count the full hours that have passed (otherwise indexing -1 can be inconsistent)
        if trial_hour in hourly_counts:
            hourly_counts[trial_hour] = count

    # 4) Sort and split into historical vs. current
    sorted_hours = sorted(hourly_counts.keys())
    counts       = [hourly_counts[hr] for hr in sorted_hours]
    if len(counts) < 2:
        print("Not enough data for hourly median.")
        return False

    historical = counts[:-1]         # first 71 hours
    current    = counts[-1]         # latest hour (the latest hour that has passed in entirety - not the technical current hour)
    median_val = (
      sorted(historical)[len(historical)//2]
      if len(historical)%2==1
      else (sorted(historical)[len(historical)//2 - 1] 
          + sorted(historical)[len(historical)//2]) / 2
    )
    threshold  = median_val * 1.35 + 4

    print(f"Hourly window: {sorted_hours[0]} → {sorted_hours[-1]}")
    print("Counts per hour:")
    for hr, cnt in zip(sorted_hours, counts):
        print(f"  {hr.isoformat()}: {cnt}")
    print(f"Historical median (past 71h): {median_val}")
    print(f"Threshold (1.35× median + 4): {threshold}")
    print(f"Current hour count: {current}")

    # 5) Only fire if we exceed threshold
    if current <= threshold:
        print("No spike this hour; skipping.")
        return False
    
    # Only fire if we are at a peak vs the last 2 hours
    prev1 = counts[-2]   # 1 hour ago
    prev2 = counts[-3]   # 2 hours ago
    if not (current > prev1 and current > prev2):
        print("Not a peak vs. last 3h; skipping trigger.")
        return False

    # 6) Skip if any hourly trigger in the past 3 hours
    three_hours_ago = current_hour - timedelta(hours=3)

    conn = psycopg2.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1
          FROM TrialTriggerEvents
         WHERE app = %s
           AND event_type = 'hourly'
           AND event_time >= %s
         LIMIT 1
    """, (app_name, three_hours_ago))
    already = cursor.fetchone()
    cursor.close()
    conn.close()
    if already:
        print("An hourly trigger fired in the last 3 hours; skipping.")
        return False

    # 7) Log the new hourly event
    conn = psycopg2.connect(CONN_STR)
    cursor = conn.cursor()
    event_time = datetime.now(timezone.utc)
    cursor.execute("""
        INSERT INTO TrialTriggerEvents
          (event_time, trial_count, average_delta, current_delta,
           threshold, app, event_type)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """, (
        event_time,
        current,    # trial count this hour
        None,       # average_delta (unused)
        None,       # current_delta (unused)
        threshold,
        app_name,
        'hourly'
    ))
    event_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Logged hourly trigger event ID {event_id}.")

    # 8) Fire downstream actions
    trigger_view_scraper(app_name, event_id)
    send_notification_email(app_name.capitalize(), event_id, 'hourly')

    return True


# ----------------------------
# CHECK 30 DAYS OF TRIAL COUNTS FOR A GIVEN APP (GROUPED BY DATE) FROM THE `NewTrials` TABLE.
# THIS TABLE LOGGED IN UTC
# IF TODAY'S TRIAL COUNT EXCEEDS 75% OF THE HISTORICAL MEDIAN DAILY TRIAL COUNT (EXCLUDING TODAY), TRIGGER FIRES
# RETURNS TRUE IF TRIGGER FIRED
# ----------------------------
def daily_trigger(app_name):

    # Determine our date range (last 30 days, including today)
    now = datetime.now(timezone.utc).date()  # current UTC date
    start_date = now - timedelta(days=29)    # 30 days total

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

    # Build a dictionary with one entry per day over the 30-day period, initializing counts to 0.
    daily_counts = {}
    for i in range(30):
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

    if not counts:
        print("No data available to compute the median.")
        return False

    # Compute the historical median trial value using all days except the current day.
    historical_counts = counts[:-1]  # exclude the latest day (today)
    sorted_hist_counts = sorted(historical_counts)
    n = len(sorted_hist_counts)
    if n % 2 == 1:
        median_value = sorted_hist_counts[n // 2]
    else:
        median_value = (sorted_hist_counts[n // 2 - 1] + sorted_hist_counts[n // 2]) / 2

    # Today's trial count is the last value.
    current_trial_value = counts[-1]

    # Detailed logging for debugging.
    print(f"Date range: {sorted_dates[0]} to {sorted_dates[-1]}")
    print("Daily counts:")
    for dt, count in zip(sorted_dates, counts):
        print(f"  {dt.isoformat()}: {count}")

    THRESHOLD = median_value * .75 # threshold is 75% of the median

    print(f"Historical median daily trial value (excluding current day): {median_value}")
    print(f"Threshold: {THRESHOLD}")
    print(f"Current daily trial value: {current_trial_value}")

    # Trigger if exceeds threshold.
    if current_trial_value > THRESHOLD:

        # Check the most recent DAILY trigger event for this app.
        try:
            conn = psycopg2.connect(CONN_STR)
            cursor = conn.cursor()
            check_query = """
                SELECT trial_count, event_time
                FROM TrialTriggerEvents
                WHERE app = %s AND event_time::date = %s AND event_type = 'daily'
                ORDER BY event_time DESC
                LIMIT 1;
            """
            cursor.execute(check_query, (app_name, now))
            result = cursor.fetchone()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error checking trial trigger events: {str(e)}")
            return False

        # If there is a previous DAILY event for the current day, only trigger if the current trial count has increased sufficiently.
        if result:
            last_trial_value, last_event_time = result

            # Minimum required increase from previous trial count in event
            MIN_INCREASE_THRESHOLD = (median_value * .75) + 200

            # UPDATE NEW THRESHOLD
            THRESHOLD = last_trial_value + MIN_INCREASE_THRESHOLD
            
            if current_trial_value < THRESHOLD:
                return False

        # Proceed to log the event since increase is sufficient in either case (previous daily trigger or not).
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
                    app,
                    event_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """
            cursor.execute(insert_event, (
                event_time,
                counts[-1],  # current trial count (latest day)
                None,
                None,
                THRESHOLD,
                app_name,
                "daily"  # Indicate this is a daily trigger
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

        # Senda  notification email
        send_notification_email(app_name.capitalize(), event_id, "daily")

        # And finally, return True as the trigger fired
        return True
    
    else:
        return False
    

# ----------------------------
# AGGREGATE VIDEO RECORDS FROM THE `DailyVideoData` TABLE FOR THE PAST 10 DAYS.
# FOR EACH `post_url`, SELECT THE MOST RECENT LOG (BASED ON `log_time`)
# ALONG WITH ADDITIONAL COLUMNS: `view_count`, `comment_count`, `caption`,
# `create_time`, `log_time`, AND `num_likes`.
# ----------------------------
def trigger_view_scraper(app_name, event_id):

    # Capitalize first letter of the app, as this is how it is logged in `DailyVideoData`
    app_name = app_name.capitalize()

    # Calculate threshold: 21 days ago (using UTC)
    threshold_date = datetime.now(timezone.utc) - timedelta(days=21)

    # Connect to the database
    conn = psycopg2.connect(CONN_STR)
    cursor = conn.cursor()

    # Use DISTINCT ON to get, for each post_url posted in last 21 days, the row with the latest log_time.
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
            num_likes,
            share_count
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

    print("Processing videos from the past 21 days:")

    # Process each row concurrently.
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {executor.submit(process_video_row, row, event_id): row for row in rows}
        for future in as_completed(future_to_row):
            row = future_to_row[future]
            try:
                result = future.result()
                print(f"Finished processing: {result}")
            except Exception as e:
                print(f"Error processing URL {row[1]}: {e}")

    
# ----------------------------
# PROCESS A SINGLE VIDEO ROW:
# - FETCH NEW METRICS VIA APIFY FOR THE GIVEN URL.
# - CALCULATE DELTAS BETWEEN NEW AND OLD METRIC VALUES (views, comments, likes, shares).
# - RETRIEVE COMMENTS AND FILTER THEM TO EXTRACT APP-RELATED FEEDBACK.
# - UPDATE THE `VideoMetricDeltas` TABLE WITH THE METRIC DIFFERENCES.
# - LOG THE UPDATED METRICS TO THE `DailyVideoData` TABLE FOR FUTURE REFERENCE.
# ----------------------------
def process_video_row(row, event_id):

    # Unpack columns from the previous event
    _, post_url, creator_username, marketing_associate, app, old_view_count, old_comment_count, caption, create_time, log_time, old_num_likes, old_num_shares = row
    print(f"Processing URL: {post_url}")
    print(f"  Previous Metrics -> Views: {old_view_count}, Comments: {old_comment_count}, Likes: {old_num_likes}, Shares: {old_num_shares}")

    # Get new metrics from Apify.
    result = hit_apify(post_url)
    if result is None:
        print(f"  Skipping URL {post_url} due to API failure.")
        return f"Skipped {post_url}"

    username, new_view_count, new_comment_count, new_likes, new_shares = result

    # Compute deltas.
    delta_views = new_view_count - old_view_count if new_view_count is not None else None
    delta_comments = new_comment_count - old_comment_count if new_comment_count is not None else None
    delta_likes = new_likes - old_num_likes if new_likes is not None else None
    delta_shares = new_shares - old_num_shares if new_shares is not None else None

    if delta_views is not None and delta_views < 0:
        print(f"  Skipping URL {post_url} due to negative delta views ({delta_views}).")
        return f"Skipped {post_url} due to negative delta views"

    # NOTE: We used to get the comments for each URL. This is expensive, and better done in one go, as opposed to repeatedly here
    # SO WE NO LONGER DO THIS HERE.
    # # Get the app comments.
    # comments = get_comments(post_url)
    # print("Comments:", comments)
    # app_comments = get_comments_about_app(comments)
    # print("App Comments:", app_comments)
    # print("Got comments, and filtered to those about the app.")
    app_comments = None

    print(f"  Updated Metrics -> Views: {new_view_count}, Comments: {new_comment_count}, Likes: {new_likes}, Shares: {new_shares}")
    print(f"  Deltas          -> ΔViews: {delta_views}, ΔComments: {delta_comments}, ΔLikes: {delta_likes}, ΔShares: {delta_shares}\n")

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
                app_comments,
                old_shares,
                new_shares,
                delta_shares
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
            app_comments,
            old_num_shares,
            new_shares,
            delta_shares
        ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"  Logged metrics delta for URL: {post_url}\n")

        # Log new data to `DailyVideoData`, so if there is another trigger, we will use this updated entry as the base calculation
        insert_time = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")
        log_to_dvd(post_url, creator_username, marketing_associate, app, new_view_count, new_comment_count, caption, create_time, insert_time, new_likes, new_shares)
        print(f"  Logged updated row to DailyVideoData for URL: {post_url}\n")
        
        return f"Processed {post_url}"
    except Exception as e:
        print(f"  Error logging metrics for URL {post_url}: {str(e)}\n")
        return f"Error processing {post_url}"


# ----------------------------
# LOG THE UPDATED VIDEO METRICS TO THE `DailyVideoData` TABLE.
# NOTE: IF THE STRUCTRE OF THE TABLE IS CHANGED, THIS ALONG WITH `run_apify_update.py` and `db_manager.py`
# WOULD NEED TO BE UDPATED. THESE ARE THE ENTRY POINTS FOR NEW LOGS IN `DailyVideoData` TABLE
# ----------------------------
def log_to_dvd(url, username, associate, app, view_count, comment_count, caption, created_at, insert_time, likes_count, share_count):

    try:
        conn = psycopg2.connect(CONN_STR)
        cursor = conn.cursor()
        query = """
        INSERT INTO DailyVideoData 
        (post_url, creator_username, marketing_associate, app, view_count, comment_count, caption, create_time, log_time, num_likes, share_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        cursor.execute(query, (
            url, 
            username, 
            associate, 
            app, 
            view_count, 
            comment_count, 
            caption, 
            created_at, 
            insert_time,
            likes_count,
            share_count
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"  Error logging to DailyVideoData for URL {url}: {str(e)}\n")
        return
                    

# ----------------------------
# HIT APIFY FOR A URL, AND RETURN THE METRICS
# ----------------------------
def hit_apify(url):

    tiktok_regex = r"tiktok"
    instagram_regex = r"instagram"

    if re.search(tiktok_regex, url):
        url_type = "tiktok"
    elif re.search(instagram_regex, url):
        url_type = "instagram"
    else:
        print(f"URL '{url}' does not match TikTok or Instagram. Skipping.")
        return None

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

            # Extract the username and share count based on platform
            # For insagram, we keep share_count as 0
            share_count = 0
            if url_type == "tiktok":
                author_meta = item.get("authorMeta", {})
                username = author_meta.get("name", None)

                # only tiktok allows us to get the share count, we obtain it here
                share_count = item.get("shareCount", 0)
            elif url_type == "instagram":
                username = item.get("ownerUsername", None)


            return username, view_count, comment_count, likes_count, share_count
        
        else:
            return None

    except Exception as e:
        print(f"Error processing url {url}: {str(e)}")
        return None


# ----------------------------
# SEND A NOTIFICATION EMAIL
# ----------------------------
def send_notification_email(app, event_id, event_type):
    vids = get_top_three(event_id)

    # decide wording based on event_type
    if event_type == 'hourly':
        timeframe = 'in the past hour'
    else:
        timeframe = 'today'

    # Load environment variables
    FROM_EMAIL = os.getenv("FROM_EMAIL")
    PASSWORD   = os.getenv("APP_EMAIL_PW")
    TO_EMAILS  = [email.strip() for email in os.getenv("TO_EMAIL").split(',') if email.strip()]

    # Define email subject
    timestamp = datetime.now(ZoneInfo("America/New_York")).strftime("%b %d, %Y %I:%M %p")
    subject = f"Trial Trigger Event for {app} - {timestamp}"

    # Build the HTML message as a plain string (no extra imports)
    html_message = (
        "<html>"
          "<head>"
            "<style>"
              ".card { width: 14rem; margin: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.2); display: inline-block; background-color: rgba(124,235,157,0.4); border-radius: 10px; }"
              ".card-body { padding: 10px; }"
              ".card-subtitle { font-size: 0.95rem; color: #6c757d; }"
              ".card-text { font-size: 0.9rem; }"
              ".card-link { text-decoration: none; color: #007bff; }"
              ".container { text-align: center; }"
              ".big-link { font-size: 1.5rem; font-weight: bold; }"
            "</style>"
          "</head>"
          "<body>"
            f"<p>This app has seen a significant increase in the number of new trials {timeframe}.</p>"
            "<p>Below are the top three videos for this event:</p>"
            "<div class='container'>"
              "<u><h3>Top Trending Videos For This Event</h3></u>"
              "<div>"
    )

    # Loop through each video and create a card
    for vid in vids:
        card_html = (
            "<div class='card'>"
              "<div class='card-body'>"
                f"<h6 class='card-subtitle'>{vid.get('creator_username', 'Unknown Creator')}</h6>"
                "<p class='card-text'>"
                  f"<strong>&Delta; Views:</strong> {vid.get('delta_views', 0):,}<br>"
                  f"<strong>&Delta; Comments:</strong> {vid.get('delta_comments', 0):,}<br>"
                  f"<strong>&Delta; Likes:</strong> {vid.get('delta_likes', 0):,}<br>"
                  f"<strong>&Delta; Shares:</strong> {vid.get('delta_shares', 0):,}"
                "</p>"
                f"<a href='{vid['post_url']}' class='card-link' target='_blank'>View Post</a>"
              "</div>"
            "</div>"
        )
        html_message += card_html

    # Finish the HTML message with a larger "Check it out" link and closing signature
    html_message += (
              "</div>"
            "</div>"
            f"<p class='big-link'>Check it out: <a href='https://website-5g58.onrender.com/video_metrics/{event_id}'>Video Metrics</a></p>"
            "<p>-Trial Uptick Automated Message</p>"
          "</body>"
        "</html>"
    )

    # Manually build email content with a Content-Type header for HTML
    email_content = f"Subject: {subject}\nContent-Type: text/html\n\n{html_message}"

    try:
        print(f"Sending email to {TO_EMAILS}...")
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(FROM_EMAIL, PASSWORD)
        server.sendmail(FROM_EMAIL, TO_EMAILS, email_content)
        server.quit()
        print("Email sent successfully!")
    except smtplib.SMTPAuthenticationError:
        print("Failed to authenticate. Check your email/password.")
    except Exception as e:
        print(f"An error occurred: {e}")


# ----------------------------
# GET THE TOP THREE VIDEOS TO BE USED IN THE NOTIFICATION EMAIL
# ----------------------------
def get_top_three(event_id):

    try:
        conn = psycopg2.connect(CONN_STR)
        cursor = conn.cursor()

        # Videos for the trigger event
        query = """
            SELECT *
            FROM VideoMetricDeltas
            WHERE trial_trigger_event_id = %s
            ORDER BY id ASC;
        """
        cursor.execute(query, (event_id,))
        rows = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]

        # Convert rows to a list of dictionaries.
        video_metrics = [dict(zip(headers, row)) for row in rows]

        # Convert the event row to a dictionary (so you can display its fields easily in the template)
        event_details = dict(zip(headers, rows)) if rows else {}

        # Define a function to calculate the score.
        # NOTE: THIS SHOULD BE IDENTICAL TO THE SCORING CALCULATION IN get_engagement in server.py. Otherwise will be inconsistent
        def calculate_score(metric):
            delta_views = metric.get('delta_views') or 0
            delta_comments = metric.get('delta_comments') or 0
            delta_likes = metric.get('delta_likes') or 0
            delta_shares = metric.get('delta_shares') or 0
            return delta_views + delta_comments + (delta_likes * 5) + (delta_shares * 10)

        # First, calculate and store the raw score for each metric.
        for metric in video_metrics:
            metric['raw_score'] = calculate_score(metric)

        # Calculate the total raw score.
        total_raw_score = sum(metric['raw_score'] for metric in video_metrics)

        # Now, compute the normalized score for each metric.
        for metric in video_metrics:
            metric['score'] = round((metric['raw_score'] / total_raw_score) * 100, 3)

        # Sort the metrics by score in descending order.
        video_metrics_sorted = sorted(video_metrics, key=lambda m: m['score'], reverse=True)

        first_three = video_metrics_sorted[:3]

        return first_three


    except Exception as e:
        print("Error obtaining data for the trigger event top three\n")
        return []



# ----------------------------
# MAIN, CHECK WHETHER TRIGGER SHOULD BE FIRED FOR EACH APP
# ----------------------------
if __name__ == "__main__":

    # Define your app names properly
    APP_NAMES = ["saga", "berry", "haven", "astra"]

    for app in APP_NAMES:
        print("=================================")
        print("Looking at metrics for: ", app)
        if trial_trigger(app):
            print(app, ": Threshold exceeded, running scraper")
        else:
            print(app, ": Threshold NOT exceeded, NOT running scraper")
        print("=================================")

    # # EMAIL TESTING
    # send_notification_email("Testing", 926)

    # # DEBUG / RETROACTIVE ROW ADDITION, this will probably not be used again
    # # If need to add something retroactively, can do so like this:
    # row = 0, "https://www.tiktok.com/@piperrockelle/video/7491073331660180778", "piperrockelle", "Dylano", "Haven", 0, 0, None, None, None, 0, 0
    # process_video_row(row, 1916)