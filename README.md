# Trial Trigger and Comments Processing System

This system automates the monitoring and analysis of trial sign-up activity across various apps and triggers a series of data processing steps when increased activity is detected. It is comprised of several interconnected components:

- **Trial Data Analysis:**  
  The system retrieves trial sign-up counts from the `NewTrials` database for the past 30 days (logged in UTC). It calculates the median trial count (excluding today's data) and compares it against today's count. If today's count exceeds the median by a defined threshold, the system flags this as significant activity.

- **Triggering Further Processing:**  
  When the threshold is exceeded, a trigger event is recorded in the `TrialTriggerEvents` database. This event initiates further processing, including updating video engagement metrics and logging detailed delta information.

- **Video Metrics Update:**  
  For videos associated with the app from the past 10 days (stored in the `DailyVideoData` table), the system concurrently retrieves updated metrics—such as views, comments, likes, and shares—via the Apify API. It calculates the differences (deltas) between the new and previously recorded metrics and logs:
  - Updated video metrics back to the `DailyVideoData` table.
  - The metric deltas to the `VideoMetricDeltas` table for further analysis.

- **Notification System:**  
  Upon triggering an event and processing the updates, the system sends an email notification to alert relevant stakeholders about the increased trial activity and subsequent video metrics changes.

- **Comments Fetching and Filtering Utilities:**  
  The system also includes utilities that:
  - Fetch comments from social media posts (Instagram and TikTok) using Apify actors.
  - Leverage OpenAI's API to filter these comments, returning only those that indicate direct engagement with the app (such as actions related to downloads, trial sign-ups, or explicit interest).

This comprehensive solution ensures that any significant change in trial sign-up activity is quickly detected, analyzed, and communicated, while also keeping video engagement metrics up to date for strategic decision-making.
