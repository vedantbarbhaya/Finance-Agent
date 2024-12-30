\import os
import praw
import pandas as pd
from typing import Annotated, List
from functools import wraps
from datetime import datetime, timezone
from src.utils import decorate_all_methods, save_output, SavePathType
import os
from praw.models import MoreComments


import json
from typing import List
from praw.models import Comment, Submission

def init_reddit_client(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        global reddit_client
        if not all(
            [os.environ.get("REDDIT_CLIENT_ID"), os.environ.get("REDDIT_CLIENT_SECRET")]
        ):
            print("Please set the environment variables for Reddit API credentials.")
            return None
        else:
            reddit_client = praw.Reddit(
                client_id=os.environ["REDDIT_CLIENT_ID"],
                client_secret=os.environ["REDDIT_CLIENT_SECRET"],
                user_agent="python:finrobot:v0.1 (by /u/finrobot)",
            )
            print("Reddit client initialized")
            return func(*args, **kwargs)

    return wrapper

def test_extract_post_details():
    with open('/Users/vedantbarbhaya/Desktop/Cursor projects/Finance-Agent-Sytem/src/data-sources/sample_reddit_post.json', 'r') as file:
        data = json.load(file)
    title, url = extract_post_details(data)
    assert title == "Example Post Title", "Title does not match expected"
    assert url == "https://reddit.com/r/examplepost", "URL does not match expected"



@decorate_all_methods(init_reddit_client)
class RedditUtils:

    def get_reddit_posts(
        self,
        query: str,
        start_date: str,
        end_date: str,
        limit: int = 1000,
        num_comments: int = 10,  # New parameter to limit the number of comments
        selected_columns: List[str] = ["created_utc", "title", "score", "num_comments"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        """
        Get Reddit posts and their top comments from specified subreddits based on the search query and date range.
        """
        post_data = []
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

        for subreddit_name in ["wallstreetbets", "stocks", "investing"]:
            subreddit = reddit_client.subreddit(subreddit_name)
            posts = subreddit.search(query, limit=limit)
            for post in posts:
                if start_timestamp <= post.created_utc <= end_timestamp:
                    post.comments.replace_more(limit=0)  # Expands all MoreComments objects
                    comments = sorted(post.comments.list(), key=lambda x: x.score, reverse=True)[:num_comments]

                    post_info = {
                        "created_utc": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "id": post.id,
                        "title": post.title,
                        "selftext": post.selftext,
                        "score": post.score,
                        "num_comments": post.num_comments,
                        "url": post.url,
                        "top_comments": [{"comment_id": comment.id, "text": comment.body, "score": comment.score} for comment in comments]
                    }
                    post_data.append(post_info)

        # Convert to DataFrame for any additional processing
        df = pd.DataFrame(post_data)
        if save_path:
            self.save_to_langchain_document(post_data, save_path)  # Assuming the save function is adapted and moved to be a method

        return df

    def save_to_langchain_document(self, posts, file_path):
        documents = []
        for post in posts:
            document = {
                "post_id": post['id'],
                "title": post['title'],
                "url": post['url'],
                "created_utc": post['created_utc'],
                "content": post['selftext'],
                "comments": post['top_comments']
            }
            documents.append(document)

        with open(file_path, 'w') as f:
            json.dump(documents, f, indent=4)

        print(f"Saved {len(documents)} documents to {file_path}")




if __name__ == "__main__":
    from src.utils import register_keys_from_json

    # Correct relative path from src/data-sources to the project root
    base_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config_api_keys.json')
    register_keys_from_json(base_path)

    # Define a custom output path for the JSON document
    output_path = '/Users/vedantbarbhaya/Desktop/Cursor projects/Finance-Agent-Sytem/output/langchain_document.json'

    # Create an instance of RedditUtils
    reddit_utils_instance = RedditUtils()

    # Call get_reddit_posts with the custom output path
    df = reddit_utils_instance.get_reddit_posts(
        query="NVDA", start_date="2023-05-01", end_date="2023-06-01", limit=1000,
        save_path=output_path  # Pass the custom save path to the method
    )
    print(df.head())

    # Optionally save the DataFrame to a CSV file
    # df.to_csv("/Users/vedantbarbhaya/Desktop/Cursor projects/Finance-Agent-Sytem/output/reddit_posts.csv", index=False)