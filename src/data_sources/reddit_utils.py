import os
import praw
import pandas as pd
from typing import Annotated, List
from functools import wraps
from datetime import datetime, timezone
from src.utils import decorate_all_methods, save_output, SavePathType
import os
from praw.models import MoreComments

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


# @decorate_all_methods(init_reddit_client)
# class RedditUtils:
#
#     def get_reddit_posts(
#         query: Annotated[
#             str, "Search query, e.g. 'AAPL OR Apple Inc OR #AAPL OR (Apple AND stock)'"
#         ],
#         start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
#         end_date: Annotated[str, "End date in yyyy-mm-dd format"],
#         limit: Annotated[
#             int, "Maximum number of posts to fetch, default to 1000"
#         ] = 1000,
#         selected_columns: Annotated[
#             List[str],
#             "Columns to contain in the result, should be chosen from 'created_utc', 'id', 'title', 'selftext', 'score', 'num_comments', 'url', default to ['created_utc', 'title', 'score', 'num_comments']",
#         ] = ["created_utc", "title", "score", "num_comments"],
#         save_path: SavePathType = None,
#     ) -> pd.DataFrame:
#         """
#         Get Reddit posts from r/wallstreetbets & r/stocks & r/investing based on the search query and date range.
#         """
#
#         post_data = []
#
#         start_timestamp = int(
#             datetime.strptime(start_date, "%Y-%m-%d")
#             .replace(tzinfo=timezone.utc)
#             .timestamp()
#         )
#         end_timestamp = int(
#             datetime.strptime(end_date, "%Y-%m-%d")
#             .replace(tzinfo=timezone.utc)
#             .timestamp()
#         )
#
#         for subreddit_name in ["wallstreetbets", "stocks", "investing"]:
#             print("Searching in subreddit:", subreddit_name)
#             subreddit = reddit_client.subreddit(subreddit_name)
#             posts = subreddit.search(query, limit=limit)
#
#             for post in posts:
#                 if start_timestamp <= post.created_utc <= end_timestamp:
#                     post_data.append(
#                         [
#                             datetime.fromtimestamp(
#                                 post.created_utc, tz=timezone.utc
#                             ).strftime("%Y-%m-%d %H:%M:%S"),
#                             post.id,
#                             post.title,
#                             post.selftext,
#                             post.score,
#                             post.num_comments,
#                             post.url,
#                         ]
#                     )
#
#         output = pd.DataFrame(
#             post_data,
#             columns=[
#                 "created_utc",
#                 "id",
#                 "title",
#                 "selftext",
#                 "score",
#                 "num_comments",
#                 "url",
#             ],
#         )
#         output = output[selected_columns]
#
#         save_output(output, f"reddit posts related to {query}", save_path=save_path)
#
#         return output
    


@decorate_all_methods(init_reddit_client)
class RedditUtils:
    def get_reddit_posts(
        self,
        query: Annotated[str, "Search query"],
        start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
        end_date: Annotated[str, "End date in yyyy-mm-dd format"],
        limit: Annotated[int, "Maximum number of posts to fetch"] = 1000,
        num_comments: Annotated[int, "Number of top comments to fetch"] = 5,
        selected_columns: Annotated[List[str], "Columns to contain in the result"] = ["created_utc", "title", "score", "num_comments"],
        save_path: SavePathType = None,
    ) -> pd.DataFrame:
        post_data = []
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

        for subreddit_name in ["wallstreetbets", "stocks", "investing"]:
            subreddit = reddit_client.subreddit(subreddit_name)
            posts = subreddit.search(query, limit=limit)
            for post in posts:
                if start_timestamp <= post.created_utc <= end_timestamp:
                    post_comments = []
                    for comment in post.comments:
                        if isinstance(comment, MoreComments):
                            continue
                        post_comments.append((comment.body, comment.score))
                        if len(post_comments) >= num_comments:
                            break

                    post_data.append([
                        datetime.fromtimestamp(post.created_utc, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        post.id,
                        post.title,
                        post.selftext,
                        post.score,
                        post.num_comments,
                        post.url,
                        post_comments  # This will include top comments as a list of tuples (comment text, score)
                    ])

        columns = ["created_utc", "id", "title", "selftext", "score", "num_comments", "url", "top_comments"]
        output = pd.DataFrame(post_data, columns=columns)
        output = output[selected_columns + ['top_comments'] if 'top_comments' in selected_columns else selected_columns]

        save_output(output, f"reddit posts related to {query}", save_path=save_path)
        return output



# Example usage
if __name__ == "__main__":

    from src.utils import register_keys_from_json
       
    # Correct relative path from src/data-sources to the project root
    base_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config_api_keys.json')
    register_keys_from_json(base_path)

    # df = RedditUtils.get_reddit_posts(query="AAPL OR Apple Inc OR #AAPL OR (Apple AND stock)", start_date="2023-05-01", end_date="2023-06-01", limit=1000)
    df = RedditUtils.get_reddit_posts(
        query="NVDA", start_date="2023-05-01", end_date="2023-06-01", limit=1000
    )
    print(df.head())
    df.to_csv("reddit_posts.csv", index=False)
