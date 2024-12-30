import os
from dotenv import load_dotenv
from typing import Annotated
from pandas import DataFrame
import pandas as pd

from finnlp.data_sources.news.cnbc_streaming import CNBC_Streaming
from finnlp.data_sources.news.yicai_streaming import Yicai_Streaming
from finnlp.data_sources.news.investorplace_streaming import InvestorPlace_Streaming
# from finnlp.data_sources.news.eastmoney_streaming import Eastmoney_Streaming
from finnlp.data_sources.social_media.xueqiu_streaming import Xueqiu_Streaming
from finnlp.data_sources.social_media.stocktwits_streaming import Stocktwits_Streaming
# from finnlp.data_sources.social_media.reddit_streaming import Reddit_Streaming
from finnlp.data_sources.news.sina_finance_date_range import Sina_Finance_Date_Range
from finnlp.data_sources.news.finnhub_date_range import Finnhub_Date_Range

from src.utils import save_output, SavePathType

US_Proxy = {
    "use_proxy": "us_free",
    "max_retry": 5,
    "proxy_pages": 5,
}
CN_Proxy = {
    "use_proxy": "china_free",
    "max_retry": 5,
    "proxy_pages": 5,
}

load_dotenv()


def streaming_download(streaming, config, tag, keyword, rounds, selected_columns, save_path):
    downloader = streaming(config)

    # Initiate the downloader based on available methods
    if hasattr(downloader, 'download_streaming_search'):
        downloader.download_streaming_search(keyword, rounds)
    elif hasattr(downloader, 'download_streaming_stock'):
        downloader.download_streaming_stock(keyword, rounds)
    else:
        downloader.download_streaming_all(rounds)

    # Define 'selected' outside the conditionals to ensure scope availability
    selected = pd.DataFrame()  # Initialize as an empty DataFrame

    # Check if any data was downloaded
    if downloader.dataframe.empty:
        print("No data was downloaded.")
    else:
        # Check if the necessary columns are available
        if set(selected_columns).issubset(downloader.dataframe.columns):
            selected = downloader.dataframe[selected_columns]
            # Uncomment to print selected data if needed
            # print("Selected data:")
            # print(selected.head())  # Or any further processing
            save_output(selected, tag, save_path)
        else:
            print("The expected columns are missing from the DataFrame.")
            print("Available columns:", downloader.dataframe.columns)

    # Return the selected DataFrame (it will be empty if conditions were not met)
    return selected


def date_range_download(date_range, config, tag, start_date, end_date, stock, selected_columns, save_path):
    downloader = date_range(config)
    if hasattr(downloader, 'download_date_range_stock'):
        downloader.download_date_range_stock(start_date, end_date, stock)
    else:
        downloader.download_date_range_all(start_date, end_date)
    if hasattr(downloader, 'gather_content'):
        downloader.gather_content()
    # print(downloader.dataframe.columns)
    selected_news = downloader.dataframe[selected_columns]
    save_output(selected_news, tag, save_path)
    return selected_news


class FinNLPUtils:
    """
  Utilities for downloading news and social media data using various streaming and date-range downloaders.
  """

    def cnbc_news_download(
            self,  # Add self here
            keyword: Annotated[str, "Keyword to search in news stream"],
            rounds: Annotated[int, "Number of rounds to search. Default to 1"] = 1,
            selected_columns: Annotated[
                list[str], "List of column names of news to return, default columns selected"] = [
                "author", "datePublished", "description", "section", "cn:title", "summary"
            ],
            save_path: SavePathType = None
    ) -> DataFrame:
        return streaming_download(CNBC_Streaming, {}, "CNBC News", keyword, rounds, selected_columns, save_path)

    def yicai_news_download(
            self,  # Add self here
            keyword: Annotated[str, "Keyword to search in news stream"],
            rounds: Annotated[int, "Number of rounds to search. Default to 1"] = 1,
            selected_columns: Annotated[
                list[str], "List of column names of news to return, default columns selected"] = [
                "author", "creationDate", "desc", "source", "title"
            ],
            save_path: SavePathType = None
    ) -> DataFrame:
        return streaming_download(Yicai_Streaming, {}, "Yicai News", keyword, rounds, selected_columns, save_path)

    def investor_place_news_download(
            self,  # Add self here
            keyword: Annotated[str, "Keyword to search in news stream"],
            rounds: Annotated[int, "Number of rounds to search. Default to 1"] = 1,
            selected_columns: Annotated[
                list[str], "List of column names of news to return, default columns selected"] = [
                "title", "time", "author", "summary"
            ],
            save_path: SavePathType = None
    ) -> DataFrame:
        return streaming_download(InvestorPlace_Streaming, {}, "Investor Place News", keyword, rounds, selected_columns,
                                  save_path)

    def sina_finance_news_download(
            self,  # Add self here
            start_date: Annotated[str, "Start date of the news to retrieve, YYYY-mm-dd"],
            end_date: Annotated[str, "End date of the news to retrieve, YYYY-mm-dd"],
            selected_columns: Annotated[
                list[str], "List of column names of news to return, default columns selected"] = [
                "title", "author", "content"
            ],
            save_path: SavePathType = None
    ) -> DataFrame:
        return date_range_download(Sina_Finance_Date_Range, {}, "Sina Finance News", start_date, end_date, None,
                                   selected_columns, save_path)

    def finnhub_news_download(
            self,  # Add self here
            start_date: Annotated[str, "Start date of the news to retrieve, YYYY-mm-dd"],
            end_date: Annotated[str, "End date of the news to retrieve, YYYY-mm-dd"],
            stock: Annotated[str, "Stock symbol, e.g. AAPL"],
            selected_columns: Annotated[
                list[str], "List of column names of news to return, default columns selected"] = [
                "headline", "datetime", "source", "summary"
            ],
            save_path: SavePathType = None
    ) -> DataFrame:
        print(os.environ['FINNHUB_API_KEY'])
        return date_range_download(Finnhub_Date_Range, {"token": os.environ['FINNHUB_API_KEY']}, "Finnhub News",
                                   start_date, end_date, stock, selected_columns, save_path)

    def xueqiu_social_media_download(
            self,  # Add self here
            stock: Annotated[str, "Stock symbol, e.g. 'AAPL'"],
            rounds: Annotated[int, "Number of rounds to search. Default to 1"] = 1,
            selected_columns: Annotated[
                list[str], "List of column names of news to return, default columns selected"] = [
                "created_at", "description", "title", "text", "target", "source"
            ],
            save_path: SavePathType = None
    ) -> DataFrame:
        return streaming_download(Xueqiu_Streaming, {}, "Xueqiu Social Media", stock, rounds, selected_columns,
                                  save_path)

    def stocktwits_social_media_download(
            self,  # Add self here
            stock: Annotated[str, "Stock symbol, e.g. 'AAPL'"],
            rounds: Annotated[int, "Number of rounds to search. Default to 1"] = 1,
            selected_columns: Annotated[
                list[str], "List of column names of news to return, default columns selected"] = [
                "created_at", "body"
            ],
            save_path: SavePathType = None
    ) -> DataFrame:
        return streaming_download(Stocktwits_Streaming, {}, "Stocktwits Social Media", stock, rounds, selected_columns,
                                  save_path)

    # def reddit_social_media_download(
    #     pages: Annotated[int, "Number of pages to retrieve. Default to 1"] = 1,
    #     selected_columns: Annotated[list[str], """
    #         List of column names of news to return, should be chosen from 'id',
    #         'body', 'created_at', 'user', 'source', 'symbols', 'prices',
    #         'mentioned_users', 'entities', 'liked_by_self', 'reshared_by_self',
    #         'conversation', 'links', 'likes', 'reshare_message', 'structurable',
    #         'reshares'. Default to ['created_at', 'body']
    #     """] = ['created_at', 'body'],
    #     verbose: Annotated[bool, "Whether to print downloaded news to console. Default to True"] = True,
    #     save_path: Annotated[str, "If specified (recommended if the amount of news is large), the downloaded news will be saved to save_path. Default to None"] = None,
    # ) -> DataFrame:
    #     return streaming_download(Reddit_Streaming, {}, "Reddit Social Media", None, pages, selected_columns, save_path)


if __name__ == "__main__":
    obj = FinNLPUtils()
    #data = obj.cnbc_news_download(keyword="tesla", save_path="cnbc_tesla.csv")
    #print(type(data)) # dataframe
    #data = obj.investor_place_news_download("tesla", save_path="invpl_tesla.csv")
    #print(type(data))  # dataframe
    #data = obj.finnhub_news_download(start_date="2024-03-02", end_date="2024-03-02", stock="AAPL",
    #                                save_path="finnhub_aapl_news.csv")
    # print(type(data))  # dataframe
    data = obj.stocktwits_social_media_download(stock="AAPL", save_path="stocktwits_aapl.csv")
    print(type(data))  # dataframe

