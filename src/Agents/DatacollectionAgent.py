import requests
import yfinance as yf
import pandas as pd

class DataCollectionAgent:
    def __init__(self, finhub_api_key):
        self.finhub_api_key = finhub_api_key
        self.supported_sources = ["finhub", "yfinance"]

    ### FETCHING FROM FINHUB ###
    def fetch_data_from_finhub(self, symbol, endpoint="stock/candle", resolution="D", from_date=None, to_date=None):
        """
        Fetch financial data from Finhub API.
        """
        base_url = "https://finnhub.io/api/v1/"
        params = {
            "symbol": symbol,
            "resolution": resolution,  # Daily resolution
            "from": from_date,
            "to": to_date,
            "token": self.finhub_api_key
        }
        url = f"{base_url}{endpoint}"
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from Finhub: {e}")
            return None

    ### FETCHING FROM YFINANCE ###
    def fetch_data_from_yfinance(self, symbol, start_date=None, end_date=None):
        """
        Fetch financial data using yfinance.
        """
        try:
            stock = yf.Ticker(symbol)
            data = stock.history(start=start_date, end=end_date)
            return data.reset_index()  # Convert to DataFrame
        except Exception as e:
            print(f"Error fetching data from yfinance: {e}")
            return None

    ### PROCESS AND NORMALIZE DATA ###
    def normalize_data(self, data, source):
        """
        Normalize data from different sources into a consistent format.
        """
        if source == "finhub":
            # Convert Finhub's JSON response into a DataFrame
            try:
                df = pd.DataFrame({
                    "timestamp": data.get("t", []),
                    "open": data.get("o", []),
                    "high": data.get("h", []),
                    "low": data.get("l", []),
                    "close": data.get("c", []),
                    "volume": data.get("v", []),
                })
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
                return df
            except Exception as e:
                print(f"Error normalizing Finhub data: {e}")
                return None
        elif source == "yfinance":
            # Ensure yfinance data conforms to a standard structure
            return data.rename(columns={"Date": "timestamp", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
        else:
            print("Unsupported data source.")
            return None

    ### MAIN METHOD TO FETCH DATA ###
    def fetch_data(self, symbol, source, start_date=None, end_date=None):
        """
        Fetch data dynamically from a specified source.
        """
        if source not in self.supported_sources:
            return f"Unsupported source: {source}. Supported sources are: {', '.join(self.supported_sources)}"

        if source == "finhub":
            raw_data = self.fetch_data_from_finhub(symbol, from_date=start_date, to_date=end_date)
        elif source == "yfinance":
            raw_data = self.fetch_data_from_yfinance(symbol, start_date=start_date, end_date=end_date)
        else:
            return None

        # Normalize and return data
        return self.normalize_data(raw_data, source)

    ### DYNAMIC TASK EXECUTION ###
    def execute_task(self, query, symbol, source="yfinance", start_date=None, end_date=None):
        """
        Fetch data and perform FinNLP tasks based on the query.
        """
        # Fetch data
        data = self.fetch_data(symbol, source, start_date, end_date)
        if data is None or data.empty:
            return "No data retrieved or data processing failed."

        # Determine and execute FinNLP tasks
        if "sentiment" in query.lower():
            print("Performing sentiment analysis...")  # Placeholder
            # Integrate FinNLP sentiment pipeline here
        elif "summary" in query.lower():
            print("Performing summarization...")  # Placeholder
            # Integrate FinNLP summarization pipeline here
        elif "entities" in query.lower():
            print("Performing entity recognition...")  # Placeholder
            # Integrate FinNLP NER pipeline here
        else:
            return "Query does not match any known tasks."

        # Return normalized data as a fallback (for demo)
        return data

# Example Usage
agent = DataCollectionAgent(finhub_api_key="YOUR_FINHUB_API_KEY")
query = "What is the sentiment of recent stock performance?"
symbol = "AAPL"
source = "yfinance"
start_date = "2023-01-01"
end_date = "2023-12-31"

result = agent.execute_task(query, symbol, source, start_date, end_date)
print(result)
