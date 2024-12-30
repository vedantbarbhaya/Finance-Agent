import asyncio
from collectors import APICollector, WebCollector
from config import Config

async def main():
    """Main entry point for the application."""
    # Example of using collectors
    api_collector = APICollector("https://api.example.com/data")
    web_collector = WebCollector("https://example.com")
    
    try:
        # Collect data from different sources
        api_data = await api_collector.collect()
        web_data = await web_collector.collect()
        
        # Process your collected data here
        print("API Data:", api_data)
        print("Web Data:", web_data)
        
    except Exception as e:
        print(f"Error collecting data: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 