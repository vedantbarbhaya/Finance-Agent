from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class to manage environment variables."""
    
    @staticmethod
    def get_env_variable(key: str, default=None):
        """
        Get an environment variable.
        
        Args:
            key (str): The name of the environment variable
            default: The default value if the environment variable is not found
            
        Returns:
            The value of the environment variable or the default value
        """
        return os.getenv(key, default)
    
    # Add specific getters for your API keys
    @staticmethod
    def get_api_key_1():
        return Config.get_env_variable('API_KEY_1')
    
    @staticmethod
    def get_api_key_2():
        return Config.get_env_variable('API_KEY_2')
    
    @staticmethod
    def get_openai_api_key():
        return Config.get_env_variable('OPENAI_API_KEY') 