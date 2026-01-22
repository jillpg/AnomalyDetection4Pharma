import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger with a standard format.
    Output: Console (StreamHandler)
    Format: [TIMESTAMP] [LEVEL] [MODULE] - Message
    """
    logger = logging.getLogger(name)
    
    # Singleton-like behavior: avoid adding handlers if they exist
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] [%(name)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
        # Add handler
        logger.addHandler(handler)
        
    return logger
