import os
from src.pipeline.main_pipeline import MainPipeline
from src.utils.logger import Logger


def main():
    logger = Logger(__name__)
    logger.info("Starting NexaBank Data Pipeline")
    
    try:
        pipeline = MainPipeline()
        logger.info("Pipeline initialized successfully")
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 