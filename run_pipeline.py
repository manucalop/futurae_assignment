from futurae_assignment.config import Config
from futurae_assignment.pipeline import pipeline

if __name__ == "__main__":
    config = Config()
    pipeline.run(config.pipeline)
