from .fetcher import get_weather_data
from .pipeline import run_pipeline

__all__ = ["get_weather_data", "run_pipeline"]


def main():
    """Run the weather API pipeline"""
    from .pipeline import run_pipeline

    return run_pipeline()


if __name__ == "__main__":
    main()
