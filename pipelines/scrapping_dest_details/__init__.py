from .fetcher import get_destination_info
from .pipeline import run_pipeline

__all__ = ["get_destination_info", "run_pipeline"]


def main():
    """Run the Wikipedia scraping pipeline"""
    from .pipeline import run_pipeline

    return run_pipeline()


if __name__ == "__main__":
    main()
