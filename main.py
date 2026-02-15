import argparse
import sys
import logging
import os

def main():
    parser = argparse.ArgumentParser(description="Run Data Engineering Pipelines")
    parser.add_argument("--manifest", required=True, help="Path to YAML manifest")
    parser.add_argument("--env", help="Override ENV variable (e.g. dev, prd)")
    
    args = parser.parse_args()

    # Set ENV before importing application logic
    if args.env:
        os.environ["ENV"] = args.env

    # Deferred import to ensure config picks up the ENV variable
    from src.core.runner import run_pipeline

    try:
        run_pipeline(args.manifest)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
