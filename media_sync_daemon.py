import argparse
import datetime
import gc
import logging
import os
import sys
import time

from config import config, validate_config, ConfigError, update_config_path
from drivers import DriverError, create_driver
from media_sync import (
    create_mergin_client,
    mc_download,
    media_sync_push,
    mc_pull,
    MediaSyncError,
)
from version import __version__


def setup_logger():
    logger = logging.getLogger("media-sync")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def run_sync_cycle(mc, driver, logger):
    try:
        logger.info("Pulling changes from Mergin...")
        files_to_sync = mc_pull(mc)
        media_sync_push(mc, driver, files_to_sync)
        logger.info("Sync complete.")

        # Force garbage collection
        gc.collect()

    except MediaSyncError as e:
        logger.error(f"Media sync error: {e}")


def main():
    parser = argparse.ArgumentParser(
        prog="media_sync_daemon.py",
        description="Synchronization tool for media files in Mergin Maps project to other backends.",
        epilog="www.merginmaps.com",
    )

    parser.add_argument(
        "config_file",
        nargs="?",
        default="config.yaml",
        help="Path to file with configuration. Default is ./config.yaml",
    )

    args = parser.parse_args()
    logger = setup_logger()
    logger.info(f"== Starting Mergin Media Sync daemon v{__version__} ==")

    try:
        update_config_path(args.config_file)
        validate_config(config)
    except (IOError, ConfigError) as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    sleep_time = config.as_int("daemon.sleep_time")

    try:
        driver = create_driver(config)
    except DriverError as e:
        logger.error(f"Driver error: {e}")
        sys.exit(1)

    logger.info("Logging in to Mergin...")
    try:
        mc = create_mergin_client()
        if not os.path.exists(config.project_working_dir):
            logger.info("Project directory not found. Downloading from Mergin...")
            files_to_sync = mc_download(mc)
            media_sync_push(mc, driver, files_to_sync)
    except MediaSyncError as e:
        logger.error(f"Initial sync error: {e}")
        sys.exit(1)

    logger.info("Entering sync loop...")
    while True:
        logger.info(f"Heartbeat: {datetime.datetime.utcnow().isoformat()} UTC")
        run_sync_cycle(mc, driver, logger)

        # Check token expiry
        try:
            delta = mc._auth_session["expire"] - datetime.datetime.now(datetime.timezone.utc)
            if delta.total_seconds() < 3600:
                logger.info("Refreshing Mergin auth token...")
                mc = create_mergin_client()
        except Exception as e:
            logger.warning(f"Error checking token expiration: {e}")

        logger.info(f"Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
