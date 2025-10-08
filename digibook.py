# DIGIMONITOR is part of the DIGIBOOK collection.
# DIGIBOOK Copyright (C) 2024-2025 Daniel A. L.
# Repository: https://github.com/caminodelaserpiente/DigiBook

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


from DigiMonitor.app.src.scraper.youtube import YTScraper
import argparse
import logging
import os
import sys


VERSION_INFO = """DIGIBOOK Copyright (C) 2024-2025 Daniel A. L.
DigiBook 2.0.0 (Caminodelaserpiente 蛇道 2025)
Repository: https://github.com/caminodelaserpiente/DigiBook
Comes with ABSOLUTELY NO WARRANTY to the extent 
permitted by applicable law.
Redistribution of this software is
covered by the terms of the DIGIBOOK LICENSE."""


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def main():
    """
    Main function for CLI configuration and execution.
    """

    # 0. Check --version early
    if '--version' in sys.argv:
        print(VERSION_INFO)
        sys.exit(0)

    # 1. Parser creation
    parser = argparse.ArgumentParser(
        description="DIGIBOOK: Scraping tool for collecting data from digital platforms.",
        epilog="""
        Usage examples:
        python digimonitor.py -u urls_input/youtube_urls.txt --max-concurrent 10 --headless
        """
    )

    # 2. Arguments definition

    parser.add_argument(
        '-u', '--urls-file',
        type=str,
        required=True,
        help='Path, file, YouTube URLs list (one per line).'
    )

    parser.add_argument(
        '-c', '--max-concurrent',
        type=int,
        default=3,
        help='Number, maximum, concurrent tabs, scraping. Default 3.'
    )

    parser.add_argument(
        '--headless',
        action='store_false',
        help='Mode, browser, headless execution.'
    )

    parser.add_argument(
        '-o', '--output-dir',
        type=str,
        default="out_storage",
        help="Directory, storage, data output. Default 'out_storage'."
    )

    parser.add_argument(
        '--version', 
        action='store_true', 
        help='Script, version information.'
    )

    # 3. Parse arguments
    args = parser.parse_args()

    # 4. Validation
    if not args.max_concurrent > 0:
        logging.error("Argument error: --max-concurrent must be greater than zero.")
        parser.exit(status=1)

    # 5. Output directory creation
    os.makedirs(args.output_dir, exist_ok=True)
    logging.info(f"Output directory: {args.output_dir}")

    # 6. Logic execution
    try:
        logging.info(f"Reading URLs file: {args.urls_file}")
        with open(args.urls_file, "r") as f:
            urls = [line.strip() for line in f if line.strip()]

        if not urls:
            logging.warning("URLs file empty. No data for processing.")
            return

        scraper = YTScraper(urls, args.max_concurrent, output_dir=args.output_dir, headless=args.headless)
        scraper.run()

    except FileNotFoundError:
        logging.error(f"Error: File not found '{args.urls_file}'. Verify path.")
        parser.exit(status=1)


# Entry point
if __name__ == "__main__":
    main()
