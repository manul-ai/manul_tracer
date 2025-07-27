"""CLI interface for the Manul Tracer visualization app."""

import argparse
import sys
import subprocess
import os
from pathlib import Path


def main():
    """Main CLI entry point for manul-tracer command."""
    parser = argparse.ArgumentParser(
        description="Manul Tracer - OpenAI API call tracing and visualization",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--database', '-d',
        type=str,
        required=True,
        help='Path to the DuckDB database file'
    )
    
    parser.add_argument(
        '--port', '-p',
        type=int,
        default=8501,
        help='Port for the Streamlit app (default: 8501)'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='Host address for the Streamlit app (default: localhost)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode'
    )
    
    args = parser.parse_args()
    
    # Validate database file
    db_path = Path(args.database)
    if not db_path.exists():
        print(f"Error: Database file does not exist: {db_path}")
        print("Create a database file by running the tracer first.")
        sys.exit(1)
    
    # Find the streamlit app entry point
    app_file = Path(__file__).parent / "streamlit_app" / "main.py"
    
    if not app_file.exists():
        print(f"Error: Streamlit app not found at {app_file}")
        sys.exit(1)
    
    # Set environment variables for the app
    env = os.environ.copy()
    env['MANUL_DATABASE_PATH'] = str(db_path.absolute())
    if args.debug:
        env['MANUL_DEBUG'] = 'true'
    
    # Build streamlit command
    cmd = [
        'streamlit', 'run', str(app_file),
        '--server.port', str(args.port),
        '--server.address', args.host,
        '--server.headless', 'true',
        '--browser.gatherUsageStats', 'false'
    ]
    
    print(f"Starting Manul Tracer visualization app...")
    print(f"Database: {db_path.absolute()}")
    print(f"URL: http://{args.host}:{args.port}")
    print(f"Press Ctrl+C to stop")
    
    try:
        subprocess.run(cmd, env=env, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error starting Streamlit app: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        sys.exit(0)


if __name__ == '__main__':
    main()