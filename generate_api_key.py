#!/usr/bin/env python3
"""
Utility script to generate secure API keys for the OncoTwin API.

Usage:
    python generate_api_key.py [--length LENGTH] [--count COUNT]

Examples:
    python generate_api_key.py                    # Generate one 64-character key
    python generate_api_key.py --length 32         # Generate one 32-character key
    python generate_api_key.py --count 5           # Generate 5 keys
    python generate_api_key.py --length 32 --count 3  # Generate 3 keys of 32 chars
"""

import secrets
import argparse
import sys


def generate_api_key(length: int = 64) -> str:
    """
    Generate a cryptographically secure random API key.
    
    Args:
        length: Length of the key in characters (default: 64)
    
    Returns:
        A hexadecimal string of the specified length
    """
    # Generate random bytes and convert to hex
    # Each byte produces 2 hex characters, so we need length/2 bytes
    num_bytes = length // 2
    random_bytes = secrets.token_bytes(num_bytes)
    api_key = random_bytes.hex()
    
    # If length is odd, truncate to exact length
    return api_key[:length]


def main():
    parser = argparse.ArgumentParser(
        description="Generate secure API keys for OncoTwin API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Generate one 64-character key
  %(prog)s --length 32        # Generate one 32-character key
  %(prog)s --count 5          # Generate 5 keys
  %(prog)s --length 32 --count 3  # Generate 3 keys of 32 chars
        """
    )
    
    parser.add_argument(
        "--length",
        type=int,
        default=64,
        help="Length of the API key in characters (default: 64)"
    )
    
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of API keys to generate (default: 1)"
    )
    
    args = parser.parse_args()
    
    if args.length < 16:
        print("Warning: Keys shorter than 16 characters are not recommended for security.", file=sys.stderr)
    
    if args.length % 2 != 0:
        print("Warning: Odd-length keys will be truncated. Consider using even lengths.", file=sys.stderr)
    
    print(f"Generating {args.count} API key(s) of length {args.length}:\n")
    
    for i in range(args.count):
        key = generate_api_key(args.length)
        print(f"API Key {i+1}: {key}")
    
    print("\n" + "="*70)
    print("To use this key, add it to your .env file:")
    print(f"API_KEY={generate_api_key(args.length) if args.count == 1 else '<your_key_here>'}")
    print("\nOr set it as an environment variable:")
    print(f"export API_KEY=<your_key_here>")


if __name__ == "__main__":
    main()

