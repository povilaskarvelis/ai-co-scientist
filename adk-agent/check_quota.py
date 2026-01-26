#!/usr/bin/env python3
"""Quick check if Gemini API quota is available."""
from dotenv import load_dotenv
load_dotenv()

from google import genai

client = genai.Client()
try:
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents="Say 'OK' and nothing else"
    )
    print(f"✓ API working: {response.text.strip()}")
except Exception as e:
    if "429" in str(e):
        print("✗ Rate limited")
        # Show the retry delay
        import re
        match = re.search(r'retry in (\d+)', str(e))
        if match:
            print(f"   Retry in: {match.group(1)} seconds")
        # Check for limit value
        match2 = re.search(r'limit: (\d+)', str(e))
        if match2:
            print(f"   Limit: {match2.group(1)}")
    else:
        print(f"✗ Error: {e}")
