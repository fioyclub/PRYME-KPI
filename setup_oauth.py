#!/usr/bin/env python3
"""
OAuth Setup Script for Telegram KPI Bot

This script helps you set up OAuth authentication for Google APIs.
Run this script locally to generate the token.json file.
"""

import os
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# Google API scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

TOKEN_FILE = 'token.json'
CREDENTIALS_FILE = 'credentials.json'

def setup_oauth():
    """Set up OAuth authentication and generate token.json"""
    
    print("🔐 Google OAuth Setup for Telegram KPI Bot")
    print("=" * 50)
    
    # Check if credentials.json exists
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"❌ Error: {CREDENTIALS_FILE} not found!")
        print("\n📋 To get credentials.json:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Select your project")
        print("3. Go to 'APIs & Services' > 'Credentials'")
        print("4. Click 'Create Credentials' > 'OAuth 2.0 Client IDs'")
        print("5. Choose 'Desktop application'")
        print("6. Download the JSON file and rename it to 'credentials.json'")
        print("7. Place it in the same directory as this script")
        return False
    
    creds = None
    
    # Check if token.json already exists
    if os.path.exists(TOKEN_FILE):
        print(f"📄 Found existing {TOKEN_FILE}")
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        
        # Check if credentials are valid
        if creds and creds.valid:
            print("✅ Existing token is valid!")
            print_token_info(creds)
            return True
        
        # Try to refresh expired token
        if creds and creds.expired and creds.refresh_token:
            print("🔄 Token expired, trying to refresh...")
            try:
                creds.refresh(Request())
                print("✅ Token refreshed successfully!")
                save_token(creds)
                print_token_info(creds)
                return True
            except Exception as e:
                print(f"❌ Failed to refresh token: {e}")
                print("🔄 Will create new token...")
    
    # Create new token through OAuth flow
    print("🌐 Starting OAuth flow...")
    print("📱 Your browser will open for authorization")
    
    try:
        flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
        creds = flow.run_local_server(port=0)
        
        print("✅ OAuth flow completed successfully!")
        save_token(creds)
        print_token_info(creds)
        
        return True
        
    except Exception as e:
        print(f"❌ OAuth flow failed: {e}")
        return False

def save_token(creds):
    """Save credentials to token.json"""
    with open(TOKEN_FILE, 'w') as token:
        token.write(creds.to_json())
    print(f"💾 Token saved to {TOKEN_FILE}")

def print_token_info(creds):
    """Print token information"""
    print("\n📊 Token Information:")
    print(f"   Valid: {creds.valid}")
    print(f"   Expired: {creds.expired}")
    print(f"   Has Refresh Token: {bool(creds.refresh_token)}")
    
    if hasattr(creds, 'expiry') and creds.expiry:
        print(f"   Expires: {creds.expiry}")

def generate_env_token():
    """Generate token for environment variable"""
    if not os.path.exists(TOKEN_FILE):
        print(f"❌ {TOKEN_FILE} not found. Run OAuth setup first.")
        return
    
    print("\n🔧 Environment Variable Setup")
    print("=" * 30)
    
    with open(TOKEN_FILE, 'r') as f:
        token_data = f.read()
    
    print("📋 Add this to your Render environment variables:")
    print("\nKey: GOOGLE_OAUTH_TOKEN_JSON")
    print("Value:")
    print(token_data)
    
    print("\n💡 Or use this one-liner format:")
    # Minify JSON for easier copying
    token_obj = json.loads(token_data)
    minified = json.dumps(token_obj, separators=(',', ':'))
    print(minified)

def main():
    """Main function"""
    print("🚀 Welcome to Google OAuth Setup!")
    print("\nWhat would you like to do?")
    print("1. Set up OAuth (generate token.json)")
    print("2. Generate environment variable from existing token.json")
    print("3. Check current token status")
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    if choice == '1':
        if setup_oauth():
            print("\n🎉 OAuth setup completed successfully!")
            print(f"📄 {TOKEN_FILE} has been created")
            print("\n💡 Next steps:")
            print("1. Copy token.json to your project directory")
            print("2. Or run option 2 to get environment variable format")
        else:
            print("\n❌ OAuth setup failed")
    
    elif choice == '2':
        generate_env_token()
    
    elif choice == '3':
        if os.path.exists(TOKEN_FILE):
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
            print_token_info(creds)
        else:
            print(f"❌ {TOKEN_FILE} not found")
    
    else:
        print("❌ Invalid choice")

if __name__ == '__main__':
    main()