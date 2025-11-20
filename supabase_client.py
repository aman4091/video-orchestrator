#!/usr/bin/env python3
"""
Supabase Database Client for F5-TTS Bot
========================================
Handles all database operations including:
- API key management with rotation
- YouTube channel & video tracking
- 15-day processed video cooldown
- Global counter for audio file naming
- Custom prompts storage
- Multi-chat configuration
"""

import os
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from supabase import create_client, Client

class SupabaseClient:
    def __init__(self, url: Optional[str] = None, key: Optional[str] = None):
        """Initialize Supabase client with URL and anon key"""
        self.url = url or os.getenv("SUPABASE_URL")
        self.key = key or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY")

        if not self.url or not self.key:
            print("⚠️ Supabase credentials not set. Use /set_supabase_url and /set_supabase_key commands.")
            self.client = None
            self.supabase = None
        else:
            try:
                self.client: Client = create_client(self.url, self.key)
                self.supabase = self.client  # Compatibility alias
                print("✅ Supabase client initialized")
            except Exception as e:
                self.supabase = None
                print(f"❌ Supabase connection error: {e}")
                self.client = None

    def is_connected(self) -> bool:
        """Check if Supabase client is connected"""
        return self.client is not None

    # =============================================================================
    # TABLE INITIALIZATION
    # =============================================================================

    def init_tables(self) -> bool:
        """
        Initialize all required tables.
        NOTE: This assumes tables are already created in Supabase dashboard.
        Returns True if tables exist, False otherwise.
        """
        if not self.is_connected():
            return False

        try:
            # Check if tables exist by querying them
            tables_to_check = [
                'api_keys', 'youtube_channels', 'processed_videos',
                'prompts', 'chat_configs', 'global_counter', 'audio_links',
                'direct_script_audio'
            ]

            for table in tables_to_check:
                self.client.table(table).select("*").limit(1).execute()

            print("✅ All Supabase tables verified")
            return True
        except Exception as e:
            print(f"⚠️ Table check failed: {e}")
            print("📝 Please create tables using SQL schema in Supabase dashboard")
            return False

    def get_table_creation_sql(self) -> str:
        """Return SQL for creating all required tables"""
        return """
-- API Keys Table
CREATE TABLE IF NOT EXISTS api_keys (
    id BIGSERIAL PRIMARY KEY,
    key_type TEXT NOT NULL CHECK (key_type IN ('youtube', 'supadata', 'deepseek')),
    api_key TEXT NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_used TIMESTAMPTZ DEFAULT NOW(),
    usage_count INTEGER DEFAULT 0
);

-- YouTube Channels Table
CREATE TABLE IF NOT EXISTS youtube_channels (
    id BIGSERIAL PRIMARY KEY,
    channel_url TEXT NOT NULL UNIQUE,
    channel_id TEXT NOT NULL,
    channel_name TEXT,
    videos_json JSONB,  -- Top 1000 videos cache
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

-- Processed Videos Table
CREATE TABLE IF NOT EXISTS processed_videos (
    id BIGSERIAL PRIMARY KEY,
    video_id TEXT NOT NULL,
    video_url TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    processed_date TIMESTAMPTZ DEFAULT NOW(),
    chat_id TEXT NOT NULL,
    audio_counter INTEGER
);

-- Create index for 15-day lookup
CREATE INDEX IF NOT EXISTS idx_processed_videos_date ON processed_videos (video_id, processed_date DESC);

-- Prompts Table
CREATE TABLE IF NOT EXISTS prompts (
    id BIGSERIAL PRIMARY KEY,
    prompt_type TEXT NOT NULL CHECK (prompt_type IN ('deepseek', 'youtube', 'channel')),
    prompt_text TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Chat Configs Table
CREATE TABLE IF NOT EXISTS chat_configs (
    id BIGSERIAL PRIMARY KEY,
    chat_id TEXT NOT NULL UNIQUE,
    chat_name TEXT NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Global Counter Table
CREATE TABLE IF NOT EXISTS global_counter (
    id INTEGER PRIMARY KEY DEFAULT 1,
    counter INTEGER DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CHECK (id = 1)  -- Ensure only one row
);

-- Initialize counter if not exists
INSERT INTO global_counter (id, counter) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

-- Audio Links Table (for download queue)
CREATE TABLE IF NOT EXISTS audio_links (
    id BIGSERIAL PRIMARY KEY,
    enhanced_link TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_audio_links_created ON audio_links (created_at DESC);

-- Direct Script Audio Table (for raw audio storage)
CREATE TABLE IF NOT EXISTS direct_script_audio (
    id BIGSERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    gofile_link TEXT,
    file_size_mb REAL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for faster downloads
CREATE INDEX IF NOT EXISTS idx_direct_script_audio_created ON direct_script_audio (created_at DESC);

-- Default Reference Audio Table (for master reference audio)
CREATE TABLE IF NOT EXISTS default_reference_audio (
    id INTEGER PRIMARY KEY DEFAULT 1,
    filename TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    uploaded_at TIMESTAMPTZ DEFAULT NOW(),
    CHECK (id = 1)  -- Ensure only one row (single master reference)
);
"""

    # =============================================================================
    # API KEY MANAGEMENT
    # =============================================================================

    def store_api_key(self, key_type: str, api_key: str) -> bool:
        """Store or update an API key"""
        if not self.is_connected():
            return False

        try:
            # Check if key already exists
            result = self.client.table('api_keys').select('*').eq('api_key', api_key).execute()

            if result.data:
                # Update existing key
                self.client.table('api_keys').update({
                    'is_active': True,
                    'last_used': datetime.now().isoformat()
                }).eq('api_key', api_key).execute()
            else:
                # Insert new key
                self.client.table('api_keys').insert({
                    'key_type': key_type,
                    'api_key': api_key,
                    'is_active': True
                }).execute()

            print(f"✅ {key_type} API key stored")
            return True
        except Exception as e:
            print(f"❌ Error storing API key: {e}")
            return False

    def get_active_api_key(self, key_type: str) -> Optional[str]:
        """Get an active API key of specified type"""
        if not self.is_connected():
            return None

        try:
            result = self.client.table('api_keys')\
                .select('api_key')\
                .eq('key_type', key_type)\
                .eq('is_active', True)\
                .order('last_used', desc=False)\
                .limit(1)\
                .execute()

            if result.data:
                key = result.data[0]['api_key']
                # Update last_used
                self.client.table('api_keys')\
                    .update({'last_used': datetime.now().isoformat()})\
                    .eq('api_key', key)\
                    .execute()
                return key
            return None
        except Exception as e:
            print(f"❌ Error getting API key: {e}")
            return None

    def mark_key_exhausted(self, api_key: str) -> bool:
        """Mark an API key as exhausted (inactive)"""
        if not self.is_connected():
            return False

        try:
            self.client.table('api_keys')\
                .update({'is_active': False})\
                .eq('api_key', api_key)\
                .execute()
            print(f"⚠️ API key marked as exhausted")
            return True
        except Exception as e:
            print(f"❌ Error marking key exhausted: {e}")
            return False

    def rotate_supadata_key(self) -> Optional[str]:
        """
        Rotate to next available Supadata API key.
        Returns next active key or None if all exhausted.
        """
        return self.get_active_api_key('supadata')

    def get_all_api_keys_status(self) -> List[Dict[str, Any]]:
        """Get status of all API keys"""
        if not self.is_connected():
            return []

        try:
            result = self.client.table('api_keys')\
                .select('key_type, api_key, is_active, last_used, usage_count')\
                .execute()

            return result.data if result.data else []
        except Exception as e:
            print(f"❌ Error getting API keys status: {e}")
            return []

    # =============================================================================
    # YOUTUBE CHANNEL & VIDEO MANAGEMENT
    # =============================================================================

    def store_youtube_channel(self, channel_url: str, channel_id: str,
                              channel_name: str, videos: List[Dict]) -> bool:
        """Store or update YouTube channel with top 1000 videos"""
        if not self.is_connected():
            return False

        try:
            data = {
                'channel_url': channel_url,
                'channel_id': channel_id,
                'channel_name': channel_name,
                'videos_json': json.dumps(videos),
                'last_updated': datetime.now().isoformat()
            }

            # Upsert (insert or update) - use channel_url as conflict resolution key
            self.client.table('youtube_channels')\
                .upsert(data, on_conflict='channel_url')\
                .execute()
            print(f"✅ Channel cached: {channel_name} ({len(videos)} videos)")
            return True
        except Exception as e:
            print(f"❌ Error storing channel: {e}")
            return False

    def get_youtube_channel(self, channel_url: str) -> Optional[Dict]:
        """Get cached YouTube channel data"""
        if not self.is_connected():
            return None

        try:
            result = self.client.table('youtube_channels')\
                .select('*')\
                .eq('channel_url', channel_url)\
                .execute()

            if result.data:
                channel = result.data[0]
                # Parse videos JSON
                if channel.get('videos_json'):
                    channel['videos'] = json.loads(channel['videos_json'])
                return channel
            return None
        except Exception as e:
            print(f"❌ Error getting channel: {e}")
            return None

    def mark_video_processed(self, video_id: str, video_url: str, channel_id: str,
                            chat_id: str, audio_counter: int) -> bool:
        """Mark a video as processed"""
        if not self.is_connected():
            return False

        try:
            self.client.table('processed_videos').insert({
                'video_id': video_id,
                'video_url': video_url,
                'channel_id': channel_id,
                'processed_date': datetime.now().isoformat(),
                'chat_id': chat_id,
                'audio_counter': audio_counter
            }).execute()
            print(f"✅ Video marked as processed: {video_id}")
            return True
        except Exception as e:
            print(f"❌ Error marking video processed: {e}")
            return False

    def get_unprocessed_videos(self, video_ids: List[str], days: int = 15) -> List[str]:
        """
        Get list of video IDs that haven't been processed in the last N days.
        Returns IDs that are NOT in processed_videos within the time window.
        """
        if not self.is_connected():
            return video_ids  # Return all if DB not connected

        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()

            # Get recently processed video IDs
            result = self.client.table('processed_videos')\
                .select('video_id')\
                .in_('video_id', video_ids)\
                .gte('processed_date', cutoff_date)\
                .execute()

            recent_ids = {row['video_id'] for row in result.data} if result.data else set()

            # Return videos NOT in recent list
            unprocessed = [vid for vid in video_ids if vid not in recent_ids]
            print(f"📊 Unprocessed videos: {len(unprocessed)}/{len(video_ids)} (last {days} days)")
            return unprocessed
        except Exception as e:
            print(f"❌ Error checking processed videos: {e}")
            return video_ids  # Return all on error

    # =============================================================================
    # GLOBAL COUNTER MANAGEMENT
    # =============================================================================

    def get_counter(self) -> int:
        """Get current global counter value"""
        if not self.is_connected():
            return 0

        try:
            result = self.client.table('global_counter')\
                .select('counter')\
                .eq('id', 1)\
                .execute()

            if result.data:
                return result.data[0]['counter']
            return 0
        except Exception as e:
            print(f"❌ Error getting counter: {e}")
            return 0

    def increment_counter(self) -> int:
        """Increment global counter and return new value (atomic operation)"""
        if not self.is_connected():
            return 0

        try:
            # Use PostgreSQL RPC for atomic increment
            # This ensures no race conditions between workers
            result = self.client.rpc('increment_global_counter').execute()

            if result.data:
                return result.data

            # Fallback: If RPC not available, use non-atomic method
            current = self.get_counter()
            new_value = current + 1

            self.client.table('global_counter')\
                .update({'counter': new_value, 'updated_at': datetime.now().isoformat()})\
                .eq('id', 1)\
                .execute()

            return new_value
        except Exception as e:
            print(f"❌ Error incrementing counter: {e}")
            # Emergency fallback: use timestamp-based unique ID
            import time
            return int(time.time() * 1000) % 1000000

    # =============================================================================
    # CHANNEL-SPECIFIC COUNTER (for channel-wise audio numbering)
    # =============================================================================

    def get_channel_counter(self, channel_name: str) -> int:
        """Get current counter value for specific channel"""
        if not self.is_connected():
            return 0

        try:
            result = self.client.table('channel_counters')\
                .select('counter')\
                .eq('channel_name', channel_name)\
                .execute()

            if result.data and len(result.data) > 0:
                return result.data[0]['counter']
            return 0
        except Exception as e:
            print(f"❌ Error getting channel counter for {channel_name}: {e}")
            return 0

    def increment_channel_counter(self, channel_name: str) -> int:
        """
        Increment counter for specific channel and return new value.
        Auto-creates counter entry if channel is new (starts from 1).
        """
        if not self.is_connected():
            return 0

        try:
            # Check if counter exists for this channel
            current = self.get_channel_counter(channel_name)

            if current > 0:
                # Channel exists, increment counter
                new_value = current + 1
                self.client.table('channel_counters')\
                    .update({'counter': new_value, 'updated_at': datetime.now().isoformat()})\
                    .eq('channel_name', channel_name)\
                    .execute()
                print(f"✅ Channel '{channel_name}' counter: {current} → {new_value}")
                return new_value
            else:
                # New channel, create entry with counter = 1
                self.client.table('channel_counters')\
                    .insert({
                        'channel_name': channel_name,
                        'counter': 1,
                        'created_at': datetime.now().isoformat(),
                        'updated_at': datetime.now().isoformat()
                    })\
                    .execute()
                print(f"✅ New channel '{channel_name}' created with counter = 1")
                return 1

        except Exception as e:
            print(f"❌ Error incrementing channel counter for {channel_name}: {e}")
            # Fallback: return timestamp-based counter
            return int(datetime.now().timestamp()) % 10000

    # =============================================================================
    # GOOGLE DRIVE SCRIPT PROCESSING TRACKING
    # =============================================================================

    def is_script_processed(self, channel_folder: str, script_filename: str) -> bool:
        """Check if a Google Drive script has been processed"""
        if not self.is_connected():
            return False

        try:
            result = self.client.table('processed_gdrive_scripts')\
                .select('id')\
                .eq('channel_folder', channel_folder)\
                .eq('script_filename', script_filename)\
                .execute()

            return len(result.data) > 0
        except Exception as e:
            print(f"⚠️ Error checking script status: {e}")
            return False

    def mark_script_processed(self, channel_folder: str, channel_shortform: str,
                             script_filename: str, script_path: str, audio_counter: int,
                             gofile_link: str = None, gdrive_file_id: str = None) -> bool:
        """Mark a Google Drive script as processed"""
        if not self.is_connected():
            return False

        try:
            data = {
                'channel_folder': channel_folder,
                'channel_shortform': channel_shortform,
                'script_filename': script_filename,
                'script_path': script_path,
                'audio_counter': audio_counter,
                'gofile_link': gofile_link,
                'gdrive_file_id': gdrive_file_id,
                'processed_at': datetime.now().isoformat()
            }

            self.client.table('processed_gdrive_scripts').insert(data).execute()
            print(f"✅ Marked {script_filename} as processed (counter: {audio_counter})")
            return True
        except Exception as e:
            print(f"❌ Error marking script as processed: {e}")
            return False

    def get_processed_scripts(self, channel_folder: str = None) -> list:
        """Get list of processed scripts (optionally filtered by channel)"""
        if not self.is_connected():
            return []

        try:
            query = self.client.table('processed_gdrive_scripts').select('*')

            if channel_folder:
                query = query.eq('channel_folder', channel_folder)

            result = query.order('processed_at', desc=True).execute()
            return result.data
        except Exception as e:
            print(f"❌ Error fetching processed scripts: {e}")
            return []

    # =============================================================================
    # PROMPT MANAGEMENT
    # =============================================================================

    def save_prompt(self, prompt_type: str, prompt_text: str) -> bool:
        """Save or update a prompt"""
        if not self.is_connected():
            return False

        try:
            # Upsert based on prompt_type
            data = {
                'prompt_type': prompt_type,
                'prompt_text': prompt_text,
                'updated_at': datetime.now().isoformat()
            }

            # Check if exists
            result = self.client.table('prompts')\
                .select('id')\
                .eq('prompt_type', prompt_type)\
                .execute()

            if result.data:
                # Update existing
                self.client.table('prompts')\
                    .update(data)\
                    .eq('prompt_type', prompt_type)\
                    .execute()
            else:
                # Insert new
                self.client.table('prompts').insert(data).execute()

            print(f"✅ {prompt_type} prompt saved")
            return True
        except Exception as e:
            print(f"❌ Error saving prompt: {e}")
            return False

    def get_prompt(self, prompt_type: str) -> Optional[str]:
        """Get a prompt by type"""
        if not self.is_connected():
            return None

        try:
            result = self.client.table('prompts')\
                .select('prompt_text')\
                .eq('prompt_type', prompt_type)\
                .execute()

            if result.data:
                return result.data[0]['prompt_text']
            return None
        except Exception as e:
            print(f"❌ Error getting prompt: {e}")
            return None

    # =============================================================================
    # CHAT CONFIGURATION
    # =============================================================================

    def add_chat_config(self, chat_id: str, chat_name: str) -> bool:
        """Add or update chat configuration"""
        if not self.is_connected():
            return False

        try:
            data = {
                'chat_id': chat_id,
                'chat_name': chat_name,
                'is_active': True
            }

            self.client.table('chat_configs').upsert(data).execute()
            print(f"✅ Chat config saved: {chat_name} ({chat_id})")
            return True
        except Exception as e:
            print(f"❌ Error saving chat config: {e}")
            return False

    def get_active_chats(self) -> List[Dict]:
        """Get all active chat configurations"""
        if not self.is_connected():
            return []

        try:
            result = self.client.table('chat_configs')\
                .select('*')\
                .eq('is_active', True)\
                .execute()

            return result.data if result.data else []
        except Exception as e:
            print(f"❌ Error getting active chats: {e}")
            return []

    # =============================================================================
    # AUDIO LINKS MANAGEMENT (for download queue)
    # =============================================================================

    def save_audio_link(self, enhanced_link: str) -> bool:
        """Save enhanced audio link to database for later processing"""
        if not self.is_connected():
            return False

        try:
            self.client.table('audio_links').insert({
                'enhanced_link': enhanced_link,
                'created_at': datetime.now().isoformat()
            }).execute()
            print(f"✅ Audio link saved to database")
            return True
        except Exception as e:
            print(f"❌ Error saving audio link: {e}")
            return False

    def get_pending_audio_links(self) -> List[Dict]:
        """Fetch all pending audio links from database"""
        if not self.is_connected():
            return []

        try:
            result = self.client.table('audio_links')\
                .select('id, enhanced_link')\
                .order('created_at', desc=False)\
                .execute()

            return result.data if result.data else []
        except Exception as e:
            print(f"❌ Error fetching audio links: {e}")
            return []

    def delete_audio_link(self, link_id: int) -> bool:
        """Delete processed audio link from database"""
        if not self.is_connected():
            return False

        try:
            self.client.table('audio_links')\
                .delete()\
                .eq('id', link_id)\
                .execute()
            print(f"✅ Audio link deleted from database (ID: {link_id})")
            return True
        except Exception as e:
            print(f"❌ Error deleting audio link: {e}")
            return False

    # =============================================================================
    # DIRECT SCRIPT RAW AUDIO STORAGE (Supabase Storage Integration)
    # =============================================================================

    def upload_raw_audio(self, file_path: str, bucket_name: str = "raw_audio_files") -> Optional[str]:
        """
        Upload raw audio file to Supabase Storage.
        Returns storage path on success, None on failure.
        """
        if not self.is_connected():
            return None

        try:
            import os
            filename = os.path.basename(file_path)

            # Read file
            with open(file_path, 'rb') as f:
                file_data = f.read()

            # Upload to storage
            storage_path = f"audio/{filename}"
            result = self.client.storage.from_(bucket_name).upload(
                path=storage_path,
                file=file_data,
                file_options={"content-type": "audio/wav"}
            )

            print(f"✅ Raw audio uploaded to Supabase Storage: {storage_path}")
            return storage_path
        except Exception as e:
            print(f"❌ Error uploading raw audio to Supabase: {e}")
            return None

    def save_direct_script_audio(self, filename: str, storage_path: str,
                                 gofile_link: Optional[str] = None,
                                 file_size_mb: Optional[float] = None) -> bool:
        """Save direct script audio metadata to database"""
        if not self.is_connected():
            return False

        try:
            self.client.table('direct_script_audio').insert({
                'filename': filename,
                'storage_path': storage_path,
                'gofile_link': gofile_link,
                'file_size_mb': file_size_mb,
                'created_at': datetime.now().isoformat()
            }).execute()
            print(f"✅ Direct script audio metadata saved")
            return True
        except Exception as e:
            print(f"❌ Error saving direct script audio: {e}")
            return False

    def get_pending_downloads(self) -> List[Dict]:
        """Fetch all pending audio files to download from Supabase"""
        if not self.is_connected():
            return []

        try:
            result = self.client.table('direct_script_audio')\
                .select('id, filename, storage_path, gofile_link, file_size_mb, created_at')\
                .order('created_at', desc=False)\
                .execute()

            return result.data if result.data else []
        except Exception as e:
            print(f"❌ Error fetching pending downloads: {e}")
            return []

    def download_audio_file(self, storage_path: str, local_path: str,
                           bucket_name: str = "raw_audio_files") -> bool:
        """
        Download audio file from Supabase Storage to local path.
        Returns True on success, False on failure.
        """
        if not self.is_connected():
            return False

        try:
            # Download from storage
            result = self.client.storage.from_(bucket_name).download(storage_path)

            # Save to local file
            import os
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            with open(local_path, 'wb') as f:
                f.write(result)

            print(f"✅ Audio downloaded: {os.path.basename(local_path)}")
            return True
        except Exception as e:
            print(f"❌ Error downloading audio: {e}")
            return False

    def delete_direct_script_audio(self, audio_id: int, storage_path: str,
                                   bucket_name: str = "raw_audio_files") -> bool:
        """
        Delete audio file from both Supabase Storage and database.
        Returns True if successful, False otherwise.
        """
        if not self.is_connected():
            return False

        try:
            # Delete from storage
            try:
                self.client.storage.from_(bucket_name).remove([storage_path])
                print(f"✅ Audio deleted from Supabase Storage: {storage_path}")
            except Exception as e:
                print(f"⚠️ Storage deletion warning: {e}")

            # Delete from database
            self.client.table('direct_script_audio')\
                .delete()\
                .eq('id', audio_id)\
                .execute()
            print(f"✅ Audio metadata deleted from database (ID: {audio_id})")
            return True
        except Exception as e:
            print(f"❌ Error deleting direct script audio: {e}")
            return False

    # =============================================================================
    # DEFAULT REFERENCE AUDIO MANAGEMENT
    # =============================================================================

    def upload_default_reference(self, file_path: str, bucket_name: str = "reference_audio") -> Optional[str]:
        """
        Upload default reference audio to Supabase Storage.
        This will be the master reference for all instances.
        Returns storage path on success, None on failure.
        """
        if not self.is_connected():
            return None

        try:
            import os
            filename = os.path.basename(file_path)

            # Read file
            with open(file_path, 'rb') as f:
                file_data = f.read()

            # Upload to storage
            storage_path = f"default/{filename}"

            # Delete old file if exists (replace)
            try:
                self.client.storage.from_(bucket_name).remove([storage_path])
            except:
                pass  # Ignore if doesn't exist

            result = self.client.storage.from_(bucket_name).upload(
                path=storage_path,
                file=file_data,
                file_options={"content-type": "audio/wav", "upsert": "true"}
            )

            print(f"✅ Default reference uploaded to Supabase Storage: {storage_path}")
            return storage_path
        except Exception as e:
            print(f"❌ Error uploading default reference to Supabase: {e}")
            return None

    def save_default_reference_metadata(self, filename: str, storage_path: str) -> bool:
        """
        Save default reference audio metadata to database.
        Only one row will exist (master reference).
        """
        if not self.is_connected():
            return False

        try:
            # Upsert (replace if exists)
            self.client.table('default_reference_audio').upsert({
                'id': 1,
                'filename': filename,
                'storage_path': storage_path,
                'uploaded_at': datetime.now().isoformat()
            }).execute()
            print(f"✅ Default reference metadata saved")
            return True
        except Exception as e:
            print(f"❌ Error saving default reference metadata: {e}")
            return False

    def get_default_reference(self) -> Optional[Dict]:
        """
        Get default reference audio metadata from database.
        Returns dict with filename and storage_path, or None if not set.
        """
        if not self.is_connected():
            return None

        try:
            result = self.client.table('default_reference_audio')\
                .select('filename, storage_path, uploaded_at')\
                .eq('id', 1)\
                .execute()

            if result.data:
                return result.data[0]
            return None
        except Exception as e:
            print(f"❌ Error getting default reference: {e}")
            return None

    def download_default_reference(self, local_path: str, bucket_name: str = "reference_audio") -> bool:
        """
        Download default reference audio from Supabase Storage to local path.
        Returns True on success, False on failure.
        """
        if not self.is_connected():
            return False

        try:
            # Get metadata first
            ref_data = self.get_default_reference()
            if not ref_data:
                print("⚠️ No default reference audio set")
                return False

            storage_path = ref_data['storage_path']

            # Download from storage
            result = self.client.storage.from_(bucket_name).download(storage_path)

            # Save to local file
            import os
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            with open(local_path, 'wb') as f:
                f.write(result)

            print(f"✅ Default reference downloaded: {os.path.basename(local_path)}")
            return True
        except Exception as e:
            print(f"❌ Error downloading default reference: {e}")
            return False

    # =========================================================================
    # VIDEO GENERATION SETTINGS
    # =========================================================================

    def get_video_settings(self, chat_id):
        """
        Get video generation settings for a chat

        Args:
            chat_id: Telegram chat ID

        Returns:
            dict: Video settings or default settings if not found
        """
        try:
            if not self.is_connected():
                return self._default_video_settings()

            result = self.client.table('video_settings').select('*').eq('chat_id', str(chat_id)).execute()

            if result.data and len(result.data) > 0:
                settings = result.data[0]
                # If chat doesn't have folder ID set, use current global folder
                if not settings.get('gdrive_image_folder_id'):
                    settings['gdrive_image_folder_id'] = self.get_current_image_folder()
                return settings
            else:
                # Return default settings
                return self._default_video_settings()

        except Exception as e:
            print(f"❌ Error getting video settings: {e}")
            return self._default_video_settings()

    def _default_video_settings(self):
        """Default video settings (matches F:\Scripts\God's message\banner.ass)"""
        return {
            'chat_id': None,
            'video_enabled': True,  # Default ON
            'subtitle_style': 'Style: Banner,Arial,48,&H00FFFFFF,&H00FFFFFF,&H80000000,&H80000000,-1,0,0,0,100,100,0,0,4,0,0,5,40,40,40,1',
            'gdrive_image_folder_id': self.get_current_image_folder()  # Use current selected folder
        }

    def set_video_enabled(self, chat_id, enabled):
        """
        Enable/disable video generation for a chat

        Args:
            chat_id: Telegram chat ID
            enabled: True to enable, False to disable

        Returns:
            bool: Success status
        """
        try:
            if not self.is_connected():
                print("⚠️ Supabase not connected - cannot save video settings")
                return False

            # Upsert (insert or update)
            data = {
                'chat_id': str(chat_id),
                'video_enabled': enabled,
                'updated_at': 'NOW()'
            }

            result = self.client.table('video_settings').upsert(data).execute()

            status = "ENABLED" if enabled else "DISABLED"
            print(f"✅ Video generation {status} for chat {chat_id}")
            return True

        except Exception as e:
            print(f"❌ Error setting video enabled: {e}")
            return False

    def set_subtitle_style(self, chat_id, style):
        """
        Set custom subtitle style for a chat

        Args:
            chat_id: Telegram chat ID
            style: ASS style string

        Returns:
            bool: Success status
        """
        try:
            if not self.is_connected():
                print("⚠️ Supabase not connected - cannot save subtitle style")
                return False

            # Upsert (insert or update)
            data = {
                'chat_id': str(chat_id),
                'subtitle_style': style,
                'updated_at': 'NOW()'
            }

            result = self.client.table('video_settings').upsert(data).execute()

            print(f"✅ Subtitle style updated for chat {chat_id}")
            return True

        except Exception as e:
            print(f"❌ Error setting subtitle style: {e}")
            return False

    def set_gdrive_image_folder(self, chat_id, folder_id):
        """
        Set Google Drive image folder for a chat

        Args:
            chat_id: Telegram chat ID
            folder_id: Google Drive folder ID

        Returns:
            bool: Success status
        """
        try:
            if not self.is_connected():
                print("⚠️ Supabase not connected - cannot save folder ID")
                return False

            # Upsert (insert or update)
            data = {
                'chat_id': str(chat_id),
                'gdrive_image_folder_id': folder_id,
                'updated_at': 'NOW()'
            }

            result = self.client.table('video_settings').upsert(data).execute()

            print(f"✅ GDrive image folder set for chat {chat_id}: {folder_id}")
            return True

        except Exception as e:
            print(f"❌ Error setting GDrive folder: {e}")
            return False

    def save_video_output(self, counter, chat_id, audio_path, video_path, gdrive_link, gofile_link, subtitle_style):
        """
        Save video output to database (for logging)

        Args:
            counter: Global counter
            chat_id: Telegram chat ID
            audio_path: Path to audio file
            video_path: Path to video file
            gdrive_link: Google Drive link
            gofile_link: Gofile link
            subtitle_style: ASS style used

        Returns:
            bool: Success status
        """
        try:
            if not self.is_connected():
                print("⚠️ Supabase not connected - video output not saved")
                return False

            data = {
                'counter': counter,
                'chat_id': str(chat_id),
                'audio_path': audio_path,
                'video_path': video_path,
                'gdrive_link': gdrive_link,
                'gofile_link': gofile_link,
                'subtitle_style_used': subtitle_style
            }

            result = self.client.table('video_outputs').insert(data).execute()

            print(f"✅ Video output saved to database (counter: {counter})")
            return True

        except Exception as e:
            print(f"❌ Error saving video output: {e}")
            return False

    # =========================================================================
    # IMAGE FOLDER MANAGEMENT
    # =========================================================================

    def get_folder_mapping(self):
        """
        Get mapping of folder numbers to Google Drive folder IDs

        Returns:
            dict: Folder number → {id, name} mapping
        """
        return {
            0: {
                'id': os.getenv('GDRIVE_IMAGE_FOLDER_DEFAULT'),
                'name': 'Nature'
            },
            1: {
                'id': os.getenv('GDRIVE_IMAGE_FOLDER_JESUS'),
                'name': 'Jesus'
            },
            2: {
                'id': os.getenv('GDRIVE_IMAGE_FOLDER_SHORTS'),
                'name': 'Shorts'
            }
        }

    def get_current_image_folder(self):
        """
        Get current active image folder ID (global setting)

        Returns:
            str: Google Drive folder ID
        """
        try:
            if not self.is_connected():
                # Return default folder if not connected
                return os.getenv('GDRIVE_IMAGE_FOLDER_DEFAULT')

            # Get global setting (we'll use chat_id='global' for global settings)
            result = self.client.table('video_settings').select('gdrive_image_folder_id').eq('chat_id', 'global').execute()

            if result.data and len(result.data) > 0:
                folder_id = result.data[0].get('gdrive_image_folder_id')
                if folder_id:
                    return folder_id

            # Return default if no setting found
            return os.getenv('GDRIVE_IMAGE_FOLDER_DEFAULT')

        except Exception as e:
            print(f"❌ Error getting current folder: {e}")
            return os.getenv('GDRIVE_IMAGE_FOLDER_DEFAULT')

    def is_jesus_folder_active(self):
        """
        Check if Jesus folder is currently active (for multi-image support)

        Returns:
            bool: True if Jesus folder is active
        """
        try:
            current_folder = self.get_current_image_folder()
            jesus_folder = os.getenv('GDRIVE_IMAGE_FOLDER_JESUS')
            return current_folder == jesus_folder
        except Exception as e:
            print(f"❌ Error checking Jesus folder: {e}")
            return False

    def set_current_image_folder(self, folder_number: int):
        """
        Set current active image folder (global setting)

        Args:
            folder_number: Folder index (0=Nature, 1=Jesus, 2=Shorts)

        Returns:
            tuple: (success: bool, folder_name: str)
        """
        try:
            if not self.is_connected():
                return False, "Database not connected"

            folder_map = self.get_folder_mapping()

            if folder_number not in folder_map:
                return False, f"Invalid folder number. Use 0-{len(folder_map)-1}"

            folder_info = folder_map[folder_number]
            folder_id = folder_info['id']
            folder_name = folder_info['name']

            if not folder_id:
                return False, f"Folder ID not configured in .env"

            # Upsert global setting
            data = {
                'chat_id': 'global',
                'gdrive_image_folder_id': folder_id,
                'updated_at': 'NOW()'
            }

            result = self.client.table('video_settings').upsert(data).execute()

            print(f"✅ Image folder set to: {folder_name} ({folder_id})")
            return True, folder_name

        except Exception as e:
            print(f"❌ Error setting image folder: {e}")
            return False, str(e)
