"""
Channel Management Module for 7-Day Multi-Channel System
Handles CRUD operations for channels and their configurations
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class ChannelManager:
    """Manages YouTube channel configurations"""

    def __init__(self, supabase_client: SupabaseClient):
        self.supabase = supabase_client.supabase

    async def add_channel(
        self,
        channel_name: str,
        display_name: Optional[str] = None,
        channel_url: Optional[str] = None,
        channel_youtube_id: Optional[str] = None,
        reference_audio_id: Optional[str] = None,
        reference_audio_url: Optional[str] = None,
        gdrive_folder_id: Optional[str] = None,
        daily_video_target: int = 4
    ) -> Dict[str, Any]:
        """
        Add a new channel to the system

        Args:
            channel_name: Short name (GYH, BI, etc.)
            display_name: Full display name
            channel_url: YouTube channel URL
            channel_youtube_id: YouTube channel ID
            reference_audio_id: Supabase storage ID for voice
            reference_audio_url: Direct URL to reference audio
            gdrive_folder_id: Google Drive folder ID
            daily_video_target: Videos per day (default 4)

        Returns:
            Channel data dict
        """
        try:
            data = {
                'channel_name': channel_name.strip(),
                'channel_display_name': display_name or channel_name,
                'channel_url': channel_url,
                'channel_youtube_id': channel_youtube_id,
                'reference_audio_id': reference_audio_id,
                'reference_audio_url': reference_audio_url,
                'gdrive_base_folder_id': gdrive_folder_id,
                'daily_video_target': daily_video_target,
                'is_active': True
            }

            result = self.supabase.table('channels').insert(data).execute()

            if result.data:
                logger.info(f"Channel added: {channel_name}")
                return result.data[0]
            else:
                raise Exception("Failed to add channel")

        except Exception as e:
            logger.error(f"Error adding channel {channel_name}: {e}")
            raise

    async def update_channel(
        self,
        channel_name: str,
        **updates
    ) -> Dict[str, Any]:
        """
        Update channel configuration

        Args:
            channel_name: Channel to update
            **updates: Fields to update

        Returns:
            Updated channel data
        """
        try:
            # Add updated timestamp
            updates['updated_at'] = datetime.utcnow().isoformat()

            result = self.supabase.table('channels').update(updates).eq(
                'channel_name', channel_name
            ).execute()

            if result.data:
                logger.info(f"Channel updated: {channel_name}")
                return result.data[0]
            else:
                raise Exception(f"Channel not found: {channel_name}")

        except Exception as e:
            logger.error(f"Error updating channel {channel_name}: {e}")
            raise

    async def delete_channel(self, channel_name: str) -> bool:
        """
        Delete a channel (soft delete - sets inactive)

        Args:
            channel_name: Channel to delete

        Returns:
            True if successful
        """
        try:
            result = self.supabase.table('channels').update({
                'is_active': False,
                'updated_at': datetime.utcnow().isoformat()
            }).eq('channel_name', channel_name).execute()

            if result.data:
                logger.info(f"Channel deactivated: {channel_name}")
                return True
            return False

        except Exception as e:
            logger.error(f"Error deleting channel {channel_name}: {e}")
            raise

    async def get_channel(self, channel_name: str) -> Optional[Dict[str, Any]]:
        """
        Get channel by name

        Args:
            channel_name: Channel name

        Returns:
            Channel data or None
        """
        try:
            result = self.supabase.table('channels').select('*').eq(
                'channel_name', channel_name
            ).execute()

            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(f"Error getting channel {channel_name}: {e}")
            return None

    async def get_channel_by_id(self, channel_id: int) -> Optional[Dict[str, Any]]:
        """Get channel by ID"""
        try:
            result = self.supabase.table('channels').select('*').eq(
                'id', channel_id
            ).execute()

            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(f"Error getting channel ID {channel_id}: {e}")
            return None

    async def list_channels(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        List all channels

        Args:
            active_only: Only return active channels

        Returns:
            List of channel dicts
        """
        try:
            query = self.supabase.table('channels').select('*')

            if active_only:
                query = query.eq('is_active', True)

            result = query.order('channel_name').execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(f"Error listing channels: {e}")
            return []

    async def set_reference_audio(
        self,
        channel_name: str,
        audio_id: str,
        audio_url: str
    ) -> bool:
        """
        Set reference audio for a channel

        Args:
            channel_name: Channel name
            audio_id: Storage ID
            audio_url: Direct URL

        Returns:
            True if successful
        """
        try:
            return await self.update_channel(
                channel_name,
                reference_audio_id=audio_id,
                reference_audio_url=audio_url
            )

        except Exception as e:
            logger.error(f"Error setting reference audio for {channel_name}: {e}")
            return False

    async def get_channel_names(self, active_only: bool = True) -> List[str]:
        """
        Get list of channel names only

        Args:
            active_only: Only active channels

        Returns:
            List of channel names
        """
        channels = await self.list_channels(active_only)
        return [ch['channel_name'] for ch in channels]

    async def get_channel_count(self, active_only: bool = True) -> int:
        """Get total channel count"""
        channels = await self.list_channels(active_only)
        return len(channels)

    async def is_channel_active(self, channel_name: str) -> bool:
        """Check if channel is active"""
        channel = await self.get_channel(channel_name)
        return channel and channel.get('is_active', False)

    async def activate_channel(self, channel_name: str) -> bool:
        """Activate a channel"""
        try:
            result = await self.update_channel(channel_name, is_active=True)
            return bool(result)
        except:
            return False

    async def deactivate_channel(self, channel_name: str) -> bool:
        """Deactivate a channel"""
        try:
            result = await self.update_channel(channel_name, is_active=False)
            return bool(result)
        except:
            return False

    async def get_channel_stats(self, channel_name: str) -> Dict[str, Any]:
        """
        Get statistics for a channel

        Returns:
            Dict with stats like total videos, completion rate, etc.
        """
        try:
            channel = await self.get_channel(channel_name)
            if not channel:
                return {}

            channel_id = channel['id']

            # Get total daily_uploads for this channel
            result = self.supabase.table('daily_uploads').select(
                'video_status',
                count='exact'
            ).eq('channel_id', channel_id).execute()

            total = result.count if hasattr(result, 'count') else 0

            # Get completed count
            completed_result = self.supabase.table('daily_uploads').select(
                'id',
                count='exact'
            ).eq('channel_id', channel_id).eq('video_status', 'completed').execute()

            completed = completed_result.count if hasattr(completed_result, 'count') else 0

            completion_rate = (completed / total * 100) if total > 0 else 0

            return {
                'channel_name': channel_name,
                'total_videos': total,
                'completed_videos': completed,
                'completion_rate': round(completion_rate, 2),
                'is_active': channel['is_active']
            }

        except Exception as e:
            logger.error(f"Error getting stats for {channel_name}: {e}")
            return {}

    def format_channel_list(self, channels: List[Dict[str, Any]]) -> str:
        """
        Format channel list for display

        Args:
            channels: List of channel dicts

        Returns:
            Formatted string
        """
        if not channels:
            return "No channels configured."

        lines = ["📋 CONFIGURED CHANNELS:\n"]

        for ch in channels:
            status = "✅" if ch['is_active'] else "❌"
            name = ch['channel_name']
            display = ch.get('channel_display_name', name)
            target = ch.get('daily_video_target', 4)
            has_voice = "🎤" if ch.get('reference_audio_id') else "⚪"

            lines.append(f"{status} {name} ({display})")
            lines.append(f"   Videos/day: {target} | Voice: {has_voice}")

        return "\n".join(lines)


class DailyUploadManager:
    """Manages daily_uploads table operations"""

    def __init__(self, supabase_client: SupabaseClient):
        self.supabase = supabase_client.supabase

    async def create_upload_entry(
        self,
        channel_id: int,
        upload_date: str,  # YYYY-MM-DD format
        video_number: int
    ) -> Dict[str, Any]:
        """
        Create a daily_upload entry

        Args:
            channel_id: Channel ID
            upload_date: Date string
            video_number: 1-4

        Returns:
            Created entry data
        """
        try:
            data = {
                'channel_id': channel_id,
                'upload_date': upload_date,
                'video_number': video_number,
                'script_status': 'pending',
                'thumbnail_status': 'pending',
                'video_status': 'pending',
                'audio_status': 'pending'
            }

            result = self.supabase.table('daily_uploads').upsert(
                data,
                on_conflict='channel_id,upload_date,video_number'
            ).execute()

            if result.data:
                return result.data[0]
            else:
                raise Exception("Failed to create upload entry")

        except Exception as e:
            logger.error(f"Error creating upload entry: {e}")
            raise

    async def update_script(
        self,
        channel_id: int,
        upload_date: str,
        video_number: int,
        script_text: str
    ) -> Dict[str, Any]:
        """Update script for an upload"""
        try:
            result = self.supabase.table('daily_uploads').update({
                'script_text': script_text,
                'script_status': 'received',
                'script_received_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }).eq('channel_id', channel_id).eq(
                'upload_date', upload_date
            ).eq('video_number', video_number).execute()

            if result.data:
                logger.info(f"Script updated for {channel_id}/{upload_date}/V{video_number}")
                return result.data[0]
            else:
                raise Exception("Upload entry not found")

        except Exception as e:
            logger.error(f"Error updating script: {e}")
            raise

    async def update_thumbnail(
        self,
        channel_id: int,
        upload_date: str,
        video_number: int,
        gdrive_id: str,
        gdrive_url: str
    ) -> Dict[str, Any]:
        """Update thumbnail for an upload"""
        try:
            result = self.supabase.table('daily_uploads').update({
                'thumbnail_gdrive_id': gdrive_id,
                'thumbnail_gdrive_url': gdrive_url,
                'thumbnail_status': 'received',
                'updated_at': datetime.utcnow().isoformat()
            }).eq('channel_id', channel_id).eq(
                'upload_date', upload_date
            ).eq('video_number', video_number).execute()

            if result.data:
                logger.info(f"Thumbnail updated for {channel_id}/{upload_date}/V{video_number}")
                return result.data[0]
            else:
                raise Exception("Upload entry not found")

        except Exception as e:
            logger.error(f"Error updating thumbnail: {e}")
            raise

    async def update_video_status(
        self,
        channel_id: int,
        upload_date: str,
        video_number: int,
        status: str,
        video_gdrive_id: Optional[str] = None,
        video_gdrive_url: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update video status"""
        try:
            updates = {
                'video_status': status,
                'updated_at': datetime.utcnow().isoformat()
            }

            if video_gdrive_id:
                updates['video_gdrive_id'] = video_gdrive_id
            if video_gdrive_url:
                updates['video_gdrive_url'] = video_gdrive_url
            if error_message:
                updates['error_message'] = error_message
            if status == 'completed':
                updates['processing_completed_at'] = datetime.utcnow().isoformat()

            result = self.supabase.table('daily_uploads').update(updates).eq(
                'channel_id', channel_id
            ).eq('upload_date', upload_date).eq('video_number', video_number).execute()

            if result.data:
                return result.data[0]
            else:
                raise Exception("Upload entry not found")

        except Exception as e:
            logger.error(f"Error updating video status: {e}")
            raise

    async def get_upload_entry(
        self,
        channel_id: int,
        upload_date: str,
        video_number: int
    ) -> Optional[Dict[str, Any]]:
        """Get a specific upload entry"""
        try:
            result = self.supabase.table('daily_uploads').select('*').eq(
                'channel_id', channel_id
            ).eq('upload_date', upload_date).eq('video_number', video_number).execute()

            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(f"Error getting upload entry: {e}")
            return None

    async def get_date_uploads(self, upload_date: str) -> List[Dict[str, Any]]:
        """Get all uploads for a specific date"""
        try:
            result = self.supabase.table('daily_uploads').select(
                '*, channels(channel_name, channel_display_name)'
            ).eq('upload_date', upload_date).order('channel_id').order('video_number').execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(f"Error getting date uploads: {e}")
            return []

    async def get_incomplete_items(self, upload_date: str) -> List[Dict[str, Any]]:
        """Get incomplete items for a date"""
        try:
            result = self.supabase.table('daily_uploads').select(
                '*, channels(channel_name)'
            ).eq('upload_date', upload_date).or_(
                'script_status.eq.pending,thumbnail_status.eq.pending,video_status.neq.completed'
            ).execute()

            return result.data if result.data else []

        except Exception as e:
            logger.error(f"Error getting incomplete items: {e}")
            return []
