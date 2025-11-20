"""
Google Drive 7-Day Folder Management System
Auto-creates and maintains date/channel/video folder structure
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import date, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import pickle
from supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class GDriveFolderManager:
    """Manages Google Drive folder structure for 7-day planning"""

    def __init__(self, supabase_client: SupabaseClient, token_path: str = "token.pickle"):
        self.supabase = supabase_client.supabase
        self.token_path = token_path
        self.service = None
        self._initialize_service()

    def _initialize_service(self):
        """Initialize Google Drive API service"""
        try:
            if os.path.exists(self.token_path):
                with open(self.token_path, 'rb') as token:
                    creds = pickle.load(token)
                    self.service = build('drive', 'v3', credentials=creds)
                    logger.info("Google Drive service initialized")
            else:
                logger.error(f"Token file not found: {self.token_path}")
        except Exception as e:
            logger.error(f"Error initializing Drive service: {e}")

    def _create_folder(
        self,
        folder_name: str,
        parent_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Create a folder in Google Drive

        Args:
            folder_name: Name of folder
            parent_id: Parent folder ID (None for root)

        Returns:
            Folder ID or None
        """
        try:
            if not self.service:
                logger.error("Drive service not initialized")
                return None

            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }

            if parent_id:
                file_metadata['parents'] = [parent_id]

            folder = self.service.files().create(
                body=file_metadata,
                fields='id, name, webViewLink'
            ).execute()

            folder_id = folder.get('id')
            logger.info(f"Created folder: {folder_name} (ID: {folder_id})")

            return folder_id

        except Exception as e:
            logger.error(f"Error creating folder {folder_name}: {e}")
            return None

    def _folder_exists(self, folder_name: str, parent_id: Optional[str] = None) -> Optional[str]:
        """
        Check if folder exists

        Args:
            folder_name: Folder name to check
            parent_id: Parent folder ID

        Returns:
            Folder ID if exists, None otherwise
        """
        try:
            if not self.service:
                return None

            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"

            if parent_id:
                query += f" and '{parent_id}' in parents"

            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()

            files = results.get('files', [])

            if files:
                return files[0]['id']
            return None

        except Exception as e:
            logger.error(f"Error checking folder existence: {e}")
            return None

    def _get_or_create_folder(
        self,
        folder_name: str,
        parent_id: Optional[str] = None
    ) -> Optional[str]:
        """Get existing folder or create new one"""
        folder_id = self._folder_exists(folder_name, parent_id)
        if folder_id:
            return folder_id
        return self._create_folder(folder_name, parent_id)

    async def get_base_folder_id(self) -> Optional[str]:
        """
        Get base folder ID from system_config

        Returns:
            Base folder ID or None
        """
        try:
            result = self.supabase.table('system_config').select(
                'gdrive_base_folder_id'
            ).eq('id', 1).execute()

            if result.data and result.data[0].get('gdrive_base_folder_id'):
                return result.data[0]['gdrive_base_folder_id']

            logger.warning("Base folder ID not set in system_config")
            return None

        except Exception as e:
            logger.error(f"Error getting base folder ID: {e}")
            return None

    async def set_base_folder_id(self, folder_id: str) -> bool:
        """
        Set base folder ID in system_config

        Args:
            folder_id: Google Drive folder ID

        Returns:
            True if successful
        """
        try:
            self.supabase.table('system_config').update({
                'gdrive_base_folder_id': folder_id
            }).eq('id', 1).execute()

            logger.info(f"Set base folder ID: {folder_id}")
            return True

        except Exception as e:
            logger.error(f"Error setting base folder ID: {e}")
            return False

    async def create_date_structure(
        self,
        target_date: str,
        channel_names: List[str]
    ) -> Dict[str, Any]:
        """
        Create complete folder structure for a date

        Structure:
        Videos/
        └── 2025-01-21/
            ├── GYH/
            │   ├── Video_1/
            │   ├── Video_2/
            │   ├── Video_3/
            │   └── Video_4/
            ├── BI/
            └── ... (all channels)

        Args:
            target_date: Date string (YYYY-MM-DD)
            channel_names: List of channel names

        Returns:
            Dict with created folder IDs
        """
        try:
            base_folder_id = await self.get_base_folder_id()
            if not base_folder_id:
                logger.error("Base folder not configured")
                return {}

            # Create date folder
            date_folder_id = self._get_or_create_folder(target_date, base_folder_id)
            if not date_folder_id:
                logger.error(f"Failed to create date folder: {target_date}")
                return {}

            # Store in database
            await self._store_folder_info(
                folder_date=target_date,
                folder_path=f"Videos/{target_date}",
                folder_id=date_folder_id,
                parent_folder_id=base_folder_id
            )

            created_folders = {
                'date_folder_id': date_folder_id,
                'channels': {}
            }

            # Create channel folders
            for channel_name in channel_names:
                channel_folder_id = self._get_or_create_folder(
                    channel_name,
                    date_folder_id
                )

                if not channel_folder_id:
                    logger.warning(f"Failed to create channel folder: {channel_name}")
                    continue

                # Store channel folder
                await self._store_folder_info(
                    folder_date=target_date,
                    folder_path=f"Videos/{target_date}/{channel_name}",
                    folder_id=channel_folder_id,
                    parent_folder_id=date_folder_id,
                    channel_name=channel_name
                )

                # Create video folders (1-4)
                video_folders = {}
                for video_num in range(1, 5):
                    video_folder_name = f"Video_{video_num}"
                    video_folder_id = self._get_or_create_folder(
                        video_folder_name,
                        channel_folder_id
                    )

                    if video_folder_id:
                        video_folders[video_num] = video_folder_id

                        # Store video folder
                        await self._store_folder_info(
                            folder_date=target_date,
                            folder_path=f"Videos/{target_date}/{channel_name}/{video_folder_name}",
                            folder_id=video_folder_id,
                            parent_folder_id=channel_folder_id,
                            channel_name=channel_name,
                            video_number=video_num
                        )

                created_folders['channels'][channel_name] = {
                    'channel_folder_id': channel_folder_id,
                    'video_folders': video_folders
                }

            logger.info(f"Created complete structure for {target_date}")
            return created_folders

        except Exception as e:
            logger.error(f"Error creating date structure: {e}")
            return {}

    async def _store_folder_info(
        self,
        folder_date: str,
        folder_path: str,
        folder_id: str,
        parent_folder_id: Optional[str] = None,
        channel_name: Optional[str] = None,
        video_number: Optional[int] = None
    ) -> bool:
        """Store folder info in database"""
        try:
            data = {
                'folder_date': folder_date,
                'folder_path': folder_path,
                'folder_id': folder_id,
                'parent_folder_id': parent_folder_id,
                'channel_name': channel_name,
                'video_number': video_number,
                'is_active': True
            }

            self.supabase.table('gdrive_folders').upsert(
                data,
                on_conflict='folder_id'
            ).execute()

            return True

        except Exception as e:
            logger.error(f"Error storing folder info: {e}")
            return False

    async def get_video_folder_id(
        self,
        target_date: str,
        channel_name: str,
        video_number: int
    ) -> Optional[str]:
        """
        Get video folder ID from database

        Args:
            target_date: Date string
            channel_name: Channel name
            video_number: Video number (1-4)

        Returns:
            Folder ID or None
        """
        try:
            result = self.supabase.table('gdrive_folders').select('folder_id').eq(
                'folder_date', target_date
            ).eq('channel_name', channel_name).eq(
                'video_number', video_number
            ).eq('is_active', True).execute()

            if result.data:
                return result.data[0]['folder_id']
            return None

        except Exception as e:
            logger.error(f"Error getting video folder ID: {e}")
            return None

    async def create_7day_structure(self, channel_names: List[str]) -> int:
        """
        Create folder structure for next 7 days

        Args:
            channel_names: List of channel names

        Returns:
            Number of dates processed
        """
        count = 0
        for i in range(7):
            target_date = (date.today() + timedelta(days=i)).isoformat()
            result = await self.create_date_structure(target_date, channel_names)

            if result:
                count += 1

        logger.info(f"Created 7-day structure: {count} dates")
        return count

    def upload_file_to_folder(
        self,
        file_path: str,
        folder_id: str,
        file_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Upload a file to specific folder

        Args:
            file_path: Local file path
            folder_id: Destination folder ID
            file_name: Optional custom name

        Returns:
            File info dict or None
        """
        try:
            if not self.service:
                logger.error("Drive service not initialized")
                return None

            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return None

            file_metadata = {
                'name': file_name or os.path.basename(file_path),
                'parents': [folder_id]
            }

            media = MediaFileUpload(file_path, resumable=True)

            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink, webContentLink'
            ).execute()

            logger.info(f"Uploaded file: {file_metadata['name']} to folder {folder_id}")

            return {
                'file_id': file.get('id'),
                'file_name': file.get('name'),
                'web_view_link': file.get('webViewLink'),
                'web_content_link': file.get('webContentLink')
            }

        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            return None

    async def upload_script(
        self,
        script_text: str,
        target_date: str,
        channel_name: str,
        video_number: int
    ) -> Optional[str]:
        """
        Upload script to video folder

        Args:
            script_text: Script content
            target_date: Date string
            channel_name: Channel name
            video_number: Video number

        Returns:
            File ID or None
        """
        try:
            # Get video folder ID
            folder_id = await self.get_video_folder_id(
                target_date,
                channel_name,
                video_number
            )

            if not folder_id:
                logger.error(f"Video folder not found: {target_date}/{channel_name}/V{video_number}")
                return None

            # Create temp file
            temp_file = f"temp_script_{channel_name}_V{video_number}.txt"
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(script_text)

            # Upload
            result = self.upload_file_to_folder(
                temp_file,
                folder_id,
                'script.txt'
            )

            # Cleanup
            if os.path.exists(temp_file):
                os.remove(temp_file)

            if result:
                return result['file_id']
            return None

        except Exception as e:
            logger.error(f"Error uploading script: {e}")
            return None

    async def upload_thumbnail(
        self,
        thumbnail_path: str,
        target_date: str,
        channel_name: str,
        video_number: int
    ) -> Optional[Dict[str, str]]:
        """
        Upload thumbnail to video folder

        Args:
            thumbnail_path: Local thumbnail file path
            target_date: Date string
            channel_name: Channel name
            video_number: Video number

        Returns:
            Dict with file_id and url, or None
        """
        try:
            folder_id = await self.get_video_folder_id(
                target_date,
                channel_name,
                video_number
            )

            if not folder_id:
                logger.error(f"Video folder not found")
                return None

            # Get file extension
            _, ext = os.path.splitext(thumbnail_path)
            file_name = f"thumbnail{ext}"

            result = self.upload_file_to_folder(
                thumbnail_path,
                folder_id,
                file_name
            )

            if result:
                return {
                    'file_id': result['file_id'],
                    'url': result['web_view_link']
                }
            return None

        except Exception as e:
            logger.error(f"Error uploading thumbnail: {e}")
            return None

    async def get_folder_link(
        self,
        target_date: str,
        channel_name: Optional[str] = None
    ) -> Optional[str]:
        """
        Get web link to folder

        Args:
            target_date: Date string
            channel_name: Optional channel name (None for date folder)

        Returns:
            Web view link or None
        """
        try:
            query = self.supabase.table('gdrive_folders').select('folder_id').eq(
                'folder_date', target_date
            ).eq('is_active', True)

            if channel_name:
                query = query.eq('channel_name', channel_name).is_('video_number', 'null')
            else:
                query = query.is_('channel_name', 'null')

            result = query.execute()

            if not result.data:
                return None

            folder_id = result.data[0]['folder_id']

            # Get link from Drive API
            if self.service:
                file = self.service.files().get(
                    fileId=folder_id,
                    fields='webViewLink'
                ).execute()

                return file.get('webViewLink')

            return None

        except Exception as e:
            logger.error(f"Error getting folder link: {e}")
            return None

    async def archive_old_folders(self, days_old: int = 1) -> int:
        """
        Archive folders older than specified days

        Args:
            days_old: Days old threshold

        Returns:
            Number of folders archived
        """
        try:
            cutoff_date = (date.today() - timedelta(days=days_old)).isoformat()

            result = self.supabase.table('gdrive_folders').update({
                'is_active': False,
                'archived_at': datetime.utcnow().isoformat()
            }).lt('folder_date', cutoff_date).eq('is_active', True).execute()

            count = len(result.data) if result.data else 0
            logger.info(f"Archived {count} old folders")

            return count

        except Exception as e:
            logger.error(f"Error archiving folders: {e}")
            return 0
