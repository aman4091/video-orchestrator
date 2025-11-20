"""
7-Day Schedule Management and Reminder System
Handles schedule tracking, reminders, and status calculations
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class ScheduleManager:
    """Manages 7-day upload schedules and tracking"""

    def __init__(self, supabase_client: SupabaseClient):
        self.supabase = supabase_client.supabase

    def get_date_range(self, days_ahead: int = 7) -> List[str]:
        """
        Get list of dates from today to N days ahead

        Args:
            days_ahead: Number of days to include

        Returns:
            List of date strings (YYYY-MM-DD)
        """
        today = date.today()
        return [(today + timedelta(days=i)).isoformat() for i in range(days_ahead)]

    async def initialize_date(self, target_date: str) -> bool:
        """
        Initialize schedule skeleton for a date
        Creates upload_schedule and daily_uploads entries

        Args:
            target_date: Date string (YYYY-MM-DD)

        Returns:
            True if successful
        """
        try:
            # Call Supabase function to create skeleton
            result = self.supabase.rpc(
                'create_daily_uploads_skeleton',
                {'target_date': target_date}
            ).execute()

            logger.info(f"Initialized schedule for {target_date}")
            return True

        except Exception as e:
            logger.error(f"Error initializing date {target_date}: {e}")
            return False

    async def initialize_7days(self) -> int:
        """
        Initialize next 7 days schedule

        Returns:
            Number of days initialized
        """
        try:
            result = self.supabase.rpc('create_next_7days_skeleton').execute()
            logger.info("Initialized 7-day schedule")
            return 7

        except Exception as e:
            logger.error(f"Error initializing 7 days: {e}")
            return 0

    async def get_day_status(self, target_date: str) -> Dict[str, Any]:
        """
        Get comprehensive status for a specific date

        Args:
            target_date: Date string

        Returns:
            Dict with status info
        """
        try:
            # Get upload_schedule
            schedule_result = self.supabase.table('upload_schedules').select('*').eq(
                'upload_date', target_date
            ).execute()

            if not schedule_result.data:
                # Create if doesn't exist
                await self.initialize_date(target_date)
                schedule_result = self.supabase.table('upload_schedules').select('*').eq(
                    'upload_date', target_date
                ).execute()

            schedule = schedule_result.data[0] if schedule_result.data else {}

            # Get daily_uploads breakdown
            uploads_result = self.supabase.table('daily_uploads').select(
                '*, channels(channel_name)'
            ).eq('upload_date', target_date).execute()

            uploads = uploads_result.data if uploads_result.data else []

            # Calculate stats
            total = len(uploads)
            scripts_ready = sum(1 for u in uploads if u['script_status'] in ['received', 'processed'])
            thumbnails_ready = sum(1 for u in uploads if u['thumbnail_status'] == 'received')
            videos_completed = sum(1 for u in uploads if u['video_status'] == 'completed')

            completion_pct = (videos_completed / total * 100) if total > 0 else 0

            # Determine status emoji
            if completion_pct == 100:
                status_emoji = "✅"
                status_text = "READY"
            elif completion_pct >= 80:
                status_emoji = "🟢"
                status_text = "ALMOST READY"
            elif completion_pct >= 50:
                status_emoji = "🟡"
                status_text = "IN PROGRESS"
            elif completion_pct > 0:
                status_emoji = "🔵"
                status_text = "STARTED"
            else:
                status_emoji = "⚪"
                status_text = "NOT STARTED"

            # Group by channel
            channels_status = {}
            for upload in uploads:
                ch_name = upload['channels']['channel_name']
                if ch_name not in channels_status:
                    channels_status[ch_name] = {
                        'total': 0,
                        'completed': 0,
                        'scripts': 0,
                        'thumbnails': 0,
                        'missing': []
                    }

                channels_status[ch_name]['total'] += 1
                if upload['video_status'] == 'completed':
                    channels_status[ch_name]['completed'] += 1
                if upload['script_status'] in ['received', 'processed']:
                    channels_status[ch_name]['scripts'] += 1
                if upload['thumbnail_status'] == 'received':
                    channels_status[ch_name]['thumbnails'] += 1

                # Track missing items
                video_num = upload['video_number']
                if upload['script_status'] == 'pending':
                    channels_status[ch_name]['missing'].append(f"V{video_num} script")
                if upload['thumbnail_status'] == 'pending':
                    channels_status[ch_name]['missing'].append(f"V{video_num} thumbnail")

            return {
                'date': target_date,
                'status_emoji': status_emoji,
                'status_text': status_text,
                'completion_percentage': round(completion_pct, 1),
                'total_videos': total,
                'videos_completed': videos_completed,
                'scripts_ready': scripts_ready,
                'thumbnails_ready': thumbnails_ready,
                'all_complete': schedule.get('all_complete', False),
                'channels': channels_status,
                'uploads': uploads,
                'schedule': schedule
            }

        except Exception as e:
            logger.error(f"Error getting day status for {target_date}: {e}")
            return {}

    async def get_week_overview(self, start_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get 7-day overview

        Args:
            start_date: Start date (defaults to today)

        Returns:
            Week overview dict
        """
        try:
            if not start_date:
                start_date = date.today().isoformat()

            dates = self.get_date_range(7)

            # Get status for each day
            days_status = []
            total_videos = 0
            total_completed = 0

            for d in dates:
                day_status = await self.get_day_status(d)
                if day_status:
                    days_status.append(day_status)
                    total_videos += day_status.get('total_videos', 0)
                    total_completed += day_status.get('videos_completed', 0)

            overall_pct = (total_completed / total_videos * 100) if total_videos > 0 else 0

            return {
                'start_date': dates[0],
                'end_date': dates[-1],
                'days': days_status,
                'total_videos': total_videos,
                'total_completed': total_completed,
                'completion_percentage': round(overall_pct, 1)
            }

        except Exception as e:
            logger.error(f"Error getting week overview: {e}")
            return {}

    async def get_incomplete_items_for_date(self, target_date: str) -> List[Dict[str, Any]]:
        """
        Get list of incomplete items for a specific date

        Args:
            target_date: Date string

        Returns:
            List of incomplete upload dicts with details
        """
        try:
            result = self.supabase.table('daily_uploads').select(
                '*, channels(channel_name)'
            ).eq('upload_date', target_date).execute()

            if not result.data:
                return []

            incomplete = []
            for upload in result.data:
                issues = []

                if upload['script_status'] == 'pending':
                    issues.append('script')
                if upload['thumbnail_status'] == 'pending':
                    issues.append('thumbnail')
                if upload['video_status'] != 'completed':
                    issues.append('video')

                if issues:
                    incomplete.append({
                        'channel_name': upload['channels']['channel_name'],
                        'video_number': upload['video_number'],
                        'missing': issues,
                        'upload': upload
                    })

            return incomplete

        except Exception as e:
            logger.error(f"Error getting incomplete items: {e}")
            return []

    def format_day_status(self, status: Dict[str, Any], include_details: bool = True) -> str:
        """
        Format day status for Telegram display

        Args:
            status: Status dict from get_day_status()
            include_details: Include channel breakdown

        Returns:
            Formatted string
        """
        if not status:
            return "No status available"

        date_str = status['date']
        emoji = status['status_emoji']
        text = status['status_text']
        pct = status['completion_percentage']
        completed = status['videos_completed']
        total = status['total_videos']

        # Header
        lines = [f"{emoji} {date_str} - {text} ({pct}%)"]
        lines.append(f"Videos: {completed}/{total}")

        if include_details and status.get('channels'):
            lines.append("\nChannel Breakdown:")
            for ch_name, ch_data in status['channels'].items():
                ch_completed = ch_data['completed']
                ch_total = ch_data['total']

                if ch_completed == ch_total:
                    ch_emoji = "✅"
                elif ch_completed > 0:
                    ch_emoji = "🟡"
                else:
                    ch_emoji = "⚪"

                lines.append(f"{ch_emoji} {ch_name}: {ch_completed}/{ch_total}")

                if ch_data['missing']:
                    missing_str = ", ".join(ch_data['missing'][:3])  # Show first 3
                    if len(ch_data['missing']) > 3:
                        missing_str += f" +{len(ch_data['missing']) - 3} more"
                    lines.append(f"   Missing: {missing_str}")

        return "\n".join(lines)

    def format_week_overview(self, overview: Dict[str, Any]) -> str:
        """
        Format week overview for Telegram

        Args:
            overview: Week overview dict

        Returns:
            Formatted string
        """
        if not overview or not overview.get('days'):
            return "No schedule data available"

        lines = ["📅 7-DAY SCHEDULE OVERVIEW\n"]

        # Get today's date
        today = date.today().isoformat()

        for day in overview['days']:
            date_str = day['date']
            emoji = day['status_emoji']
            pct = day['completion_percentage']
            completed = day['videos_completed']
            total = day['total_videos']

            # Add day name
            dt = datetime.fromisoformat(date_str)
            day_name = dt.strftime("%a")

            # Mark today/tomorrow
            if date_str == today:
                suffix = " [TODAY]"
            elif date_str == (date.today() + timedelta(days=1)).isoformat():
                suffix = " [TOMORROW]"
            else:
                suffix = ""

            lines.append(
                f"{day_name} {date_str}{suffix}: {emoji} {pct}% ({completed}/{total})"
            )

        # Overall stats
        total_pct = overview['completion_percentage']
        total_completed = overview['total_completed']
        total_videos = overview['total_videos']

        lines.append(f"\nOverall: {total_completed}/{total_videos} ({total_pct}%)")

        return "\n".join(lines)

    async def mark_complete(
        self,
        channel_name: str,
        upload_date: str,
        video_number: int
    ) -> bool:
        """
        Manually mark a video as complete

        Args:
            channel_name: Channel name
            upload_date: Date string
            video_number: Video number (1-4)

        Returns:
            True if successful
        """
        try:
            # Get channel ID
            ch_result = self.supabase.table('channels').select('id').eq(
                'channel_name', channel_name
            ).execute()

            if not ch_result.data:
                logger.error(f"Channel not found: {channel_name}")
                return False

            channel_id = ch_result.data[0]['id']

            # Update status
            result = self.supabase.table('daily_uploads').update({
                'script_status': 'processed',
                'thumbnail_status': 'received',
                'video_status': 'completed',
                'audio_status': 'completed',
                'processing_completed_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }).eq('channel_id', channel_id).eq('upload_date', upload_date).eq(
                'video_number', video_number
            ).execute()

            if result.data:
                logger.info(f"Marked complete: {channel_name}/{upload_date}/V{video_number}")
                return True
            return False

        except Exception as e:
            logger.error(f"Error marking complete: {e}")
            return False

    async def calculate_time_remaining(self, upload_date: str) -> Tuple[int, int, int]:
        """
        Calculate time remaining until upload deadline

        Args:
            upload_date: Date string

        Returns:
            Tuple of (days, hours, minutes) remaining
        """
        try:
            # Get deadline time from upload_schedule
            schedule_result = self.supabase.table('upload_schedules').select(
                'upload_deadline_time'
            ).eq('upload_date', upload_date).execute()

            deadline_time = "08:00:00"  # Default
            if schedule_result.data:
                deadline_time = schedule_result.data[0].get('upload_deadline_time', deadline_time)

            # Combine date and time
            deadline_dt = datetime.strptime(
                f"{upload_date} {deadline_time}",
                "%Y-%m-%d %H:%M:%S"
            )

            # Calculate difference
            now = datetime.now()
            diff = deadline_dt - now

            if diff.total_seconds() < 0:
                return (0, 0, 0)

            days = diff.days
            hours = diff.seconds // 3600
            minutes = (diff.seconds % 3600) // 60

            return (days, hours, minutes)

        except Exception as e:
            logger.error(f"Error calculating time remaining: {e}")
            return (0, 0, 0)


class ReminderManager:
    """Manages reminder logic and tracking"""

    def __init__(self, supabase_client: SupabaseClient):
        self.supabase = supabase_client.supabase
        self.schedule_mgr = ScheduleManager(supabase_client)

    async def should_send_tomorrow_reminder(self) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Check if tomorrow incomplete reminder should be sent

        Returns:
            Tuple of (should_send, incomplete_items)
        """
        try:
            tomorrow = (date.today() + timedelta(days=1)).isoformat()
            incomplete = await self.schedule_mgr.get_incomplete_items_for_date(tomorrow)

            if not incomplete:
                return (False, [])

            # Check last reminder time
            schedule_result = self.supabase.table('upload_schedules').select(
                'last_reminder_sent_at, reminder_type'
            ).eq('upload_date', tomorrow).execute()

            if schedule_result.data:
                last_sent = schedule_result.data[0].get('last_reminder_sent_at')
                if last_sent:
                    last_dt = datetime.fromisoformat(last_sent.replace('Z', '+00:00'))
                    now = datetime.utcnow()
                    minutes_since = (now - last_dt).total_seconds() / 60

                    # Send every 30 minutes
                    if minutes_since < 30:
                        return (False, incomplete)

            return (True, incomplete)

        except Exception as e:
            logger.error(f"Error checking tomorrow reminder: {e}")
            return (False, [])

    async def should_send_today_reminder(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if today upload ready reminder should be sent

        Returns:
            Tuple of (should_send, status_dict)
        """
        try:
            today = date.today().isoformat()
            status = await self.schedule_mgr.get_day_status(today)

            if not status or not status.get('all_complete'):
                return (False, {})

            # Check last reminder
            schedule_result = self.supabase.table('upload_schedules').select(
                'last_reminder_sent_at, reminder_type'
            ).eq('upload_date', today).execute()

            if schedule_result.data:
                last_sent = schedule_result.data[0].get('last_reminder_sent_at')
                reminder_type = schedule_result.data[0].get('reminder_type')

                if last_sent and reminder_type == 'ready':
                    last_dt = datetime.fromisoformat(last_sent.replace('Z', '+00:00'))
                    now = datetime.utcnow()
                    hours_since = (now - last_dt).total_seconds() / 3600

                    # Send every 3 hours
                    if hours_since < 3:
                        return (False, status)

            return (True, status)

        except Exception as e:
            logger.error(f"Error checking today reminder: {e}")
            return (False, {})

    async def log_reminder(
        self,
        reminder_type: str,
        upload_date: str,
        message_text: str,
        incomplete_count: int = 0,
        channels_mentioned: List[str] = None
    ) -> bool:
        """
        Log a sent reminder

        Args:
            reminder_type: Type of reminder
            upload_date: Date the reminder was about
            message_text: Message content
            incomplete_count: Number of incomplete items
            channels_mentioned: List of channel names mentioned

        Returns:
            True if logged successfully
        """
        try:
            # Insert into reminder_logs
            log_data = {
                'reminder_type': reminder_type,
                'upload_date': upload_date,
                'message_text': message_text,
                'incomplete_count': incomplete_count,
                'channels_mentioned': channels_mentioned or []
            }

            self.supabase.table('reminder_logs').insert(log_data).execute()

            # Update upload_schedules
            self.supabase.table('upload_schedules').update({
                'last_reminder_sent_at': datetime.utcnow().isoformat(),
                'reminder_type': reminder_type
            }).eq('upload_date', upload_date).execute()

            logger.info(f"Logged reminder: {reminder_type} for {upload_date}")
            return True

        except Exception as e:
            logger.error(f"Error logging reminder: {e}")
            return False

    def format_tomorrow_incomplete_reminder(
        self,
        incomplete: List[Dict[str, Any]],
        target_date: str
    ) -> str:
        """
        Format tomorrow incomplete reminder message

        Args:
            incomplete: List of incomplete items
            target_date: Tomorrow's date

        Returns:
            Formatted message
        """
        if not incomplete:
            return ""

        dt = datetime.fromisoformat(target_date)
        date_display = dt.strftime("%d-%b")

        lines = [f"⚠️ TOMORROW ({date_display}) UPLOAD - INCOMPLETE\n"]
        lines.append(f"Missing Items ({len(incomplete)} total):\n")

        # Group by channel
        by_channel = {}
        for item in incomplete:
            ch = item['channel_name']
            if ch not in by_channel:
                by_channel[ch] = []
            by_channel[ch].append(item)

        for ch_name, items in sorted(by_channel.items()):
            lines.append(f"• {ch_name}:")
            for item in items:
                v_num = item['video_number']
                missing = item['missing']

                script_status = "✅" if 'script' not in missing else "❌"
                thumb_status = "✅" if 'thumbnail' not in missing else "❌"

                lines.append(f"  Video {v_num}: Script {script_status} | Thumbnail {thumb_status}")

        # Add time remaining

        lines.append("\nUse /mark_complete to update status")

        return "\n".join(lines)

    def format_today_ready_reminder(self, status: Dict[str, Any]) -> str:
        """
        Format today upload ready reminder

        Args:
            status: Day status dict

        Returns:
            Formatted message
        """
        if not status:
            return ""

        date_str = status['date']
        dt = datetime.fromisoformat(date_str)
        date_display = dt.strftime("%d-%b")

        completed = status['videos_completed']
        total = status['total_videos']

        lines = [f"✅ TODAY ({date_display}) - READY FOR UPLOAD\n"]
        lines.append(f"All {total} videos completed!\n")

        # Add GDrive link if available
        if status.get('schedule', {}).get('gdrive_folder_link'):
            lines.append(f"📁 GDrive: {status['schedule']['gdrive_folder_link']}\n")

        lines.append("⏰ Upload by: 8:00 AM")
        lines.append(f"\nCompletion: {completed}/{total} ({status['completion_percentage']}%)")

        return "\n".join(lines)
