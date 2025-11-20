"""
Orchestrator Bot for Railway Deployment
SIRF tracking, reminders, aur scheduling ke liye
Audio/Video generation NAHI karega
"""

import logging
import os
import asyncio
from datetime import datetime
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import pytz

# Import our managers
from supabase_client import SupabaseClient
from channel_manager import ChannelManager, DailyUploadManager
from schedule_manager import ScheduleManager, ReminderManager
from gdrive_folder_manager import GDriveFolderManager
from inline_selection_handler import InlineSelectionHandler, detect_content_type

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class OrchestratorBot:
    """
    Lightweight orchestrator bot
    NO audio/video generation
    SIRF tracking, reminders, commands
    """

    def __init__(self):
        # Environment variables
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN not set!")

        # Active chat IDs (update with your actual IDs)
        self.active_chat_ids = [
            int(os.getenv('CHAT_ID_1', '0')),
            # Add more if needed
        ]
        self.active_chat_ids = [cid for cid in self.active_chat_ids if cid != 0]

        # Supabase client
        self.supabase_client = SupabaseClient()

        # Managers
        self.channel_mgr = ChannelManager(self.supabase_client)
        self.upload_mgr = DailyUploadManager(self.supabase_client)
        self.schedule_mgr = ScheduleManager(self.supabase_client)
        self.reminder_mgr = ReminderManager(self.supabase_client)
        self.gdrive_mgr = GDriveFolderManager(self.supabase_client)
        self.inline_handler = InlineSelectionHandler(self.supabase_client)

        # Scheduler for reminders
        self.scheduler = AsyncIOScheduler(timezone=pytz.timezone('Asia/Kolkata'))

        # Application
        self.application = Application.builder().token(self.bot_token).build()

        logger.info("OrchestratorBot initialized")

    def setup_handlers(self):
        """Register all command handlers"""

        # Setup commands
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("setup_7day", self.cmd_setup_7day))

        # Channel management
        self.application.add_handler(CommandHandler("add_channel", self.cmd_add_channel))
        self.application.add_handler(CommandHandler("list_channels", self.cmd_list_channels))

        # Status commands
        self.application.add_handler(CommandHandler("week_status", self.cmd_week_status))
        self.application.add_handler(CommandHandler("today_status", self.cmd_today_status))
        self.application.add_handler(CommandHandler("tomorrow_status", self.cmd_tomorrow_status))
        self.application.add_handler(CommandHandler("day_status", self.cmd_day_status))

        # Manual updates
        self.application.add_handler(CommandHandler("mark_complete", self.cmd_mark_complete))

        # Message handlers (script/thumbnail detection)
        self.application.add_handler(
            MessageHandler(
                filters.TEXT & ~filters.COMMAND,
                self.handle_message
            )
        )

        self.application.add_handler(
            MessageHandler(
                filters.PHOTO | filters.Document.IMAGE,
                self.handle_message
            )
        )

        # Callback query handler for inline buttons
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))

        logger.info("All handlers registered")

    def setup_scheduler(self):
        """Setup APScheduler for reminders"""
        try:
            # Tomorrow incomplete (every 30 min)
            self.scheduler.add_job(
                self.job_tomorrow_incomplete,
                trigger=IntervalTrigger(minutes=30),
                id='tomorrow_incomplete',
                replace_existing=True
            )

            # Today ready (every 3 hours)
            self.scheduler.add_job(
                self.job_today_ready,
                trigger=IntervalTrigger(hours=3),
                id='today_ready',
                replace_existing=True
            )

            # Morning checklist (6 AM)
            self.scheduler.add_job(
                self.job_morning_checklist,
                trigger=CronTrigger(hour=6, minute=0),
                id='morning_checklist',
                replace_existing=True
            )

            # Daily overview (9 AM)
            self.scheduler.add_job(
                self.job_daily_overview,
                trigger=CronTrigger(hour=9, minute=0),
                id='daily_overview',
                replace_existing=True
            )

            # Don't start here - will be started by Application's post_init
            logger.info("Scheduler jobs configured (4 jobs)")

        except Exception as e:
            logger.error(f"Scheduler setup error: {e}")

    # ================================================================
    # COMMAND HANDLERS
    # ================================================================

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start command"""
        await update.message.reply_text(
            "🤖 Orchestrator Bot Active!\n\n"
            "Commands:\n"
            "/setup_7day - Initialize system\n"
            "/week_status - 7-day overview\n"
            "/today_status - Today's status\n"
            "/tomorrow_status - Tomorrow's status\n"
            "/list_channels - Show channels\n\n"
            "Send scripts or thumbnails to organize!"
        )

    async def cmd_setup_7day(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Setup 7-day system"""
        try:
            await update.message.reply_text("🔧 Setting up 7-day system...")

            # Initialize 7 days
            days = await self.schedule_mgr.initialize_7days()

            # Get channels
            channels = await self.channel_mgr.list_channels(active_only=True)
            channel_names = [ch['channel_name'] for ch in channels]

            # Create GDrive folders
            folders = await self.gdrive_mgr.create_7day_structure(channel_names)

            await update.message.reply_text(
                f"✅ 7-Day System Setup Complete!\n\n"
                f"• Database: {days} days initialized\n"
                f"• Channels: {len(channels)} active\n"
                f"• GDrive: {folders} date folders created\n\n"
                f"Channels: {', '.join(channel_names)}\n\n"
                f"Ready to use!"
            )

        except Exception as e:
            logger.error(f"Setup error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def cmd_add_channel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add new channel"""
        try:
            if len(context.args) < 1:
                await update.message.reply_text(
                    "Usage: /add_channel <name> [display_name]"
                )
                return

            channel_name = context.args[0]
            display_name = " ".join(context.args[1:]) if len(context.args) > 1 else channel_name

            result = await self.channel_mgr.add_channel(
                channel_name=channel_name,
                display_name=display_name
            )

            if result:
                await update.message.reply_text(f"✅ Channel added: {channel_name}")
            else:
                await update.message.reply_text("❌ Failed to add channel")

        except Exception as e:
            logger.error(f"Add channel error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def cmd_list_channels(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all channels"""
        try:
            channels = await self.channel_mgr.list_channels(active_only=False)
            formatted = self.channel_mgr.format_channel_list(channels)
            await update.message.reply_text(formatted)

        except Exception as e:
            logger.error(f"List channels error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def cmd_week_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show 7-day overview"""
        try:
            await update.message.reply_text("📊 Fetching...")

            overview = await self.schedule_mgr.get_week_overview()

            if not overview:
                await update.message.reply_text("No data available.")
                return

            formatted = self.schedule_mgr.format_week_overview(overview)
            await update.message.reply_text(formatted)

        except Exception as e:
            logger.error(f"Week status error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def cmd_today_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show today's status"""
        try:
            from datetime import date
            today = date.today().isoformat()

            status = await self.schedule_mgr.get_day_status(today)

            if not status:
                await update.message.reply_text("No data for today.")
                return

            formatted = self.schedule_mgr.format_day_status(status, include_details=True)

            # Add GDrive link
            gdrive_link = await self.gdrive_mgr.get_folder_link(today)
            if gdrive_link:
                formatted += f"\n\n📁 GDrive: {gdrive_link}"

            await update.message.reply_text(formatted)

        except Exception as e:
            logger.error(f"Today status error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def cmd_tomorrow_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show tomorrow's status"""
        try:
            from datetime import date, timedelta
            tomorrow = (date.today() + timedelta(days=1)).isoformat()

            status = await self.schedule_mgr.get_day_status(tomorrow)

            if not status:
                await update.message.reply_text("No data for tomorrow.")
                return

            formatted = self.schedule_mgr.format_day_status(status, include_details=True)

            # Add time remaining
            days, hours, mins = await self.schedule_mgr.calculate_time_remaining(tomorrow)
            formatted += f"\n\n⏰ Time until upload: {days}d {hours}h {mins}m"

            await update.message.reply_text(formatted)

        except Exception as e:
            logger.error(f"Tomorrow status error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def cmd_day_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show specific date status"""
        try:
            if len(context.args) < 1:
                await update.message.reply_text("Usage: /day_status YYYY-MM-DD")
                return

            target_date = context.args[0]

            status = await self.schedule_mgr.get_day_status(target_date)

            if not status:
                await update.message.reply_text(f"No data for {target_date}")
                return

            formatted = self.schedule_mgr.format_day_status(status, include_details=True)
            await update.message.reply_text(formatted)

        except Exception as e:
            logger.error(f"Day status error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    async def cmd_mark_complete(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Mark video as complete manually"""
        try:
            if len(context.args) < 3:
                await update.message.reply_text(
                    "Usage: /mark_complete <channel> <date> <video_num>"
                )
                return

            channel_name = context.args[0]
            target_date = context.args[1]
            video_num = int(context.args[2])

            success = await self.schedule_mgr.mark_complete(
                channel_name,
                target_date,
                video_num
            )

            if success:
                await update.message.reply_text(
                    f"✅ Marked complete:\n{channel_name} / {target_date} / Video {video_num}"
                )
            else:
                await update.message.reply_text("❌ Failed")

        except Exception as e:
            logger.error(f"Mark complete error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)}")

    # ================================================================
    # MESSAGE HANDLERS
    # ================================================================

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle script or thumbnail messages"""
        try:
            content_type = detect_content_type(update.message)

            if not content_type:
                return

            if content_type == 'script':
                script_text = update.message.text
                await self.inline_handler.start_selection(
                    update,
                    context,
                    'script',
                    script_text
                )

            elif content_type == 'thumbnail':
                if update.message.photo:
                    photo = update.message.photo[-1]
                    file_id = photo.file_id
                elif update.message.document:
                    file_id = update.message.document.file_id
                else:
                    return

                content_data = {
                    'file_id': file_id,
                    'url': None
                }

                await self.inline_handler.start_selection(
                    update,
                    context,
                    'thumbnail',
                    content_data
                )

        except Exception as e:
            logger.error(f"Handle message error: {e}")

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle inline button callbacks"""
        try:
            await self.inline_handler.handle_callback(update, context)
        except Exception as e:
            logger.error(f"Callback error: {e}")

    # ================================================================
    # SCHEDULED JOBS
    # ================================================================

    async def job_tomorrow_incomplete(self):
        """Check tomorrow's incomplete items (every 30 min)"""
        try:
            should_send, incomplete = await self.reminder_mgr.should_send_tomorrow_reminder()

            if not should_send or not incomplete:
                return

            from datetime import date, timedelta
            tomorrow = (date.today() + timedelta(days=1)).isoformat()

            message = self.reminder_mgr.format_tomorrow_incomplete_reminder(incomplete, tomorrow)

            for chat_id in self.active_chat_ids:
                await self.application.bot.send_message(chat_id=chat_id, text=message)

            # Log reminder
            channels_mentioned = list(set([item['channel_name'] for item in incomplete]))
            await self.reminder_mgr.log_reminder(
                'tomorrow_incomplete',
                tomorrow,
                message,
                len(incomplete),
                channels_mentioned
            )

        except Exception as e:
            logger.error(f"Tomorrow incomplete job error: {e}")

    async def job_today_ready(self):
        """Check if today ready (every 3 hours)"""
        try:
            should_send, status = await self.reminder_mgr.should_send_today_reminder()

            if not should_send or not status:
                return

            from datetime import date
            today = date.today().isoformat()

            message = self.reminder_mgr.format_today_ready_reminder(status)

            for chat_id in self.active_chat_ids:
                await self.application.bot.send_message(chat_id=chat_id, text=message)

            await self.reminder_mgr.log_reminder('today_ready', today, message, 0, [])

        except Exception as e:
            logger.error(f"Today ready job error: {e}")

    async def job_morning_checklist(self):
        """Morning 6 AM checklist"""
        try:
            from datetime import date
            today = date.today().isoformat()

            status = await self.schedule_mgr.get_day_status(today)

            if not status:
                return

            message_lines = ["🔍 MORNING CHECKLIST\n"]
            message_lines.append(f"Upload Date: {today}\n")

            for ch_name, ch_data in status['channels'].items():
                completed = ch_data['completed']
                total = ch_data['total']
                emoji = "✅" if completed == total else "⚠️"
                message_lines.append(f"{emoji} {ch_name}: {completed}/{total}")

            message_lines.append(f"\nOverall: {status['videos_completed']}/{status['total_videos']}")
            message_lines.append(f"Status: {status['status_text']}")
            message_lines.append("\n⏰ Upload Deadline: 8:00 AM")

            message = "\n".join(message_lines)

            for chat_id in self.active_chat_ids:
                await self.application.bot.send_message(chat_id=chat_id, text=message)

        except Exception as e:
            logger.error(f"Morning checklist job error: {e}")

    async def job_daily_overview(self):
        """Daily 9 AM overview"""
        try:
            overview = await self.schedule_mgr.get_week_overview()

            if not overview:
                return

            message = self.schedule_mgr.format_week_overview(overview)
            message = "🌅 GOOD MORNING - WEEKLY OVERVIEW\n\n" + message

            for chat_id in self.active_chat_ids:
                await self.application.bot.send_message(chat_id=chat_id, text=message)

        except Exception as e:
            logger.error(f"Daily overview job error: {e}")

    # ================================================================
    # RUN
    # ================================================================

    def run(self):
        """Start the bot"""
        try:
            logger.info("Starting Orchestrator Bot...")

            # Setup handlers
            self.setup_handlers()

            # Setup scheduler
            self.setup_scheduler()

            # Add post_init callback to start scheduler
            async def post_init(app):
                self.scheduler.start()
                logger.info("Scheduler started with 4 jobs")

            self.application.post_init = post_init

            # Start polling
            logger.info("Bot polling started")
            self.application.run_polling(drop_pending_updates=True)

        except Exception as e:
            logger.error(f"Run error: {e}")
            raise


if __name__ == "__main__":
    bot = OrchestratorBot()
    bot.run()
