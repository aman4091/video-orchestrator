"""
Inline Button Selection System for Multi-Date Channel Assignment
Handles 3-step selection: Date → Channel → Video Number
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import date, timedelta, datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import CallbackContext
from channel_manager import ChannelManager, DailyUploadManager
from supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


class InlineSelectionHandler:
    """Handles inline button selection workflow"""

    def __init__(self, supabase_client: SupabaseClient):
        self.supabase_client = supabase_client
        self.channel_mgr = ChannelManager(supabase_client)
        self.upload_mgr = DailyUploadManager(supabase_client)

        # Store pending selections in user_data
        # Format: {user_id: {content_type, content_data, step, selections}}

    def get_date_buttons(self) -> InlineKeyboardMarkup:
        """
        Create date selection buttons (7 days)

        Returns:
            InlineKeyboardMarkup with date buttons
        """
        today = date.today()
        buttons = []

        for i in range(7):
            target_date = today + timedelta(days=i)
            date_str = target_date.isoformat()

            # Format display
            if i == 0:
                label = f"Today ({target_date.strftime('%d-%b')})"
            elif i == 1:
                label = f"Tomorrow ({target_date.strftime('%d-%b')})"
            else:
                label = target_date.strftime("%d-%b (%a)")

            button = InlineKeyboardButton(
                label,
                callback_data=f"date:{date_str}"
            )
            buttons.append([button])

        # Add cancel button
        buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])

        return InlineKeyboardMarkup(buttons)

    async def get_channel_buttons(self) -> InlineKeyboardMarkup:
        """
        Create channel selection buttons

        Returns:
            InlineKeyboardMarkup with channel buttons
        """
        channels = await self.channel_mgr.list_channels(active_only=True)

        buttons = []
        row = []

        for i, channel in enumerate(channels):
            ch_name = channel['channel_name']
            button = InlineKeyboardButton(
                ch_name,
                callback_data=f"channel:{ch_name}"
            )
            row.append(button)

            # 3 buttons per row
            if len(row) == 3 or i == len(channels) - 1:
                buttons.append(row)
                row = []

        # Add back and cancel buttons
        buttons.append([
            InlineKeyboardButton("⬅️ Back", callback_data="back_to_date"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel")
        ])

        return InlineKeyboardMarkup(buttons)

    def get_video_number_buttons(self) -> InlineKeyboardMarkup:
        """
        Create video number selection buttons (1-4)

        Returns:
            InlineKeyboardMarkup with video number buttons
        """
        buttons = [
            [
                InlineKeyboardButton("Video 1", callback_data="video:1"),
                InlineKeyboardButton("Video 2", callback_data="video:2")
            ],
            [
                InlineKeyboardButton("Video 3", callback_data="video:3"),
                InlineKeyboardButton("Video 4", callback_data="video:4")
            ],
            [
                InlineKeyboardButton("⬅️ Back", callback_data="back_to_channel"),
                InlineKeyboardButton("❌ Cancel", callback_data="cancel")
            ]
        ]

        return InlineKeyboardMarkup(buttons)

    async def start_selection(
        self,
        update: Update,
        context: CallbackContext,
        content_type: str,
        content_data: Any
    ) -> bool:
        """
        Start the selection workflow

        Args:
            update: Telegram update
            context: Callback context
            content_type: 'script' or 'thumbnail'
            content_data: Script text or file info

        Returns:
            True if started successfully
        """
        try:
            user_id = update.effective_user.id

            # Store selection state
            if 'selections' not in context.user_data:
                context.user_data['selections'] = {}

            context.user_data['selections'][user_id] = {
                'content_type': content_type,
                'content_data': content_data,
                'step': 'date',
                'selected_date': None,
                'selected_channel': None,
                'selected_video': None,
                'message_id': None
            }

            # Send date selection
            keyboard = self.get_date_buttons()

            if content_type == 'script':
                text = "📅 Select upload date for this script:"
            else:
                text = "📅 Select upload date for this thumbnail:"

            message = await update.message.reply_text(
                text,
                reply_markup=keyboard
            )

            context.user_data['selections'][user_id]['message_id'] = message.message_id

            return True

        except Exception as e:
            logger.error(f"Error starting selection: {e}")
            return False

    async def handle_callback(
        self,
        update: Update,
        context: CallbackContext
    ) -> bool:
        """
        Handle callback query from inline buttons

        Args:
            update: Telegram update with callback query
            context: Callback context

        Returns:
            True if handled successfully
        """
        try:
            query = update.callback_query
            await query.answer()

            user_id = update.effective_user.id
            data = query.data

            # Get selection state
            if 'selections' not in context.user_data:
                context.user_data['selections'] = {}

            if user_id not in context.user_data['selections']:
                await query.edit_message_text("❌ Selection expired. Please try again.")
                return False

            state = context.user_data['selections'][user_id]

            # Handle cancel
            if data == "cancel":
                await query.edit_message_text("❌ Selection cancelled.")
                del context.user_data['selections'][user_id]
                return True

            # Handle back buttons
            if data == "back_to_date":
                state['step'] = 'date'
                state['selected_channel'] = None
                keyboard = self.get_date_buttons()
                await query.edit_message_text(
                    "📅 Select upload date:",
                    reply_markup=keyboard
                )
                return True

            if data == "back_to_channel":
                state['step'] = 'channel'
                state['selected_video'] = None
                keyboard = await self.get_channel_buttons()
                await query.edit_message_text(
                    f"📺 Select channel for {state['selected_date']}:",
                    reply_markup=keyboard
                )
                return True

            # Handle selections
            if data.startswith("date:"):
                selected_date = data.split(":")[1]
                state['selected_date'] = selected_date
                state['step'] = 'channel'

                keyboard = await self.get_channel_buttons()
                date_display = datetime.fromisoformat(selected_date).strftime("%d-%b")

                await query.edit_message_text(
                    f"📺 Select channel for {date_display}:",
                    reply_markup=keyboard
                )
                return True

            if data.startswith("channel:"):
                selected_channel = data.split(":")[1]
                state['selected_channel'] = selected_channel
                state['step'] = 'video'

                keyboard = self.get_video_number_buttons()

                await query.edit_message_text(
                    f"🎬 Select video number for {selected_channel}:",
                    reply_markup=keyboard
                )
                return True

            if data.startswith("video:"):
                selected_video = int(data.split(":")[1])
                state['selected_video'] = selected_video

                # Complete selection - process the content
                success = await self._process_selection(state, context)

                if success:
                    date_display = datetime.fromisoformat(state['selected_date']).strftime("%d-%b")
                    await query.edit_message_text(
                        f"✅ Saved for {date_display} / {state['selected_channel']} / Video {selected_video}"
                    )
                else:
                    await query.edit_message_text("❌ Error saving. Please try again.")

                # Cleanup
                del context.user_data['selections'][user_id]
                return True

            return False

        except Exception as e:
            logger.error(f"Error handling callback: {e}")
            return False

    async def _process_selection(
        self,
        state: Dict[str, Any],
        context: CallbackContext
    ) -> bool:
        """
        Process completed selection

        Args:
            state: Selection state dict
            context: Callback context

        Returns:
            True if processed successfully
        """
        try:
            content_type = state['content_type']
            content_data = state['content_data']
            selected_date = state['selected_date']
            selected_channel = state['selected_channel']
            selected_video = state['selected_video']

            # Get channel ID
            channel = await self.channel_mgr.get_channel(selected_channel)
            if not channel:
                logger.error(f"Channel not found: {selected_channel}")
                return False

            channel_id = channel['id']

            # Ensure upload entry exists
            await self.upload_mgr.create_upload_entry(
                channel_id,
                selected_date,
                selected_video
            )

            if content_type == 'script':
                # Save script
                result = await self.upload_mgr.update_script(
                    channel_id,
                    selected_date,
                    selected_video,
                    content_data
                )

                logger.info(f"Script saved: {selected_date}/{selected_channel}/V{selected_video}")
                return bool(result)

            elif content_type == 'thumbnail':
                # content_data should have file_id and url
                result = await self.upload_mgr.update_thumbnail(
                    channel_id,
                    selected_date,
                    selected_video,
                    content_data['file_id'],
                    content_data['url']
                )

                logger.info(f"Thumbnail saved: {selected_date}/{selected_channel}/V{selected_video}")
                return bool(result)

            return False

        except Exception as e:
            logger.error(f"Error processing selection: {e}")
            return False

    async def handle_bulk_selection(
        self,
        update: Update,
        context: CallbackContext,
        content_type: str,
        items: List[Any],
        auto_assign: bool = False
    ) -> bool:
        """
        Handle multiple items at once

        Args:
            update: Telegram update
            context: Callback context
            content_type: 'script' or 'thumbnail'
            items: List of content items
            auto_assign: If True, auto-assign to Video 1-4

        Returns:
            True if successful
        """
        try:
            # For bulk, show date and channel selection
            # Then auto-assign to Video 1, 2, 3, 4

            user_id = update.effective_user.id

            # Store bulk state
            if 'bulk_selections' not in context.user_data:
                context.user_data['bulk_selections'] = {}

            context.user_data['bulk_selections'][user_id] = {
                'content_type': content_type,
                'items': items,
                'step': 'date',
                'selected_date': None,
                'selected_channel': None
            }

            # Send date selection
            keyboard = self.get_date_buttons()
            text = f"📅 Select date for {len(items)} {content_type}s:"

            message = await update.message.reply_text(text, reply_markup=keyboard)

            return True

        except Exception as e:
            logger.error(f"Error handling bulk selection: {e}")
            return False

    async def handle_bulk_callback(
        self,
        update: Update,
        context: CallbackContext
    ) -> bool:
        """Handle bulk selection callbacks"""
        try:
            query = update.callback_query
            await query.answer()

            user_id = update.effective_user.id

            if 'bulk_selections' not in context.user_data:
                return False

            if user_id not in context.user_data['bulk_selections']:
                return False

            state = context.user_data['bulk_selections'][user_id]
            data = query.data

            if data.startswith("date:"):
                selected_date = data.split(":")[1]
                state['selected_date'] = selected_date
                state['step'] = 'channel'

                keyboard = await self.get_channel_buttons()
                await query.edit_message_text(
                    f"📺 Select channel for bulk upload:",
                    reply_markup=keyboard
                )
                return True

            if data.startswith("channel:"):
                selected_channel = data.split(":")[1]
                state['selected_channel'] = selected_channel

                # Process all items
                success_count = await self._process_bulk(state)

                date_display = datetime.fromisoformat(state['selected_date']).strftime("%d-%b")

                await query.edit_message_text(
                    f"✅ Saved {success_count}/{len(state['items'])} items\n"
                    f"Date: {date_display}\n"
                    f"Channel: {selected_channel}\n"
                    f"Videos: 1-{success_count}"
                )

                del context.user_data['bulk_selections'][user_id]
                return True

            return False

        except Exception as e:
            logger.error(f"Error handling bulk callback: {e}")
            return False

    async def _process_bulk(self, state: Dict[str, Any]) -> int:
        """Process bulk items - auto-assign to Video 1-4"""
        try:
            items = state['items']
            content_type = state['content_type']
            selected_date = state['selected_date']
            selected_channel = state['selected_channel']

            # Get channel ID
            channel = await self.channel_mgr.get_channel(selected_channel)
            if not channel:
                return 0

            channel_id = channel['id']

            success_count = 0

            for i, item in enumerate(items[:4]):  # Max 4 items
                video_number = i + 1

                try:
                    # Create entry
                    await self.upload_mgr.create_upload_entry(
                        channel_id,
                        selected_date,
                        video_number
                    )

                    if content_type == 'script':
                        await self.upload_mgr.update_script(
                            channel_id,
                            selected_date,
                            video_number,
                            item
                        )
                    elif content_type == 'thumbnail':
                        await self.upload_mgr.update_thumbnail(
                            channel_id,
                            selected_date,
                            video_number,
                            item['file_id'],
                            item['url']
                        )

                    success_count += 1

                except Exception as e:
                    logger.error(f"Error processing bulk item {i}: {e}")
                    continue

            return success_count

        except Exception as e:
            logger.error(f"Error in bulk processing: {e}")
            return 0


# Helper function to check if message contains script or thumbnail
def detect_content_type(message) -> Optional[str]:
    """
    Detect if message contains script or thumbnail

    Args:
        message: Telegram message object

    Returns:
        'script', 'thumbnail', or None
    """
    if message.text and len(message.text) > 50:
        # Likely a script
        return 'script'

    if message.photo:
        # Thumbnail image
        return 'thumbnail'

    if message.document:
        # Check if image file
        if message.document.mime_type and message.document.mime_type.startswith('image/'):
            return 'thumbnail'

    return None
