"""
Admin Module

This module handles all admin-specific functionality including:
- KPI checking for sales representatives (/check command)
- Setting monthly KPI targets (/setting command)
- Role-based access control for admin users
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ContextTypes, ConversationHandler, CommandHandler, 
    CallbackQueryHandler, MessageHandler, filters
)

import auth
import google_sheets
import utils

logger = logging.getLogger(__name__)

# Conversation states for /check command
CHECK_SELECT_USER, CHECK_DISPLAY_KPI = range(2)

# Conversation states for /setting command  
SETTING_SELECT_USER, SETTING_INPUT_MEETUP, SETTING_INPUT_SALES, SETTING_CONFIRM = range(4)


# ============================================================================
# KPI CHECKING SYSTEM (/check command)
# ============================================================================

@auth.require_admin
async def check_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Start KPI checking conversation - display sales representative selection
    
    Args:
        update: Telegram update object
        context: Telegram context object
        
    Returns:
        int: Next conversation state
    """
    try:
        logger.info(f"Admin {update.effective_user.id} started KPI checking")
        
        # Get all sales representatives
        all_users = google_sheets.get_all_users()
        sales_reps = [user for user in all_users if user.get('role') == 'sales']
        
        if not sales_reps:
            await update.message.reply_text(
                "📋 **No Sales Representatives Found**\n\n"
                "There are currently no registered sales representatives in the system.\n"
                "Sales reps need to register first using the /register command.",
                parse_mode='Markdown'
            )
            return ConversationHandler.END
        
        # Create selection keyboard
        keyboard = utils.create_sales_rep_keyboard(sales_reps)
        
        await update.message.reply_text(
            "👥 **Select Sales Representative**\n\n"
            "Choose a sales representative to view their KPI progress:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        return CHECK_SELECT_USER
        
    except Exception as e:
        logger.error(f"Error in check_command: {e}")
        await update.message.reply_text(
            utils.format_error_message("system", "Failed to load sales representatives.")
        )
        return ConversationHandler.END


async def check_select_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle sales representative selection for KPI checking
    
    Args:
        update: Telegram update object
        context: Telegram context object
        
    Returns:
        int: Next conversation state or END
    """
    try:
        query = update.callback_query
        await query.answer()
        
        callback_data = query.data
        
        # Handle cancel
        if callback_data == "cancel_selection":
            await query.edit_message_text(
                "❌ **KPI Check Cancelled**\n\n"
                "You can start again anytime with /check",
                parse_mode='Markdown'
            )
            return ConversationHandler.END
        
        # Extract user ID from callback data
        user_id = utils.extract_user_id_from_callback(callback_data)
        if not user_id:
            await query.edit_message_text(
                utils.format_error_message("validation", "Invalid selection.")
            )
            return ConversationHandler.END
        
        # Store selected user ID in context
        context.user_data['selected_user_id'] = user_id
        
        # Get user info
        user_info = google_sheets.get_user_by_id(user_id)
        if not user_info:
            await query.edit_message_text(
                utils.format_error_message("not_found", "Selected user not found.")
            )
            return ConversationHandler.END
        
        # Get current month progress
        now = datetime.now()
        progress = google_sheets.calculate_user_progress(user_id, now.month, now.year)
        
        if not progress:
            # No targets set for current month
            await query.edit_message_text(
                f"📊 **KPI Progress for {user_info['name']}**\n\n"
                f"🚫 **No targets set for {now.strftime('%B %Y')}**\n\n"
                f"Use /setting to set monthly targets for this sales representative.\n\n"
                f"👤 **User Info:**\n"
                f"• Name: {user_info['name']}\n"
                f"• Nationality: {user_info['nationality']}\n"
                f"• Phone: {user_info['phone']}\n"
                f"• Upline: {user_info['upline']}\n"
                f"• Registered: {user_info['registration_date'][:10]}",
                parse_mode='Markdown'
            )
        else:
            # Display progress with visual indicators
            progress_summary = utils.format_progress_summary(
                progress['current_meetups'], progress['meetup_target'],
                progress['current_sales'], progress['sales_target']
            )
            
            # Calculate overall completion
            meetup_pct = progress['meetup_percentage']
            sales_pct = progress['sales_percentage']
            overall_pct = (meetup_pct + sales_pct) / 2
            
            # Get performance status
            if overall_pct >= 100:
                status_emoji = "🏆"
                status_text = "All targets achieved!"
            elif overall_pct >= 75:
                status_emoji = "⭐"
                status_text = "Excellent performance!"
            elif overall_pct >= 50:
                status_emoji = "👍"
                status_text = "Good progress!"
            elif overall_pct >= 25:
                status_emoji = "📈"
                status_text = "Making progress!"
            else:
                status_emoji = "🚀"
                status_text = "Just getting started!"
            
            await query.edit_message_text(
                f"📊 **KPI Progress for {user_info['name']}**\n"
                f"📅 **Period:** {now.strftime('%B %Y')}\n\n"
                f"{progress_summary}\n\n"
                f"{status_emoji} **Status:** {status_text}\n\n"
                f"📈 **Details:**\n"
                f"• Meetup submissions: {progress['meetup_records_count']}\n"
                f"• Sales submissions: {progress['sales_records_count']}\n\n"
                f"👤 **User Info:**\n"
                f"• Nationality: {user_info['nationality']}\n"
                f"• Phone: {user_info['phone']}\n"
                f"• Upline: {user_info['upline']}",
                parse_mode='Markdown'
            )
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error in check_select_user: {e}")
        await query.edit_message_text(
            utils.format_error_message("system", "Failed to load KPI progress.")
        )
        return ConversationHandler.END


async def check_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle cancellation of check conversation"""
    await update.message.reply_text(
        "❌ **KPI Check Cancelled**\n\n"
        "You can start again anytime with /check",
        parse_mode='Markdown'
    )
    return ConversationHandler.END


# ============================================================================
# TARGET SETTING SYSTEM (/setting command) 
# ============================================================================

@auth.require_admin
async def setting_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Start target setting conversation - display sales representative selection
    
    Args:
        update: Telegram update object
        context: Telegram context object
        
    Returns:
        int: Next conversation state
    """
    try:
        logger.info(f"Admin {update.effective_user.id} started target setting")
        
        # Get all sales representatives
        all_users = google_sheets.get_all_users()
        sales_reps = [user for user in all_users if user.get('role') == 'sales']
        
        if not sales_reps:
            await update.message.reply_text(
                "📋 **No Sales Representatives Found**\n\n"
                "There are currently no registered sales representatives in the system.\n"
                "Sales reps need to register first using the /register command.",
                parse_mode='Markdown'
            )
            return ConversationHandler.END
        
        # Create selection keyboard
        keyboard = utils.create_sales_rep_keyboard(sales_reps)
        
        await update.message.reply_text(
            "🎯 **Set Monthly KPI Targets**\n\n"
            "Choose a sales representative to set their monthly targets:",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        return SETTING_SELECT_USER
        
    except Exception as e:
        logger.error(f"Error in setting_command: {e}")
        await update.message.reply_text(
            utils.format_error_message("system", "Failed to load sales representatives.")
        )
        return ConversationHandler.END


async def setting_select_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle sales representative selection for target setting
    
    Args:
        update: Telegram update object
        context: Telegram context object
        
    Returns:
        int: Next conversation state or END
    """
    try:
        query = update.callback_query
        await query.answer()
        
        callback_data = query.data
        
        # Handle cancel
        if callback_data == "cancel_selection":
            await query.edit_message_text(
                "❌ **Target Setting Cancelled**\n\n"
                "You can start again anytime with /setting",
                parse_mode='Markdown'
            )
            return ConversationHandler.END
        
        # Extract user ID from callback data
        user_id = utils.extract_user_id_from_callback(callback_data)
        if not user_id:
            await query.edit_message_text(
                utils.format_error_message("validation", "Invalid selection.")
            )
            return ConversationHandler.END
        
        # Store selected user ID in context
        context.user_data['selected_user_id'] = user_id
        
        # Get user info
        user_info = google_sheets.get_user_by_id(user_id)
        if not user_info:
            await query.edit_message_text(
                utils.format_error_message("not_found", "Selected user not found.")
            )
            return ConversationHandler.END
        
        context.user_data['selected_user_name'] = user_info['name']
        
        # Check for existing targets for current month
        now = datetime.now()
        existing_targets = google_sheets.get_monthly_targets(user_id, now.month, now.year)
        
        existing_info = ""
        if existing_targets:
            existing_info = (
                f"\n\n📋 **Current Targets for {now.strftime('%B %Y')}:**\n"
                f"• Meetups: {existing_targets['meetup_target']}\n"
                f"• Sales: ${existing_targets['sales_target']:,.2f}\n"
                f"*(These will be overwritten)*"
            )
        
        await query.edit_message_text(
            f"🎯 **Setting Targets for {user_info['name']}**\n"
            f"📅 **Period:** {now.strftime('%B %Y')}{existing_info}\n\n"
            f"🤝 **Step 1/2: Enter Meetup Target**\n\n"
            f"Please enter the number of meetups (client meetings) this sales representative should complete this month.\n\n"
            f"💡 *Example: 20*",
            parse_mode='Markdown'
        )
        
        return SETTING_INPUT_MEETUP
        
    except Exception as e:
        logger.error(f"Error in setting_select_user: {e}")
        await query.edit_message_text(
            utils.format_error_message("system", "Failed to process selection.")
        )
        return ConversationHandler.END


async def setting_input_meetup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle meetup target input with validation
    
    Args:
        update: Telegram update object
        context: Telegram context object
        
    Returns:
        int: Next conversation state
    """
    try:
        meetup_input = update.message.text.strip()
        
        # Validate input
        try:
            meetup_target = int(meetup_input)
            if meetup_target < 0:
                raise ValueError("Negative number")
            if meetup_target > 1000:  # Reasonable upper limit
                raise ValueError("Too large")
        except ValueError:
            await update.message.reply_text(
                "⚠️ **Invalid Input**\n\n"
                "Please enter a valid number between 0 and 1000 for the meetup target.\n\n"
                "💡 *Example: 20*",
                parse_mode='Markdown'
            )
            return SETTING_INPUT_MEETUP
        
        # Store meetup target
        context.user_data['meetup_target'] = meetup_target
        
        user_name = context.user_data.get('selected_user_name', 'Selected User')
        
        await update.message.reply_text(
            f"🎯 **Setting Targets for {user_name}**\n\n"
            f"✅ **Meetup Target:** {meetup_target} meetings\n\n"
            f"💰 **Step 2/2: Enter Sales Target**\n\n"
            f"Please enter the sales amount target for this month (in dollars).\n\n"
            f"💡 *Examples: 5000, 10000.50*",
            parse_mode='Markdown'
        )
        
        return SETTING_INPUT_SALES
        
    except Exception as e:
        logger.error(f"Error in setting_input_meetup: {e}")
        await update.message.reply_text(
            utils.format_error_message("system", "Failed to process meetup target.")
        )
        return ConversationHandler.END


async def setting_input_sales(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle sales target input with validation
    
    Args:
        update: Telegram update object
        context: Telegram context object
        
    Returns:
        int: Next conversation state
    """
    try:
        sales_input = update.message.text.strip()
        
        # Validate input
        try:
            sales_target = float(sales_input)
            if sales_target < 0:
                raise ValueError("Negative number")
            if sales_target > 1000000:  # Reasonable upper limit
                raise ValueError("Too large")
        except ValueError:
            await update.message.reply_text(
                "⚠️ **Invalid Input**\n\n"
                "Please enter a valid number between 0 and 1,000,000 for the sales target.\n\n"
                "💡 *Examples: 5000, 10000.50*",
                parse_mode='Markdown'
            )
            return SETTING_INPUT_SALES
        
        # Store sales target
        context.user_data['sales_target'] = sales_target
        
        # Create confirmation message
        user_name = context.user_data.get('selected_user_name', 'Selected User')
        meetup_target = context.user_data.get('meetup_target', 0)
        now = datetime.now()
        
        # Create confirmation keyboard
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Confirm", callback_data="confirm_targets"),
                InlineKeyboardButton("❌ Cancel", callback_data="cancel_targets")
            ]
        ])
        
        await update.message.reply_text(
            f"🎯 **Confirm Targets for {user_name}**\n"
            f"📅 **Period:** {now.strftime('%B %Y')}\n\n"
            f"🤝 **Meetup Target:** {meetup_target} meetings\n"
            f"💰 **Sales Target:** ${sales_target:,.2f}\n\n"
            f"❓ **Confirm these targets?**",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        
        return SETTING_CONFIRM
        
    except Exception as e:
        logger.error(f"Error in setting_input_sales: {e}")
        await update.message.reply_text(
            utils.format_error_message("system", "Failed to process sales target.")
        )
        return ConversationHandler.END


async def setting_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle target confirmation and storage
    
    Args:
        update: Telegram update object
        context: Telegram context object
        
    Returns:
        int: ConversationHandler.END
    """
    try:
        query = update.callback_query
        await query.answer()
        
        callback_data = query.data
        
        if callback_data == "cancel_targets":
            await query.edit_message_text(
                "❌ **Target Setting Cancelled**\n\n"
                "No targets were saved. You can start again anytime with /setting",
                parse_mode='Markdown'
            )
            return ConversationHandler.END
        
        if callback_data == "confirm_targets":
            # Get stored data
            user_id = context.user_data.get('selected_user_id')
            user_name = context.user_data.get('selected_user_name', 'Selected User')
            meetup_target = context.user_data.get('meetup_target')
            sales_target = context.user_data.get('sales_target')
            
            if not all([user_id, meetup_target is not None, sales_target is not None]):
                await query.edit_message_text(
                    utils.format_error_message("validation", "Missing target data.")
                )
                return ConversationHandler.END
            
            # Save targets to Google Sheets
            now = datetime.now()
            success = google_sheets.set_monthly_targets(
                user_id, now.month, now.year, meetup_target, sales_target
            )
            
            if success:
                await query.edit_message_text(
                    f"🎉 **Targets Set Successfully!**\n\n"
                    f"👤 **Sales Rep:** {user_name}\n"
                    f"📅 **Period:** {now.strftime('%B %Y')}\n\n"
                    f"🤝 **Meetup Target:** {meetup_target} meetings\n"
                    f"💰 **Sales Target:** ${sales_target:,.2f}\n\n"
                    f"✅ The sales representative can now track their progress using /kpi\n"
                    f"📊 You can check their progress anytime using /check",
                    parse_mode='Markdown'
                )
                
                logger.info(f"Admin {update.effective_user.id} set targets for user {user_id}: "
                          f"meetups={meetup_target}, sales={sales_target}")
            else:
                await query.edit_message_text(
                    utils.format_error_message("system", "Failed to save targets to database.")
                )
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error in setting_confirm: {e}")
        await query.edit_message_text(
            utils.format_error_message("system", "Failed to save targets.")
        )
        return ConversationHandler.END


async def setting_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle cancellation of setting conversation"""
    await update.message.reply_text(
        "❌ **Target Setting Cancelled**\n\n"
        "You can start again anytime with /setting",
        parse_mode='Markdown'
    )
    return ConversationHandler.END


# ============================================================================
# CONVERSATION HANDLERS SETUP
# ============================================================================

def get_admin_handlers():
    """
    Get all admin conversation handlers
    
    Returns:
        list: List of conversation handlers for admin functionality
    """
    
    # KPI Check conversation handler
    check_handler = ConversationHandler(
        entry_points=[CommandHandler('check', check_command)],
        states={
            CHECK_SELECT_USER: [
                CallbackQueryHandler(check_select_user, pattern=r'^(select_user_\d+|cancel_selection)$')
            ],
        },
        fallbacks=[
            CommandHandler('cancel', check_cancel),
            MessageHandler(filters.TEXT & ~filters.COMMAND, check_cancel)
        ],
        per_chat=True,
        per_message=False,
        name="admin_check"
    )
    
    # Target Setting conversation handler
    setting_handler = ConversationHandler(
        entry_points=[CommandHandler('setting', setting_command)],
        states={
            SETTING_SELECT_USER: [
                CallbackQueryHandler(setting_select_user, pattern=r'^(select_user_\d+|cancel_selection)$')
            ],
            SETTING_INPUT_MEETUP: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, setting_input_meetup)
            ],
            SETTING_INPUT_SALES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, setting_input_sales)
            ],
            SETTING_CONFIRM: [
                CallbackQueryHandler(setting_confirm, pattern=r'^(confirm_targets|cancel_targets)$')
            ],
        },
        fallbacks=[
            CommandHandler('cancel', setting_cancel),
        ],
        per_chat=True,
        per_message=False,
        name="admin_setting"
    )
    
    return [check_handler, setting_handler]