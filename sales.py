"""
Sales Module

This module handles all sales representative functionality including:
- User registration process
- Personal KPI progress viewing
- KPI submission (meetups and sales)
- Photo upload handling
"""

import logging
import gc
from datetime import datetime
from typing import Optional, Dict, Any
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ContextTypes, 
    ConversationHandler, 
    CommandHandler, 
    MessageHandler, 
    filters
)
import auth
import google_sheets
import utils

logger = logging.getLogger(__name__)

# Registration conversation states
REGISTRATION_NAME, REGISTRATION_NATIONALITY, REGISTRATION_PHONE, REGISTRATION_UPLINE = range(4)

# Registration Conversation Handler
@auth.require_sales
async def register_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Start registration conversation for new sales representatives
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: Next conversation state
    """
    try:
        user_id = update.effective_user.id
        user_name = update.effective_user.first_name or "User"
        
        # Check if user is already registered
        existing_user = google_sheets.get_user_by_id(user_id)
        if existing_user:
            message = (
                f"👋 Hello {existing_user['name']}!\n\n"
                "✅ You are already registered in the KPI system.\n\n"
                "📊 Use /kpi to view your progress\n"
                "🤝 Use /submitkpi to submit meetup records\n"
                "💰 Use /submitsale to submit sales records"
            )
            await update.message.reply_text(message)
            return ConversationHandler.END
        
        # Start registration process
        greeting_emoji = utils.get_greeting_emoji()
        message = (
            f"{greeting_emoji} **Welcome to KPI Bot Registration!**\n\n"
            f"Hello {user_name}! Let's get you registered in the system.\n\n"
            "📝 I'll need to collect some information from you:\n"
            "• Your full name\n"
            "• Your nationality\n"
            "• Your phone number\n"
            "• Your upline's name\n\n"
            "Let's start! Please enter your **full name**:"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        # Store user_id in context for later use
        context.user_data['registration_user_id'] = user_id
        context.user_data['registration_start_time'] = datetime.now()
        
        logger.info(f"Started registration process for user {user_id} ({user_name})")
        return REGISTRATION_NAME
        
    except Exception as e:
        logger.error(f"Error in register_command: {e}")
        await update.message.reply_text(
            utils.format_error_message("registration", "Failed to start registration process.")
        )
        return ConversationHandler.END

async def registration_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle name input during registration
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: Next conversation state
    """
    try:
        name = update.message.text.strip()
        
        # Validate name input
        if not name or len(name) < 2:
            await update.message.reply_text(
                "⚠️ Please enter a valid name (at least 2 characters).\n\n"
                "Enter your **full name**:"
            )
            return REGISTRATION_NAME
        
        if len(name) > 100:
            await update.message.reply_text(
                "⚠️ Name is too long (maximum 100 characters).\n\n"
                "Enter your **full name**:"
            )
            return REGISTRATION_NAME
        
        # Store name in context
        context.user_data['registration_name'] = name
        
        message = (
            f"✅ Great! Your name: **{name}**\n\n"
            "🌍 Now, please enter your **nationality**:"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        logger.info(f"Registration name collected for user {context.user_data.get('registration_user_id')}: {name}")
        return REGISTRATION_NATIONALITY
        
    except Exception as e:
        logger.error(f"Error in registration_name: {e}")
        await update.message.reply_text(
            utils.format_error_message("validation", "Failed to process name input.")
        )
        return REGISTRATION_NAME

async def registration_nationality(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle nationality input during registration
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: Next conversation state
    """
    try:
        nationality = update.message.text.strip()
        
        # Validate nationality input
        if not nationality or len(nationality) < 2:
            await update.message.reply_text(
                "⚠️ Please enter a valid nationality (at least 2 characters).\n\n"
                "Enter your **nationality**:"
            )
            return REGISTRATION_NATIONALITY
        
        if len(nationality) > 50:
            await update.message.reply_text(
                "⚠️ Nationality is too long (maximum 50 characters).\n\n"
                "Enter your **nationality**:"
            )
            return REGISTRATION_NATIONALITY
        
        # Store nationality in context
        context.user_data['registration_nationality'] = nationality
        
        message = (
            f"✅ Nationality: **{nationality}**\n\n"
            "📱 Now, please enter your **phone number** (with country code if international):"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        logger.info(f"Registration nationality collected for user {context.user_data.get('registration_user_id')}: {nationality}")
        return REGISTRATION_PHONE
        
    except Exception as e:
        logger.error(f"Error in registration_nationality: {e}")
        await update.message.reply_text(
            utils.format_error_message("validation", "Failed to process nationality input.")
        )
        return REGISTRATION_NATIONALITY

async def registration_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle phone number input during registration
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: Next conversation state
    """
    try:
        phone = update.message.text.strip()
        
        # Validate phone input
        if not phone or len(phone) < 7:
            await update.message.reply_text(
                "⚠️ Please enter a valid phone number (at least 7 digits).\n\n"
                "Enter your **phone number**:"
            )
            return REGISTRATION_PHONE
        
        if len(phone) > 20:
            await update.message.reply_text(
                "⚠️ Phone number is too long (maximum 20 characters).\n\n"
                "Enter your **phone number**:"
            )
            return REGISTRATION_PHONE
        
        # Basic phone number validation (contains digits)
        if not any(char.isdigit() for char in phone):
            await update.message.reply_text(
                "⚠️ Phone number must contain digits.\n\n"
                "Enter your **phone number**:"
            )
            return REGISTRATION_PHONE
        
        # Store phone in context
        context.user_data['registration_phone'] = phone
        
        message = (
            f"✅ Phone: **{phone}**\n\n"
            "👥 Finally, please enter your **upline's name** (the person who referred you):"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        logger.info(f"Registration phone collected for user {context.user_data.get('registration_user_id')}: {phone}")
        return REGISTRATION_UPLINE
        
    except Exception as e:
        logger.error(f"Error in registration_phone: {e}")
        await update.message.reply_text(
            utils.format_error_message("validation", "Failed to process phone input.")
        )
        return REGISTRATION_PHONE

async def registration_upline(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle upline name input and complete registration
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: ConversationHandler.END
    """
    try:
        upline = update.message.text.strip()
        
        # Validate upline input
        if not upline or len(upline) < 2:
            await update.message.reply_text(
                "⚠️ Please enter a valid upline name (at least 2 characters).\n\n"
                "Enter your **upline's name**:"
            )
            return REGISTRATION_UPLINE
        
        if len(upline) > 100:
            await update.message.reply_text(
                "⚠️ Upline name is too long (maximum 100 characters).\n\n"
                "Enter your **upline's name**:"
            )
            return REGISTRATION_UPLINE
        
        # Store upline in context
        context.user_data['registration_upline'] = upline
        
        # Prepare user data for registration
        user_data = {
            'user_id': context.user_data['registration_user_id'],
            'name': context.user_data['registration_name'],
            'nationality': context.user_data['registration_nationality'],
            'phone': context.user_data['registration_phone'],
            'upline': upline,
            'registration_date': datetime.now().isoformat(),
            'role': 'sales'
        }
        
        # Attempt to register user in Google Sheets
        registration_success = google_sheets.register_user(user_data)
        
        if registration_success:
            # Registration successful
            success_message = (
                "🎉 **Registration Completed Successfully!**\n\n"
                "📋 **Your Information:**\n"
                f"• **Name:** {user_data['name']}\n"
                f"• **Nationality:** {user_data['nationality']}\n"
                f"• **Phone:** {user_data['phone']}\n"
                f"• **Upline:** {user_data['upline']}\n\n"
                "✅ You are now registered in the KPI system!\n\n"
                "🚀 **What's Next?**\n"
                "📊 Use /kpi to view your progress\n"
                "🤝 Use /submitkpi to submit meetup records\n"
                "💰 Use /submitsale to submit sales records\n\n"
                "Welcome to the team! 🎊"
            )
            
            await update.message.reply_text(success_message, parse_mode='Markdown')
            
            logger.info(f"Registration completed successfully for user {user_data['user_id']} ({user_data['name']})")
            
        else:
            # Registration failed
            error_message = (
                "❌ **Registration Failed**\n\n"
                "We encountered an error while saving your registration data.\n"
                "This might be due to:\n"
                "• Database connection issues\n"
                "• Duplicate registration attempt\n"
                "• System maintenance\n\n"
                "🔄 Please try again later using /register\n"
                "If the problem persists, contact your administrator."
            )
            
            await update.message.reply_text(error_message, parse_mode='Markdown')
            
            logger.error(f"Registration failed for user {user_data['user_id']} ({user_data['name']})")
        
        # Clean up context data
        registration_keys = [
            'registration_user_id', 'registration_name', 'registration_nationality',
            'registration_phone', 'registration_upline', 'registration_start_time'
        ]
        for key in registration_keys:
            context.user_data.pop(key, None)
        
        # Force garbage collection to clean up memory
        gc.collect()
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error in registration_upline: {e}")
        await update.message.reply_text(
            utils.format_error_message("registration", "Failed to complete registration.")
        )
        
        # Clean up context data on error
        registration_keys = [
            'registration_user_id', 'registration_name', 'registration_nationality',
            'registration_phone', 'registration_upline', 'registration_start_time'
        ]
        for key in registration_keys:
            context.user_data.pop(key, None)
        
        return ConversationHandler.END

async def registration_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle registration cancellation
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: ConversationHandler.END
    """
    try:
        user_id = update.effective_user.id if update.effective_user else "Unknown"
        
        message = (
            "❌ **Registration Cancelled**\n\n"
            "Your registration process has been cancelled.\n"
            "No data has been saved.\n\n"
            "🔄 You can start registration again anytime using /register"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        # Clean up context data
        registration_keys = [
            'registration_user_id', 'registration_name', 'registration_nationality',
            'registration_phone', 'registration_upline', 'registration_start_time'
        ]
        for key in registration_keys:
            context.user_data.pop(key, None)
        
        logger.info(f"Registration cancelled by user {user_id}")
        
        # Force garbage collection
        gc.collect()
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error in registration_cancel: {e}")
        return ConversationHandler.END

# Create registration conversation handler
def create_registration_handler() -> ConversationHandler:
    """
    Create and return the registration conversation handler
    
    Returns:
        ConversationHandler: Configured registration conversation handler
    """
    return ConversationHandler(
        entry_points=[CommandHandler('register', register_command)],
        states={
            REGISTRATION_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, registration_name)
            ],
            REGISTRATION_NATIONALITY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, registration_nationality)
            ],
            REGISTRATION_PHONE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, registration_phone)
            ],
            REGISTRATION_UPLINE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, registration_upline)
            ],
        },
        fallbacks=[
            CommandHandler('cancel', registration_cancel),
            MessageHandler(filters.COMMAND, registration_cancel)
        ],
        per_chat=True,
        per_user=True,
        allow_reentry=True,
        name="registration_conversation"
    )
# KPI Viewing Handler
@auth.require_sales
async def kpi_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Display current KPI progress for the sales representative
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
    """
    try:
        user_id = update.effective_user.id
        user_name = update.effective_user.first_name or "User"
        
        # Check if user is registered
        user_data = google_sheets.get_user_by_id(user_id)
        if not user_data:
            message = (
                "🚫 **Not Registered**\n\n"
                f"Hello {user_name}! You need to register first before viewing your KPI progress.\n\n"
                "📝 Use /register to get started"
            )
            await update.message.reply_text(message, parse_mode='Markdown')
            return
        
        # Get current month and year
        now = datetime.now()
        current_month = now.month
        current_year = now.year
        
        # Calculate user progress for current month
        progress = google_sheets.calculate_user_progress(user_id, current_month, current_year)
        
        if not progress:
            # No targets set for current month
            month_name = now.strftime("%B %Y")
            message = (
                f"📊 **KPI Progress - {month_name}**\n\n"
                f"👋 Hello {user_data['name']}!\n\n"
                "🎯 **No targets set for this month**\n\n"
                "Your admin needs to set your monthly targets before you can track progress.\n"
                "Please contact your admin to set up your KPI targets.\n\n"
                "💡 **Available Commands:**\n"
                "🤝 /submitkpi - Submit meetup records\n"
                "💰 /submitsale - Submit sales records"
            )
            await update.message.reply_text(message, parse_mode='Markdown')
            return
        
        # Format progress display
        month_name = now.strftime("%B %Y")
        
        # Create progress summary using utility function
        progress_summary = utils.format_progress_summary(
            progress['current_meetups'],
            progress['meetup_target'],
            progress['current_sales'],
            progress['sales_target']
        )
        
        # Calculate days remaining in month
        import calendar
        days_in_month = calendar.monthrange(current_year, current_month)[1]
        days_remaining = days_in_month - now.day
        
        # Determine motivational message based on progress
        overall_pct = (progress['meetup_percentage'] + progress['sales_percentage']) / 2
        
        if overall_pct >= 100:
            motivation = "🏆 Outstanding! You've achieved all your targets!"
            next_action = "Keep up the excellent work and help your teammates!"
        elif overall_pct >= 75:
            motivation = "⭐ Excellent progress! You're almost there!"
            next_action = "Just a little more push to reach 100%!"
        elif overall_pct >= 50:
            motivation = "👍 Good progress! You're on the right track!"
            next_action = f"Focus on the remaining targets with {days_remaining} days left!"
        elif overall_pct >= 25:
            motivation = "📈 Making progress! Keep pushing forward!"
            next_action = f"Increase your efforts - {days_remaining} days remaining!"
        else:
            motivation = "🚀 Time to accelerate! You've got this!"
            next_action = f"Focus and push hard - {days_remaining} days left!"
        
        # Build complete message
        message = (
            f"📊 **KPI Progress - {month_name}**\n\n"
            f"👋 Hello {user_data['name']}!\n\n"
            f"{progress_summary}\n\n"
            f"📅 **Time Remaining:** {days_remaining} days\n\n"
            f"💪 **{motivation}**\n"
            f"🎯 {next_action}\n\n"
            "💡 **Quick Actions:**\n"
            "🤝 /submitkpi - Submit meetup records\n"
            "💰 /submitsale - Submit sales records"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        logger.info(f"KPI progress displayed for user {user_id} ({user_data['name']})")
        
    except Exception as e:
        logger.error(f"Error in kpi_command: {e}")
        await update.message.reply_text(
            utils.format_error_message("kpi_display", "Failed to retrieve KPI progress.")
        )

def format_kpi_display(progress: Dict[str, Any], user_name: str, month_name: str) -> str:
    """
    Format KPI progress display with visual indicators
    
    Args:
        progress (dict): Progress data from calculate_user_progress
        user_name (str): User's name
        month_name (str): Current month name
        
    Returns:
        str: Formatted KPI display message
    """
    try:
        # Create individual progress bars
        meetup_bar = utils.format_progress_bar(
            progress['current_meetups'], 
            progress['meetup_target']
        )
        
        sales_bar = utils.format_progress_bar(
            int(progress['current_sales']), 
            int(progress['sales_target'])
        )
        
        # Calculate overall completion
        overall_pct = (progress['meetup_percentage'] + progress['sales_percentage']) / 2
        
        # Determine overall status
        if overall_pct >= 100:
            overall_emoji = "🏆"
            overall_status = "All targets achieved!"
        elif overall_pct >= 75:
            overall_emoji = "⭐"
            overall_status = "Excellent progress!"
        elif overall_pct >= 50:
            overall_emoji = "👍"
            overall_status = "Good progress!"
        elif overall_pct >= 25:
            overall_emoji = "📈"
            overall_status = "Making progress!"
        else:
            overall_emoji = "🚀"
            overall_status = "Just getting started!"
        
        # Format currency for sales
        sales_current_formatted = utils.format_currency(progress['current_sales'])
        sales_target_formatted = utils.format_currency(progress['sales_target'])
        
        # Build the display
        display = (
            f"📊 **KPI Progress - {month_name}**\n\n"
            f"👋 Hello {user_name}!\n\n"
            f"🤝 **Meetups:**\n"
            f"{meetup_bar}\n"
            f"Target: {progress['meetup_target']} meetups\n"
            f"Current: {progress['current_meetups']} meetups\n\n"
            f"💰 **Sales:**\n"
            f"{sales_bar}\n"
            f"Target: {sales_target_formatted}\n"
            f"Current: {sales_current_formatted}\n\n"
            f"{overall_emoji} **Overall:** {overall_status} ({overall_pct:.1f}%)"
        )
        
        return display
        
    except Exception as e:
        logger.error(f"Error formatting KPI display: {e}")
        return "❌ Error formatting KPI display"

def get_kpi_motivation_message(progress_percentage: float, days_remaining: int) -> tuple:
    """
    Get motivational message based on progress percentage
    
    Args:
        progress_percentage (float): Overall progress percentage
        days_remaining (int): Days remaining in month
        
    Returns:
        tuple: (motivation_message, next_action_message)
    """
    if progress_percentage >= 100:
        motivation = "🏆 Outstanding! You've achieved all your targets!"
        next_action = "Keep up the excellent work and help your teammates!"
    elif progress_percentage >= 75:
        motivation = "⭐ Excellent progress! You're almost there!"
        next_action = "Just a little more push to reach 100%!"
    elif progress_percentage >= 50:
        motivation = "👍 Good progress! You're on the right track!"
        next_action = f"Focus on the remaining targets with {days_remaining} days left!"
    elif progress_percentage >= 25:
        motivation = "📈 Making progress! Keep pushing forward!"
        next_action = f"Increase your efforts - {days_remaining} days remaining!"
    else:
        motivation = "🚀 Time to accelerate! You've got this!"
        next_action = f"Focus and push hard - {days_remaining} days left!"
    
    return motivation, next_action

# Meetup KPI Submission Handler
# Conversation states for meetup submission
MEETUP_CLIENT_COUNT, MEETUP_PHOTO_UPLOAD = range(2)

@auth.require_sales
async def submit_kpi_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Start meetup KPI submission conversation
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: Next conversation state
    """
    try:
        user_id = update.effective_user.id
        user_name = update.effective_user.first_name or "User"
        
        # Check if user is registered
        user_data = google_sheets.get_user_by_id(user_id)
        if not user_data:
            message = (
                "🚫 **Not Registered**\n\n"
                f"Hello {user_name}! You need to register first before submitting KPI records.\n\n"
                "📝 Use /register to get started"
            )
            await update.message.reply_text(message, parse_mode='Markdown')
            return ConversationHandler.END
        
        # Start meetup submission process
        greeting_emoji = utils.get_greeting_emoji()
        message = (
            f"{greeting_emoji} **Meetup KPI Submission**\n\n"
            f"Hello {user_data['name']}! Let's record your meetup activity.\n\n"
            "🤝 **Step 1:** Enter the number of clients you met with today.\n\n"
            "💡 **Tip:** Enter a positive number (e.g., 3 for three clients)\n\n"
            "Please enter the **number of clients**:"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        # Store user data in context for later use
        context.user_data['meetup_user_id'] = user_id
        context.user_data['meetup_user_name'] = user_data['name']
        context.user_data['meetup_start_time'] = datetime.now()
        
        logger.info(f"Started meetup KPI submission for user {user_id} ({user_data['name']})")
        return MEETUP_CLIENT_COUNT
        
    except Exception as e:
        logger.error(f"Error in submit_kpi_command: {e}")
        await update.message.reply_text(
            utils.format_error_message("kpi_submission", "Failed to start meetup submission process.")
        )
        return ConversationHandler.END

async def meetup_client_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle client count input for meetup submission
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: Next conversation state
    """
    try:
        client_count_text = update.message.text.strip()
        
        # Validate client count input
        try:
            client_count = int(client_count_text)
        except ValueError:
            await update.message.reply_text(
                "⚠️ Please enter a valid number.\n\n"
                "Example: 3 (for three clients)\n\n"
                "Enter the **number of clients**:"
            )
            return MEETUP_CLIENT_COUNT
        
        if client_count < 1:
            await update.message.reply_text(
                "⚠️ Please enter a positive number (at least 1).\n\n"
                "Enter the **number of clients**:"
            )
            return MEETUP_CLIENT_COUNT
        
        if client_count > 100:
            await update.message.reply_text(
                "⚠️ That seems like a very high number. Please enter a realistic number (maximum 100).\n\n"
                "Enter the **number of clients**:"
            )
            return MEETUP_CLIENT_COUNT
        
        # Store client count in context
        context.user_data['meetup_client_count'] = client_count
        
        message = (
            f"✅ Great! You met with **{client_count}** client{'s' if client_count > 1 else ''}.\n\n"
            "📸 **Step 2:** Please upload a photo as proof of your meetup.\n\n"
            "💡 **Tips:**\n"
            "• Take a photo during or after the meetup\n"
            "• Make sure the image is clear and relevant\n"
            "• Supported formats: JPG, PNG\n\n"
            "Please **upload your photo**:"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        logger.info(f"Meetup client count collected for user {context.user_data.get('meetup_user_id')}: {client_count}")
        return MEETUP_PHOTO_UPLOAD
        
    except Exception as e:
        logger.error(f"Error in meetup_client_count: {e}")
        await update.message.reply_text(
            utils.format_error_message("validation", "Failed to process client count input.")
        )
        return MEETUP_CLIENT_COUNT

async def meetup_photo_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle photo upload for meetup submission and complete the process
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: ConversationHandler.END
    """
    try:
        # Check if message contains a photo
        if not update.message.photo:
            await update.message.reply_text(
                "📸 Please upload a photo to complete your meetup submission.\n\n"
                "💡 **How to upload:**\n"
                "• Tap the attachment button (📎)\n"
                "• Select 'Photo' or 'Camera'\n"
                "• Choose or take your photo\n\n"
                "Please **upload your photo**:"
            )
            return MEETUP_PHOTO_UPLOAD
        
        # Get the largest photo size
        photo = update.message.photo[-1]
        user_id = context.user_data['meetup_user_id']
        client_count = context.user_data['meetup_client_count']
        user_name = context.user_data['meetup_user_name']
        
        # Send processing message
        processing_message = await update.message.reply_text(
            "⏳ **Processing your submission...**\n\n"
            "📸 Uploading photo to secure storage...\n"
            "💾 Saving your KPI record...\n\n"
            "Please wait a moment..."
        )
        
        try:
            # Get Telegram file
            telegram_file = await photo.get_file()
            
            # Generate filename
            timestamp = datetime.now()
            filename = utils.generate_photo_filename(user_id, 'meetup', timestamp)
            
            # Upload photo to Google Drive with memory management
            logger.info(f"Starting photo upload for meetup submission: {filename}")
            
            # Import google_drive here to avoid circular imports
            import google_drive
            
            photo_link = await google_drive.upload_photo_from_telegram(
                telegram_file, 
                filename, 
                'meetups',
                timestamp.year,
                timestamp.month
            )
            
            if not photo_link:
                # Photo upload failed
                await processing_message.edit_text(
                    "❌ **Photo Upload Failed**\n\n"
                    "We couldn't upload your photo to secure storage.\n"
                    "This might be due to:\n"
                    "• Network connectivity issues\n"
                    "• File size too large\n"
                    "• Temporary server issues\n\n"
                    "🔄 Please try again with /submitkpi"
                )
                
                # Clean up context data
                cleanup_meetup_context(context)
                return ConversationHandler.END
            
            # Record KPI submission in Google Sheets
            logger.info(f"Recording meetup KPI submission for user {user_id}")
            
            record_success = google_sheets.record_kpi_submission(
                user_id=user_id,
                record_type='meetup',
                value=client_count,
                photo_link=photo_link,
                record_date=timestamp
            )
            
            if record_success:
                # Get updated progress for display
                current_month = timestamp.month
                current_year = timestamp.year
                progress = google_sheets.calculate_user_progress(user_id, current_month, current_year)
                
                # Build success message
                success_message = (
                    "🎉 **Meetup KPI Submitted Successfully!**\n\n"
                    f"✅ **Recorded:** {client_count} client{'s' if client_count > 1 else ''} met\n"
                    f"📸 **Photo:** Uploaded and secured\n"
                    f"📅 **Date:** {timestamp.strftime('%B %d, %Y at %I:%M %p')}\n\n"
                )
                
                # Add progress update if available
                if progress:
                    meetup_progress = utils.format_progress_bar(
                        progress['current_meetups'],
                        progress['meetup_target']
                    )
                    success_message += (
                        f"📊 **Updated Progress:**\n"
                        f"🤝 Meetups: {meetup_progress}\n\n"
                    )
                
                success_message += (
                    "🚀 **Keep up the great work!**\n\n"
                    "💡 **Next Steps:**\n"
                    "📊 /kpi - View your complete progress\n"
                    "💰 /submitsale - Submit sales records\n"
                    "🤝 /submitkpi - Submit more meetups"
                )
                
                await processing_message.edit_text(success_message, parse_mode='Markdown')
                
                logger.info(f"Meetup KPI submission completed successfully for user {user_id} ({user_name})")
                
            else:
                # KPI record failed to save
                await processing_message.edit_text(
                    "⚠️ **Partial Success**\n\n"
                    "📸 Your photo was uploaded successfully, but we couldn't save the KPI record.\n\n"
                    "📞 Please contact your administrator to manually record:\n"
                    f"• **User:** {user_name}\n"
                    f"• **Clients:** {client_count}\n"
                    f"• **Date:** {timestamp.strftime('%B %d, %Y')}\n"
                    f"• **Photo:** {photo_link}\n\n"
                    "🔄 You can also try submitting again with /submitkpi"
                )
                
                logger.error(f"Failed to record KPI submission for user {user_id}, but photo uploaded: {photo_link}")
        
        except Exception as upload_error:
            logger.error(f"Error during photo upload/processing: {upload_error}")
            await processing_message.edit_text(
                utils.format_error_message("upload", "Failed to process photo upload.")
            )
        
        # Clean up context data
        cleanup_meetup_context(context)
        
        # Force garbage collection to clean up memory
        gc.collect()
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error in meetup_photo_upload: {e}")
        await update.message.reply_text(
            utils.format_error_message("kpi_submission", "Failed to complete meetup submission.")
        )
        
        # Clean up context data on error
        cleanup_meetup_context(context)
        return ConversationHandler.END

async def meetup_submission_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle meetup submission cancellation
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: ConversationHandler.END
    """
    try:
        user_id = update.effective_user.id if update.effective_user else "Unknown"
        
        message = (
            "❌ **Meetup Submission Cancelled**\n\n"
            "Your meetup submission has been cancelled.\n"
            "No data has been saved.\n\n"
            "🔄 You can start a new submission anytime using /submitkpi"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        # Clean up context data
        cleanup_meetup_context(context)
        
        logger.info(f"Meetup submission cancelled by user {user_id}")
        
        # Force garbage collection
        gc.collect()
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error in meetup_submission_cancel: {e}")
        return ConversationHandler.END

def cleanup_meetup_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Clean up meetup submission context data
    
    Args:
        context (ContextTypes.DEFAULT_TYPE): Telegram context
    """
    try:
        meetup_keys = [
            'meetup_user_id', 'meetup_user_name', 'meetup_client_count', 'meetup_start_time'
        ]
        for key in meetup_keys:
            context.user_data.pop(key, None)
        
        logger.info("Cleaned up meetup submission context data")
        
    except Exception as e:
        logger.error(f"Error cleaning up meetup context: {e}")

# Create meetup submission conversation handler
def create_meetup_submission_handler() -> ConversationHandler:
    """
    Create and return the meetup submission conversation handler
    
    Returns:
        ConversationHandler: Configured meetup submission conversation handler
    """
    return ConversationHandler(
        entry_points=[CommandHandler('submitkpi', submit_kpi_command)],
        states={
            MEETUP_CLIENT_COUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, meetup_client_count)
            ],
            MEETUP_PHOTO_UPLOAD: [
                MessageHandler(filters.PHOTO, meetup_photo_upload),
                MessageHandler(filters.TEXT & ~filters.COMMAND, meetup_photo_upload)
            ],
        },
        fallbacks=[
            CommandHandler('cancel', meetup_submission_cancel),
            MessageHandler(filters.COMMAND, meetup_submission_cancel)
        ],
        per_chat=True,
        per_user=True,
        allow_reentry=True,
        name="meetup_submission_conversation"
    )

# Sales KPI Submission Handler
# Conversation states for sales submission
SALES_AMOUNT, SALES_PHOTO_UPLOAD = range(2)

@auth.require_sales
async def submit_sale_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Start sales KPI submission conversation
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: Next conversation state
    """
    try:
        user_id = update.effective_user.id
        user_name = update.effective_user.first_name or "User"
        
        # Check if user is registered
        user_data = google_sheets.get_user_by_id(user_id)
        if not user_data:
            message = (
                "🚫 **Not Registered**\n\n"
                f"Hello {user_name}! You need to register first before submitting sales records.\n\n"
                "📝 Use /register to get started"
            )
            await update.message.reply_text(message, parse_mode='Markdown')
            return ConversationHandler.END
        
        # Start sales submission process
        greeting_emoji = utils.get_greeting_emoji()
        message = (
            f"{greeting_emoji} **Sales KPI Submission**\n\n"
            f"Hello {user_data['name']}! Let's record your sales achievement.\n\n"
            "💰 **Step 1:** Enter the sales amount you achieved today.\n\n"
            "💡 **Tips:**\n"
            "• Enter amount in dollars (e.g., 1500 or 1500.50)\n"
            "• Don't include currency symbols\n"
            "• Use decimal point for cents if needed\n\n"
            "Please enter the **sales amount**:"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        # Store user data in context for later use
        context.user_data['sales_user_id'] = user_id
        context.user_data['sales_user_name'] = user_data['name']
        context.user_data['sales_start_time'] = datetime.now()
        
        logger.info(f"Started sales KPI submission for user {user_id} ({user_data['name']})")
        return SALES_AMOUNT
        
    except Exception as e:
        logger.error(f"Error in submit_sale_command: {e}")
        await update.message.reply_text(
            utils.format_error_message("sales_submission", "Failed to start sales submission process.")
        )
        return ConversationHandler.END

async def sales_amount_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle sales amount input for sales submission
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: Next conversation state
    """
    try:
        sales_amount_text = update.message.text.strip()
        
        # Validate sales amount input
        try:
            sales_amount = float(sales_amount_text)
        except ValueError:
            await update.message.reply_text(
                "⚠️ Please enter a valid number.\n\n"
                "Examples:\n"
                "• 1500 (for $1,500)\n"
                "• 1500.50 (for $1,500.50)\n"
                "• 250 (for $250)\n\n"
                "Enter the **sales amount**:"
            )
            return SALES_AMOUNT
        
        if sales_amount < 0:
            await update.message.reply_text(
                "⚠️ Please enter a positive amount.\n\n"
                "Enter the **sales amount**:"
            )
            return SALES_AMOUNT
        
        if sales_amount > 1000000:  # $1M limit for sanity check
            await update.message.reply_text(
                "⚠️ That seems like a very high amount. Please enter a realistic sales amount (maximum $1,000,000).\n\n"
                "Enter the **sales amount**:"
            )
            return SALES_AMOUNT
        
        # Store sales amount in context
        context.user_data['sales_amount'] = sales_amount
        
        # Format amount for display
        formatted_amount = utils.format_currency(sales_amount)
        
        message = (
            f"✅ Excellent! You achieved **{formatted_amount}** in sales.\n\n"
            "📸 **Step 2:** Please upload a photo as proof of your sale.\n\n"
            "💡 **Tips:**\n"
            "• Take a photo of the receipt, contract, or confirmation\n"
            "• Make sure the image is clear and readable\n"
            "• Include any relevant documentation\n"
            "• Supported formats: JPG, PNG\n\n"
            "Please **upload your photo**:"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        logger.info(f"Sales amount collected for user {context.user_data.get('sales_user_id')}: ${sales_amount}")
        return SALES_PHOTO_UPLOAD
        
    except Exception as e:
        logger.error(f"Error in sales_amount_input: {e}")
        await update.message.reply_text(
            utils.format_error_message("validation", "Failed to process sales amount input.")
        )
        return SALES_AMOUNT

async def sales_photo_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle photo upload for sales submission and complete the process
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: ConversationHandler.END
    """
    try:
        # Check if message contains a photo
        if not update.message.photo:
            await update.message.reply_text(
                "📸 Please upload a photo to complete your sales submission.\n\n"
                "💡 **How to upload:**\n"
                "• Tap the attachment button (📎)\n"
                "• Select 'Photo' or 'Camera'\n"
                "• Choose or take your photo\n\n"
                "Please **upload your photo**:"
            )
            return SALES_PHOTO_UPLOAD
        
        # Get the largest photo size
        photo = update.message.photo[-1]
        user_id = context.user_data['sales_user_id']
        sales_amount = context.user_data['sales_amount']
        user_name = context.user_data['sales_user_name']
        
        # Send processing message
        processing_message = await update.message.reply_text(
            "⏳ **Processing your sales submission...**\n\n"
            "📸 Uploading photo to secure storage...\n"
            "💾 Saving your sales record...\n\n"
            "Please wait a moment..."
        )
        
        try:
            # Get Telegram file
            telegram_file = await photo.get_file()
            
            # Generate filename
            timestamp = datetime.now()
            filename = utils.generate_photo_filename(user_id, 'sale', timestamp)
            
            # Upload photo to Google Drive with memory management
            logger.info(f"Starting photo upload for sales submission: {filename}")
            
            # Import google_drive here to avoid circular imports
            import google_drive
            
            photo_link = await google_drive.upload_photo_from_telegram(
                telegram_file, 
                filename, 
                'sales',
                timestamp.year,
                timestamp.month
            )
            
            if not photo_link:
                # Photo upload failed
                await processing_message.edit_text(
                    "❌ **Photo Upload Failed**\n\n"
                    "We couldn't upload your photo to secure storage.\n"
                    "This might be due to:\n"
                    "• Network connectivity issues\n"
                    "• File size too large\n"
                    "• Temporary server issues\n\n"
                    "🔄 Please try again with /submitsale"
                )
                
                # Clean up context data
                cleanup_sales_context(context)
                return ConversationHandler.END
            
            # Record KPI submission in Google Sheets
            logger.info(f"Recording sales KPI submission for user {user_id}")
            
            record_success = google_sheets.record_kpi_submission(
                user_id=user_id,
                record_type='sale',
                value=sales_amount,
                photo_link=photo_link,
                record_date=timestamp
            )
            
            if record_success:
                # Get updated progress for display
                current_month = timestamp.month
                current_year = timestamp.year
                progress = google_sheets.calculate_user_progress(user_id, current_month, current_year)
                
                # Format amount for display
                formatted_amount = utils.format_currency(sales_amount)
                
                # Build success message
                success_message = (
                    "🎉 **Sales KPI Submitted Successfully!**\n\n"
                    f"✅ **Recorded:** {formatted_amount} in sales\n"
                    f"📸 **Photo:** Uploaded and secured\n"
                    f"📅 **Date:** {timestamp.strftime('%B %d, %Y at %I:%M %p')}\n\n"
                )
                
                # Add progress update if available
                if progress:
                    sales_progress = utils.format_progress_bar(
                        int(progress['current_sales']),
                        int(progress['sales_target'])
                    )
                    success_message += (
                        f"📊 **Updated Progress:**\n"
                        f"💰 Sales: {sales_progress}\n\n"
                    )
                
                success_message += (
                    "🚀 **Outstanding achievement!**\n\n"
                    "💡 **Next Steps:**\n"
                    "📊 /kpi - View your complete progress\n"
                    "🤝 /submitkpi - Submit meetup records\n"
                    "💰 /submitsale - Submit more sales"
                )
                
                await processing_message.edit_text(success_message, parse_mode='Markdown')
                
                logger.info(f"Sales KPI submission completed successfully for user {user_id} ({user_name})")
                
            else:
                # KPI record failed to save
                formatted_amount = utils.format_currency(sales_amount)
                await processing_message.edit_text(
                    "⚠️ **Partial Success**\n\n"
                    "📸 Your photo was uploaded successfully, but we couldn't save the sales record.\n\n"
                    "📞 Please contact your administrator to manually record:\n"
                    f"• **User:** {user_name}\n"
                    f"• **Sales:** {formatted_amount}\n"
                    f"• **Date:** {timestamp.strftime('%B %d, %Y')}\n"
                    f"• **Photo:** {photo_link}\n\n"
                    "🔄 You can also try submitting again with /submitsale"
                )
                
                logger.error(f"Failed to record sales KPI submission for user {user_id}, but photo uploaded: {photo_link}")
        
        except Exception as upload_error:
            logger.error(f"Error during sales photo upload/processing: {upload_error}")
            await processing_message.edit_text(
                utils.format_error_message("upload", "Failed to process photo upload.")
            )
        
        # Clean up context data
        cleanup_sales_context(context)
        
        # Force garbage collection to clean up memory
        gc.collect()
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error in sales_photo_upload: {e}")
        await update.message.reply_text(
            utils.format_error_message("sales_submission", "Failed to complete sales submission.")
        )
        
        # Clean up context data on error
        cleanup_sales_context(context)
        return ConversationHandler.END

async def sales_submission_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Handle sales submission cancellation
    
    Args:
        update (Update): Telegram update object
        context (ContextTypes.DEFAULT_TYPE): Telegram context
        
    Returns:
        int: ConversationHandler.END
    """
    try:
        user_id = update.effective_user.id if update.effective_user else "Unknown"
        
        message = (
            "❌ **Sales Submission Cancelled**\n\n"
            "Your sales submission has been cancelled.\n"
            "No data has been saved.\n\n"
            "🔄 You can start a new submission anytime using /submitsale"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
        
        # Clean up context data
        cleanup_sales_context(context)
        
        logger.info(f"Sales submission cancelled by user {user_id}")
        
        # Force garbage collection
        gc.collect()
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error in sales_submission_cancel: {e}")
        return ConversationHandler.END

def cleanup_sales_context(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Clean up sales submission context data
    
    Args:
        context (ContextTypes.DEFAULT_TYPE): Telegram context
    """
    try:
        sales_keys = [
            'sales_user_id', 'sales_user_name', 'sales_amount', 'sales_start_time'
        ]
        for key in sales_keys:
            context.user_data.pop(key, None)
        
        logger.info("Cleaned up sales submission context data")
        
    except Exception as e:
        logger.error(f"Error cleaning up sales context: {e}")

# Create sales submission conversation handler
def create_sales_submission_handler() -> ConversationHandler:
    """
    Create and return the sales submission conversation handler
    
    Returns:
        ConversationHandler: Configured sales submission conversation handler
    """
    return ConversationHandler(
        entry_points=[CommandHandler('submitsale', submit_sale_command)],
        states={
            SALES_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, sales_amount_input)
            ],
            SALES_PHOTO_UPLOAD: [
                MessageHandler(filters.PHOTO, sales_photo_upload),
                MessageHandler(filters.TEXT & ~filters.COMMAND, sales_photo_upload)
            ],
        },
        fallbacks=[
            CommandHandler('cancel', sales_submission_cancel),
            MessageHandler(filters.COMMAND, sales_submission_cancel)
        ],
        per_chat=True,
        per_user=True,
        allow_reentry=True,
        name="sales_submission_conversation"
    )

def get_sales_handlers():
    """
    Get all sales conversation handlers and commands
    
    Returns:
        list: List of handlers for sales functionality
    """
    handlers = []
    
    try:
        # Add registration conversation handler
        registration_handler = create_registration_handler()
        handlers.append(registration_handler)
        
        # Add KPI viewing command handler
        from telegram.ext import CommandHandler
        kpi_handler = CommandHandler('kpi', kpi_command)
        handlers.append(kpi_handler)
        
        # Add meetup submission conversation handler
        meetup_handler = create_meetup_submission_handler()
        handlers.append(meetup_handler)
        
        # Add sales submission conversation handler
        sales_handler = create_sales_submission_handler()
        handlers.append(sales_handler)
        
        logger.info(f"Created {len(handlers)} sales handlers")
        return handlers
        
    except Exception as e:
        logger.error(f"Error creating sales handlers: {e}")
        return []
