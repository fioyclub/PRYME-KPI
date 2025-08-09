#!/usr/bin/env python3
"""
Telegram KPI Bot - Main Entry Point

This is the main application entry point for the Telegram KPI Bot.
It initializes the bot, sets up handlers, and manages the application lifecycle.
"""

import logging
import os
from datetime import datetime
from telegram.ext import Application
from telegram import Update
from telegram.ext import ContextTypes
from dotenv import load_dotenv

# Import error handling system first
from error_handler import setup_logging, error_handler, log_system_event, check_system_health

import auth

# Load environment variables
load_dotenv()

# Initialize comprehensive logging system
setup_logging(
    log_level=os.getenv('LOG_LEVEL', 'INFO'),
    log_file=os.getenv('LOG_FILE', 'kpi_bot.log')
)
logger = logging.getLogger(__name__)


def perform_graceful_shutdown() -> None:
    """
    Perform graceful shutdown of all bot components
    
    This function:
    1. Stops scheduled memory cleanup
    2. Performs final memory cleanup
    3. Closes any open resources
    4. Logs shutdown completion
    """
    try:
        logger.info("Starting graceful shutdown sequence...")
        log_system_event("shutdown_start", "Graceful shutdown initiated")
        
        # Stop scheduled memory cleanup
        logger.info("Stopping scheduled memory cleanup...")
        try:
            import memory_management
            if memory_management.stop_scheduled_cleanup():
                logger.info("Scheduled memory cleanup stopped successfully")
                log_system_event("memory_cleanup_stopped", "Scheduled cleanup stopped")
            else:
                logger.warning("Failed to stop scheduled memory cleanup")
                log_system_event("memory_cleanup_stop_failed", "Failed to stop scheduled cleanup", "WARNING")
        except Exception as e:
            logger.error(f"Error stopping memory cleanup: {e}")
            log_system_event("memory_cleanup_error", f"Error stopping cleanup: {e}", "ERROR")
        
        # Perform final memory cleanup
        logger.info("Performing final memory cleanup...")
        try:
            import memory_management
            cleanup_results = memory_management.emergency_cleanup()
            logger.info(f"Final cleanup completed: {cleanup_results}")
            log_system_event("final_cleanup", f"Emergency cleanup results: {cleanup_results}")
        except Exception as e:
            logger.error(f"Error during final cleanup: {e}")
            log_system_event("final_cleanup_error", f"Final cleanup error: {e}", "ERROR")
        
        # Log final memory status
        try:
            import memory_management
            memory_management.log_memory_status()
        except Exception as e:
            logger.error(f"Error logging final memory status: {e}")
        
        logger.info("Graceful shutdown sequence completed")
        log_system_event("shutdown_complete", "Graceful shutdown completed successfully")
        
    except Exception as e:
        logger.error(f"Error during graceful shutdown: {e}")
        log_system_event("shutdown_error", f"Shutdown error: {e}", "ERROR")


def comprehensive_health_check() -> dict:
    """
    Perform comprehensive health check of all system components
    
    Returns:
        dict: Health check results with detailed status for each component
    """
    health_results = {
        'overall_status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'components': {},
        'warnings': [],
        'errors': []
    }
    
    try:
        logger.info("Starting comprehensive health check...")
        
        # Check Google Sheets connectivity
        try:
            import google_sheets
            sheets_status = google_sheets.test_sheets_connection()
            health_results['components']['google_sheets'] = {
                'status': 'healthy' if sheets_status else 'unhealthy',
                'details': 'Connection test successful' if sheets_status else 'Connection test failed'
            }
            if not sheets_status:
                health_results['errors'].append('Google Sheets connection failed')
        except Exception as e:
            health_results['components']['google_sheets'] = {
                'status': 'error',
                'details': f'Error testing connection: {e}'
            }
            health_results['errors'].append(f'Google Sheets error: {e}')
        
        # Check Google Drive connectivity
        try:
            import google_drive
            drive_status = google_drive.test_drive_connection()
            health_results['components']['google_drive'] = {
                'status': 'healthy' if drive_status else 'unhealthy',
                'details': 'Connection test successful' if drive_status else 'Connection test failed'
            }
            if not drive_status:
                health_results['errors'].append('Google Drive connection failed')
        except Exception as e:
            health_results['components']['google_drive'] = {
                'status': 'error',
                'details': f'Error testing connection: {e}'
            }
            health_results['errors'].append(f'Google Drive error: {e}')
        
        # Check authentication system
        try:
            import auth
            # Test auth system by checking if role manager is initialized
            auth_status = auth.role_manager._cache_initialized
            health_results['components']['authentication'] = {
                'status': 'healthy' if auth_status else 'warning',
                'details': 'Authentication system initialized' if auth_status else 'Authentication cache not initialized'
            }
            if not auth_status:
                health_results['warnings'].append('Authentication system cache not initialized')
        except Exception as e:
            health_results['components']['authentication'] = {
                'status': 'error',
                'details': f'Error checking auth system: {e}'
            }
            health_results['errors'].append(f'Authentication error: {e}')
        
        # Check memory management system
        try:
            import memory_management
            memory_status = memory_management.get_scheduler_status()
            scheduler_healthy = memory_status.get('is_running', False) or not memory_status.get('scheduler_available', False)
            
            health_results['components']['memory_management'] = {
                'status': 'healthy' if scheduler_healthy else 'warning',
                'details': memory_status,
                'scheduler_running': memory_status.get('is_running', False),
                'scheduler_available': memory_status.get('scheduler_available', False)
            }
            
            if not scheduler_healthy:
                health_results['warnings'].append('Memory management scheduler not running')
        except Exception as e:
            health_results['components']['memory_management'] = {
                'status': 'error',
                'details': f'Error checking memory management: {e}'
            }
            health_results['errors'].append(f'Memory management error: {e}')
        
        # Check environment variables
        required_env_vars = ['TELEGRAM_BOT_TOKEN']
        optional_env_vars = ['LOG_LEVEL', 'LOG_FILE']
        
        env_status = {'required': {}, 'optional': {}}
        
        for var in required_env_vars:
            value = os.getenv(var)
            env_status['required'][var] = {
                'present': value is not None,
                'length': len(value) if value else 0
            }
            if not value:
                health_results['errors'].append(f'Required environment variable {var} not set')
        
        for var in optional_env_vars:
            value = os.getenv(var)
            env_status['optional'][var] = {
                'present': value is not None,
                'value': value if value else 'not set'
            }
        
        health_results['components']['environment'] = {
            'status': 'healthy' if all(env['present'] for env in env_status['required'].values()) else 'error',
            'details': env_status
        }
        
        # Check file system permissions
        try:
            import tempfile
            test_file = tempfile.NamedTemporaryFile(delete=True)
            test_file.close()
            
            health_results['components']['filesystem'] = {
                'status': 'healthy',
                'details': 'File system write permissions OK'
            }
        except Exception as e:
            health_results['components']['filesystem'] = {
                'status': 'error',
                'details': f'File system permission error: {e}'
            }
            health_results['errors'].append(f'File system error: {e}')
        
        # Determine overall status
        component_statuses = [comp['status'] for comp in health_results['components'].values()]
        
        if 'error' in component_statuses:
            health_results['overall_status'] = 'unhealthy'
        elif 'warning' in component_statuses or health_results['warnings']:
            health_results['overall_status'] = 'degraded'
        else:
            health_results['overall_status'] = 'healthy'
        
        # Log health check results
        logger.info(f"Health check completed: {health_results['overall_status']}")
        if health_results['errors']:
            logger.error(f"Health check errors: {health_results['errors']}")
        if health_results['warnings']:
            logger.warning(f"Health check warnings: {health_results['warnings']}")
        
        return health_results
        
    except Exception as e:
        logger.error(f"Error during health check: {e}")
        health_results['overall_status'] = 'error'
        health_results['errors'].append(f'Health check system error: {e}')
        return health_results


async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Global error handler for the bot
    
    Args:
        update: The update that caused the error
        context: The context object
    """
    logger.error(f"Exception while handling an update: {context.error}")
    
    # Get user ID if available
    user_id = None
    if isinstance(update, Update) and update.effective_user:
        user_id = update.effective_user.id
    
    # Handle the error using centralized error handler
    error_message = error_handler.handle_telegram_error(
        context.error, 
        "bot_operation", 
        user_id
    )
    
    # Try to send error message to user if possible
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                f"🤖 **Bot Error**\n\n{error_message}\n\n"
                "If this problem persists, please contact support.",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Failed to send error message to user: {e}")
    
    # Log system event
    log_system_event("bot_error", f"Error in update handling: {context.error}", "ERROR")


def setup_handlers(application: Application) -> None:
    """
    Register all conversation handlers and commands.
    
    This function will register:
    - Admin handlers (/check, /setting)
    - Sales handlers (/register, /kpi, /submitkpi, /submitsale)
    - Error handlers
    """
    try:
        # Register global error handler first
        application.add_error_handler(global_error_handler)
        logger.info("Global error handler registered")
        
        # Import and register admin handlers
        try:
            import admin
            admin_handlers = admin.get_admin_handlers()
            for handler in admin_handlers:
                application.add_handler(handler)
            logger.info(f"Registered {len(admin_handlers)} admin handlers")
            log_system_event("admin_handlers", f"Registered {len(admin_handlers)} admin handlers")
        except Exception as e:
            logger.error(f"Failed to register admin handlers: {e}")
            log_system_event("admin_handlers_failed", f"Admin handler registration failed: {e}", "ERROR")
            raise
        
        # Import and register sales handlers
        try:
            import sales
            sales_handlers = sales.get_sales_handlers()
            for handler in sales_handlers:
                application.add_handler(handler)
            logger.info(f"Registered {len(sales_handlers)} sales handlers")
            log_system_event("sales_handlers", f"Registered {len(sales_handlers)} sales handlers")
        except Exception as e:
            logger.error(f"Failed to register sales handlers: {e}")
            log_system_event("sales_handlers_failed", f"Sales handler registration failed: {e}", "ERROR")
            raise
        
        logger.info("All handlers setup completed successfully")
        log_system_event("handlers_setup", "All bot handlers registered successfully")
        
    except Exception as e:
        logger.error(f"Failed to setup handlers: {e}")
        log_system_event("handlers_setup_failed", f"Handler setup failed: {e}", "ERROR")
        raise


def main() -> None:
    """
    Initialize and start the Telegram bot.
    
    This function:
    1. Performs system health check
    2. Initializes the authentication system
    3. Creates the Application instance
    4. Sets up all handlers
    5. Starts the bot
    6. Handles graceful shutdown
    """
    try:
        logger.info("Starting Telegram KPI Bot initialization...")
        log_system_event("bot_startup", "Bot initialization started")
        
        # Perform comprehensive system health check
        logger.info("Performing comprehensive system health check...")
        health_results = comprehensive_health_check()
        logger.info(f"System health check completed: {health_results['overall_status']}")
        
        # Log health check results
        log_system_event("health_check_complete", f"Health status: {health_results['overall_status']}")
        
        # Handle unhealthy status
        if health_results['overall_status'] == 'unhealthy':
            logger.error("System health check failed. Critical components are not functioning.")
            logger.error(f"Errors: {health_results['errors']}")
            log_system_event("health_check_failed", f"Critical errors: {health_results['errors']}", "ERROR")
            
            # Check if we can continue with limited functionality
            google_sheets_ok = health_results['components'].get('google_sheets', {}).get('status') == 'healthy'
            google_drive_ok = health_results['components'].get('google_drive', {}).get('status') == 'healthy'
            
            if not google_sheets_ok and not google_drive_ok:
                logger.error("Both Google services are unavailable. Cannot continue.")
                log_system_event("startup_aborted", "Both Google services unavailable", "CRITICAL")
                return
            else:
                logger.warning("Some services are unavailable but continuing with limited functionality.")
                log_system_event("limited_functionality", "Starting with limited functionality", "WARNING")
        
        elif health_results['overall_status'] == 'degraded':
            logger.warning("System health check shows degraded performance.")
            logger.warning(f"Warnings: {health_results['warnings']}")
            log_system_event("health_check_degraded", f"Warnings: {health_results['warnings']}", "WARNING")
        
        # Initialize authentication system
        logger.info("Initializing authentication system...")
        if not auth.initialize_auth_system():
            error_msg = "Failed to initialize authentication system"
            logger.error(error_msg)
            log_system_event("auth_init_failed", error_msg, "ERROR")
            return
        
        log_system_event("auth_initialized", "Authentication system initialized successfully")
        
        # Get bot token from environment variable
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        if not bot_token:
            error_msg = "TELEGRAM_BOT_TOKEN environment variable not set"
            logger.error(error_msg)
            log_system_event("config_error", error_msg, "ERROR")
            return
        
        # Create Application instance
        logger.info("Creating Telegram Application instance...")
        application = Application.builder().token(bot_token).build()
        log_system_event("app_created", "Telegram Application instance created")
        
        # Setup all handlers
        logger.info("Setting up bot handlers...")
        setup_handlers(application)
        
        # Initialize scheduled tasks for memory management
        logger.info("Initializing memory management system...")
        try:
            import memory_management
            
            # Setup scheduled memory cleanup
            if memory_management.setup_scheduled_cleanup(
                scheduler_type='background',
                cleanup_interval_minutes=15
            ):
                # Start the scheduled cleanup
                if memory_management.start_scheduled_cleanup():
                    logger.info("Memory management scheduled cleanup started successfully")
                    log_system_event("memory_management", "Scheduled cleanup started (15-minute intervals)")
                else:
                    logger.warning("Failed to start memory management scheduled cleanup")
                    log_system_event("memory_management_warning", "Failed to start scheduled cleanup", "WARNING")
            else:
                logger.warning("Failed to setup memory management scheduler")
                log_system_event("memory_management_warning", "Failed to setup scheduler", "WARNING")
                
        except Exception as e:
            logger.error(f"Error initializing memory management: {e}")
            log_system_event("memory_management_error", f"Memory management initialization failed: {e}", "ERROR")
        
        logger.info("Starting Telegram KPI Bot polling...")
        log_system_event("bot_started", "Bot started and listening for updates")
        
        # Start the bot
        application.run_polling(
            allowed_updates=['message', 'callback_query'],
            drop_pending_updates=True
        )
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (Ctrl+C)")
        log_system_event("bot_stopped", "Bot stopped by user interrupt")
    except Exception as e:
        error_msg = f"Bot encountered a critical error: {e}"
        logger.error(error_msg)
        log_system_event("bot_critical_error", error_msg, "CRITICAL")
        
        # Handle the error using centralized error handler
        error_handler.handle_application_error(e, "bot_startup", "system_error")
        
    finally:
        # Perform graceful shutdown
        perform_graceful_shutdown()
        
        logger.info("Bot shutdown completed")
        log_system_event("bot_shutdown", "Bot shutdown completed")
        
        # Print final error statistics
        error_stats = error_handler.get_error_statistics()
        if error_stats:
            logger.info(f"Final error statistics: {error_stats}")
        else:
            logger.info("No errors recorded during this session")


if __name__ == '__main__':
    main()