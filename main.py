#!/usr/bin/env python3
"""
Telegram KPI Bot - Main Entry Point

This is the main application entry point for the Telegram KPI Bot.
It initializes the bot, sets up handlers, and manages the application lifecycle.
"""

import logging
import os
import threading
from datetime import datetime
from telegram.ext import Application
from telegram import Update
from telegram.ext import ContextTypes
from dotenv import load_dotenv
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from typing import Dict, Any, Optional
import fcntl
import sys
import atexit
import signal

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

# Global variables for process management
lock_file = None
application_instance = None


def acquire_process_lock() -> bool:
    """
    Acquire a process lock to prevent multiple instances from running.
    
    Returns:
        bool: True if lock acquired successfully, False otherwise
    """
    global lock_file
    
    try:
        # Create lock file path
        lock_file_path = os.path.join(os.getcwd(), 'kpi_bot.lock')
        
        # Try to open and lock the file
        lock_file = open(lock_file_path, 'w')
        
        # For Windows compatibility, use a different approach
        if os.name == 'nt':  # Windows
            try:
                # Try to write PID to lock file
                lock_file.write(str(os.getpid()))
                lock_file.flush()
                logger.info(f"✅ Process lock acquired (PID: {os.getpid()})")
                log_system_event("process_lock_acquired", f"Lock acquired by PID {os.getpid()}")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to acquire process lock: {e}")
                if lock_file:
                    lock_file.close()
                    lock_file = None
                return False
        else:  # Unix-like systems
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_file.write(str(os.getpid()))
                lock_file.flush()
                logger.info(f"✅ Process lock acquired (PID: {os.getpid()})")
                log_system_event("process_lock_acquired", f"Lock acquired by PID {os.getpid()}")
                return True
            except (IOError, OSError) as e:
                logger.error(f"❌ Another instance is already running: {e}")
                logger.error("❌ Cannot start multiple instances of the bot")
                if lock_file:
                    lock_file.close()
                    lock_file = None
                return False
                
    except Exception as e:
        logger.error(f"❌ Error acquiring process lock: {e}")
        if lock_file:
            lock_file.close()
            lock_file = None
        return False


def release_process_lock() -> None:
    """
    Release the process lock.
    """
    global lock_file
    
    try:
        if lock_file:
            if os.name != 'nt':  # Unix-like systems
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
            lock_file = None
            
            # Remove lock file
            lock_file_path = os.path.join(os.getcwd(), 'kpi_bot.lock')
            if os.path.exists(lock_file_path):
                os.remove(lock_file_path)
                
            logger.info("✅ Process lock released")
            log_system_event("process_lock_released", "Process lock released successfully")
            
    except Exception as e:
        logger.error(f"❌ Error releasing process lock: {e}")


def signal_handler(signum, frame):
    """
    Handle system signals for graceful shutdown.
    """
    logger.info(f"🛑 Received signal {signum}, initiating graceful shutdown...")
    log_system_event("signal_received", f"Signal {signum} received, shutting down")
    
    # Stop the application if it exists
    global application_instance
    if application_instance:
        try:
            application_instance.stop_running()
            logger.info("✅ Application stopped successfully")
        except Exception as e:
            logger.error(f"❌ Error stopping application: {e}")
    
    # Perform graceful shutdown
    perform_graceful_shutdown()
    
    # Release process lock
    release_process_lock()
    
    # Exit
    sys.exit(0)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks and status"""
    
    def do_GET(self):
        """Handle GET requests"""
        try:
            if self.path == '/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                
                html_content = """
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Telegram KPI Bot</title>
                    <style>
                        body { font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }
                        .container { max-width: 600px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                        .status { padding: 15px; border-radius: 5px; margin: 20px 0; }
                        .running { background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
                        .info { background-color: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }
                        h1 { color: #333; text-align: center; }
                        .emoji { font-size: 1.2em; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1><span class="emoji">🤖</span> Telegram KPI Bot</h1>
                        <div class="status running">
                            <strong>✅ Status:</strong> Bot is running and listening for updates
                        </div>
                        <div class="info">
                            <strong>📊 Function:</strong> KPI tracking and management for sales teams
                        </div>
                        <div class="info">
                            <strong>🔧 Features:</strong>
                            <ul>
                                <li>Sales representative registration</li>
                                <li>KPI target setting and tracking</li>
                                <li>Photo upload to Google Drive</li>
                                <li>Data storage in Google Sheets</li>
                                <li>Admin and sales role management</li>
                            </ul>
                        </div>
                        <div class="info">
                            <strong>🚀 Deployment:</strong> Running on Render Web Service
                        </div>
                    </div>
                </body>
                </html>
                """
                self.wfile.write(html_content.encode())
                
            elif self.path == '/health':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                health_data = {
                    "status": "healthy",
                    "service": "Telegram KPI Bot",
                    "timestamp": datetime.now().isoformat(),
                    "uptime": "running"
                }
                self.wfile.write(json.dumps(health_data).encode())
                
            elif self.path == '/status':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                
                status_data = {
                    "bot_status": "running",
                    "service_type": "telegram_bot",
                    "features": [
                        "sales_registration",
                        "kpi_tracking", 
                        "photo_upload",
                        "admin_management"
                    ],
                    "timestamp": datetime.now().isoformat()
                }
                self.wfile.write(json.dumps(status_data).encode())
                
            else:
                self.send_response(404)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Not Found')
                
        except Exception as e:
            logger.error(f"HTTP handler error: {e}")
            self.send_response(500)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Internal Server Error')
    
    def log_message(self, format, *args):
        """Override to use our logger instead of stderr"""
        logger.info(f"HTTP: {format % args}")


def start_http_server(port: int = 10000):
    """Start HTTP server in a separate thread"""
    try:
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        logger.info(f"HTTP server starting on port {port}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"HTTP server error: {e}")


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


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command"""
    try:
        user = update.effective_user
        user_id = user.id
        user_name = user.first_name or "User"
        
        logger.info(f"User {user_id} ({user_name}) started the bot")
        
        # Check user role
        import auth
        
        # Debug logging for admin check
        logger.info(f"Checking admin status for user {user_id}")
        logger.info(f"Admin cache initialized: {auth.role_manager._cache_initialized}")
        logger.info(f"Admin cache contents: {list(auth.role_manager._admin_cache)}")
        
        is_admin = auth.is_admin(user_id)
        user_role = auth.get_user_role(user_id)
        
        logger.info(f"User {user_id} - is_admin: {is_admin}, role: {user_role}")
        
        if user_role == 'admin':
            welcome_message = f"""
🎉 **欢迎管理员 {user_name}！**

**管理员专属命令：**
📊 /check - 查看销售代表的KPI完成情况
🎯 /setting - 为销售代表设置月度KPI目标

**销售代表功能（管理员也可使用）：**
📝 /register - 注册个人信息
📈 /kpi - 查看个人KPI进度
🤝 /submitkpi - 提交会面记录和照片
💰 /submitsale - 提交销售记录和照片

**通用命令：**
❓ /help - 获取帮助信息

您的管理员权限已激活！
"""
        else:
            welcome_message = f"""
👋 **欢迎 {user_name}！**

我是KPI跟踪机器人，可以帮助您：

**销售代表功能：**
📝 /register - 注册个人信息
📈 /kpi - 查看个人KPI进度
🤝 /submitkpi - 提交会面记录和照片
💰 /submitsale - 提交销售记录和照片

**通用命令：**
❓ /help - 获取帮助信息

请先使用 /register 注册您的信息！
"""
        
        await update.message.reply_text(welcome_message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        await update.message.reply_text("❌ 启动时出现错误，请稍后重试。")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help command"""
    try:
        user = update.effective_user
        user_id = user.id
        
        import auth
        user_role = auth.get_user_role(user_id)
        
        if user_role == 'admin':
            help_message = """
🔧 **管理员帮助**

**管理员专属命令：**
📊 /check - 查看销售代表KPI完成情况
🎯 /setting - 为销售代表设置月度KPI目标

**销售代表功能（管理员也可使用）：**
📝 /register - 注册个人信息
📈 /kpi - 查看个人KPI进度
🤝 /submitkpi - 提交会面记录和照片
💰 /submitsale - 提交销售记录和照片

**使用说明：**
• 管理员命令：
  1. 使用 /check 选择销售代表查看其KPI完成情况
  2. 使用 /setting 为销售代表设置月度目标
• 销售功能：管理员也可以像普通用户一样注册和提交KPI
• 所有数据自动保存到Google Sheets
"""
        else:
            help_message = """
📱 **销售代表帮助**

**可用命令：**
📝 /register - 注册个人信息
📈 /kpi - 查看KPI进度
🤝 /submitkpi - 提交会面记录
💰 /submitsale - 提交销售记录

**使用流程：**
1. 首先使用 /register 注册
2. 使用 /kpi 查看当前进度
3. 使用 /submitkpi 提交会面照片
4. 使用 /submitsale 提交销售照片
"""
        
        await update.message.reply_text(help_message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error in help command: {e}")
        await update.message.reply_text("❌ 获取帮助时出现错误。")


async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Global error handler for the bot
    
    Args:
        update: The update that caused the error
        context: The context object
    """
    error_str = str(context.error)
    logger.error(f"Exception while handling an update: {error_str}")
    
    # Special handling for conflict errors
    if 'conflict' in error_str.lower() and 'getUpdates' in error_str:
        logger.error("🚨 CRITICAL: Bot conflict detected!")
        logger.error("🚨 Another instance is running with the same token!")
        logger.error("🚨 This service will attempt to continue but may be unstable.")
        
        # Log critical system event
        log_system_event("bot_conflict_critical", f"Multiple instances detected: {error_str}", "CRITICAL")
        
        # Don't try to send message to user for conflict errors
        return
    
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
        
        # Register basic commands
        from telegram.ext import CommandHandler
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("help", help_command))
        logger.info("Basic commands registered (/start, /help)")
        
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


def verify_bot_identity_and_clear_webhook(bot_token: str) -> bool:
    """
    验证Bot身份并清理Webhook设置（非阻塞版本）
    
    Args:
        bot_token (str): Telegram Bot Token
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import requests
        
        # 显示Token指纹
        token_fingerprint = f"{bot_token[:10]}...{bot_token[-5:]}"
        logger.info(f"🤖 Bot Token 指纹: {token_fingerprint}")
        
        # 获取Bot信息 - 使用更短的超时时间
        logger.info("🔍 验证Bot身份...")
        try:
            response = requests.get(f"https://api.telegram.org/bot{bot_token}/getMe", timeout=5)
            if response.status_code == 200:
                bot_info = response.json()
                if bot_info['ok']:
                    bot_data = bot_info['result']
                    logger.info(f"✅ Bot 身份验证成功:")
                    logger.info(f"   - Bot 名称: {bot_data['first_name']}")
                    logger.info(f"   - Bot 用户名: @{bot_data['username']}")
                    logger.info(f"   - Bot ID: {bot_data['id']}")
                    log_system_event("bot_identity_verified", f"Bot @{bot_data['username']} (ID: {bot_data['id']}) verified")
                else:
                    logger.warning(f"⚠️  Bot身份验证响应异常: {bot_info['description']}")
                    logger.info("继续启动，稍后重试验证...")
            else:
                logger.warning(f"⚠️  Bot身份验证HTTP错误: {response.status_code}")
                logger.info("继续启动，稍后重试验证...")
        except requests.exceptions.Timeout:
            logger.warning("⚠️  Bot身份验证超时，继续启动...")
        except Exception as e:
            logger.warning(f"⚠️  Bot身份验证失败: {e}，继续启动...")
        
        # 检查并清理Webhook - 使用更短的超时时间
        logger.info("🔍 检查Webhook状态...")
        try:
            webhook_response = requests.get(f"https://api.telegram.org/bot{bot_token}/getWebhookInfo", timeout=5)
            if webhook_response.status_code == 200:
                webhook_info = webhook_response.json()
                if webhook_info['ok']:
                    webhook_data = webhook_info['result']
                    webhook_url = webhook_data.get('url', '')
                    
                    if webhook_url:
                        logger.warning(f"⚠️  发现Webhook设置: {webhook_url}")
                        logger.warning(f"📊 待处理更新数: {webhook_data.get('pending_update_count', 0)}")
                        
                        # 删除Webhook以使用Polling模式
                        logger.info("🗑️  删除Webhook以启用Polling模式...")
                        try:
                            delete_response = requests.post(f"https://api.telegram.org/bot{bot_token}/deleteWebhook", timeout=5)
                            if delete_response.status_code == 200:
                                delete_result = delete_response.json()
                                if delete_result['ok']:
                                    logger.info("✅ Webhook已成功删除，可以使用Polling模式")
                                    log_system_event("webhook_cleared", "Webhook deleted for polling mode")
                                else:
                                    logger.warning(f"⚠️  删除Webhook响应异常: {delete_result['description']}")
                            else:
                                logger.warning(f"⚠️  删除Webhook HTTP错误: {delete_response.status_code}")
                        except requests.exceptions.Timeout:
                            logger.warning("⚠️  删除Webhook超时，继续启动...")
                        except Exception as e:
                            logger.warning(f"⚠️  删除Webhook失败: {e}，继续启动...")
                    else:
                        logger.info("✅ 没有设置Webhook，可以安全使用Polling模式")
                        log_system_event("webhook_status", "No webhook set, polling mode ready")
                else:
                    logger.warning(f"⚠️  获取Webhook信息响应异常: {webhook_info['description']}")
            else:
                logger.warning(f"⚠️  Webhook检查HTTP错误: {webhook_response.status_code}")
        except requests.exceptions.Timeout:
            logger.warning("⚠️  Webhook检查超时，继续启动...")
        except Exception as e:
            logger.warning(f"⚠️  Webhook检查失败: {e}，继续启动...")
        
        # 即使网络请求失败，也继续启动
        logger.info("✅ Bot验证和Webhook检查完成（可能有警告），继续启动...")
        return True
        
    except Exception as e:
        logger.error(f"❌ Bot验证过程出现严重错误: {e}")
        # 即使出错也继续启动，避免部署卡住
        logger.warning("⚠️  忽略验证错误，继续启动...")
        return True


def main() -> None:
    """
    Initialize and start the Telegram bot.
    
    This function:
    1. Acquires process lock to prevent multiple instances
    2. Verifies bot identity and clears webhook
    3. Performs system health check
    4. Initializes the authentication system
    5. Creates the Application instance
    6. Sets up all handlers
    7. Starts the bot
    8. Handles graceful shutdown
    """
    global application_instance
    
    try:
        logger.info("🚀 Starting Telegram KPI Bot initialization...")
        log_system_event("bot_startup", "Bot initialization started")
        
        # Step 0: Acquire process lock to prevent multiple instances
        logger.info("🔒 Step 0: Acquiring process lock...")
        if not acquire_process_lock():
            logger.error("❌ Failed to acquire process lock. Another instance may be running.")
            logger.error("❌ Exiting to prevent conflicts...")
            log_system_event("startup_aborted", "Process lock acquisition failed", "CRITICAL")
            return
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        logger.info("✅ Signal handlers registered")
        
        # Register cleanup function to run on exit
        atexit.register(release_process_lock)
        
        # Get bot token first
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        if not bot_token:
            error_msg = "TELEGRAM_BOT_TOKEN environment variable not set"
            logger.error(error_msg)
            log_system_event("config_error", error_msg, "ERROR")
            return
        
        # Step 1: Verify bot identity and clear webhook
        logger.info("🔐 Step 1: Verifying bot identity and clearing webhook...")
        if not verify_bot_identity_and_clear_webhook(bot_token):
            error_msg = "Failed to verify bot identity or clear webhook"
            logger.error(error_msg)
            log_system_event("bot_verification_failed", error_msg, "ERROR")
            # Continue anyway to avoid deployment failures
            logger.warning("⚠️  Continuing despite verification failure...")
        
        # Additional conflict prevention
        logger.info("🛡️  Step 1.5: Additional conflict prevention...")
        try:
            import requests
            # Force delete webhook multiple times to ensure it's gone
            for i in range(3):
                delete_response = requests.post(f"https://api.telegram.org/bot{bot_token}/deleteWebhook", timeout=3)
                if delete_response.status_code == 200:
                    logger.info(f"✅ Webhook deletion attempt {i+1}/3 successful")
                else:
                    logger.warning(f"⚠️  Webhook deletion attempt {i+1}/3 failed")
                import time
                time.sleep(1)
        except Exception as e:
            logger.warning(f"⚠️  Additional webhook cleanup failed: {e}")
        
        # Initialize authentication system
        logger.info("🔧 Step 2: Initializing authentication system...")
        if not auth.initialize_auth_system():
            error_msg = "Failed to initialize authentication system"
            logger.error(error_msg)
            log_system_event("auth_init_failed", error_msg, "ERROR")
            return
        
        log_system_event("auth_initialized", "Authentication system initialized successfully")
        
        # Initialize Google APIs
        logger.info("Initializing Google APIs...")
        
        # Initialize Google Sheets
        import google_sheets
        if not google_sheets.authenticate_google_sheets():
            error_msg = "Failed to authenticate Google Sheets"
            logger.error(error_msg)
            log_system_event("google_sheets_auth_failed", error_msg, "ERROR")
            # Don't return here, continue with limited functionality
        else:
            logger.info("Google Sheets authentication successful")
            log_system_event("google_sheets_authenticated", "Google Sheets initialized successfully")
        
        # Initialize Google Drive
        import google_drive
        if not google_drive.authenticate_google_drive():
            error_msg = "Failed to authenticate Google Drive"
            logger.error(error_msg)
            log_system_event("google_drive_auth_failed", error_msg, "ERROR")
            # Don't return here, continue with limited functionality
        else:
            logger.info("Google Drive authentication successful")
            log_system_event("google_drive_authenticated", "Google Drive initialized successfully")
        
        # Now perform comprehensive system health check
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
        
        # Create Application instance
        logger.info("🏗️  Step 6: Creating Telegram Application instance...")
        application = Application.builder().token(bot_token).build()
        application_instance = application  # Store globally for signal handler
        log_system_event("app_created", "Telegram Application instance created")
        logger.info("✅ Application instance created successfully")
        
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
        
        # Start HTTP server for Render Web Service
        port = int(os.getenv('PORT', 10000))
        logger.info(f"Starting HTTP server on port {port}")
        http_thread = threading.Thread(target=start_http_server, args=(port,), daemon=True)
        http_thread.start()
        log_system_event("http_server_started", f"HTTP server started on port {port}")
        
        # Final safety check before starting polling (non-blocking)
        logger.info("🔍 Final safety check before starting polling...")
        
        # Verify no webhook is set one more time (with short timeout)
        try:
            import requests
            webhook_check = requests.get(f"https://api.telegram.org/bot{bot_token}/getWebhookInfo", timeout=3)
            if webhook_check.status_code == 200:
                webhook_info = webhook_check.json()
                if webhook_info['ok'] and webhook_info['result'].get('url'):
                    logger.warning("⚠️  WARNING: Webhook still set! This may cause conflicts!")
                    logger.warning("⚠️  Continuing with polling anyway...")
                    log_system_event("polling_warning", "Webhook still active but continuing", "WARNING")
                else:
                    logger.info("✅ Webhook check passed, safe to start polling")
        except requests.exceptions.Timeout:
            logger.warning("⚠️  Webhook check timeout, continuing with polling...")
        except Exception as e:
            logger.warning(f"⚠️  Could not verify webhook status: {e}, continuing anyway...")
        
        logger.info("🚀 Starting Telegram KPI Bot polling...")
        logger.info("📡 Polling configuration:")
        logger.info("   - Allowed updates: ['message', 'callback_query']")
        logger.info("   - Drop pending updates: True")
        logger.info("   - Mode: Long Polling (getUpdates)")
        log_system_event("bot_started", "Bot started and listening for updates via polling")
        
        # Start the bot with polling (with conflict handling)
        try:
            application.run_polling(
                allowed_updates=['message', 'callback_query'],
                drop_pending_updates=True
            )
        except Exception as polling_error:
            if 'conflict' in str(polling_error).lower():
                logger.error("🚨 Polling conflict detected! Another instance may be running.")
                logger.error("Waiting 30 seconds before retrying...")
                import time
                time.sleep(30)
                
                # Try to clear any webhook and retry once
                try:
                    import requests
                    requests.post(f"https://api.telegram.org/bot{bot_token}/deleteWebhook", timeout=5)
                    logger.info("Cleared webhook and retrying polling...")
                    application.run_polling(
                        allowed_updates=['message', 'callback_query'],
                        drop_pending_updates=True
                    )
                except Exception as retry_error:
                    logger.error(f"Retry failed: {retry_error}")
                    raise
            else:
                raise
        
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
        
        # Release process lock
        release_process_lock()
        
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
