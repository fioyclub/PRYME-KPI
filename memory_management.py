"""
Memory Management Module

This module provides comprehensive memory management functionality including:
- Immediate memory cleanup functions for file operations
- Temporary file deletion utilities
- Garbage collection management
- Conversation context cleanup
- Scheduled periodic cleanup
"""

import gc
import os
import tempfile
import logging
import weakref
import atexit
from typing import Optional, Any, List, Dict
from datetime import datetime
from io import BytesIO
from pathlib import Path

# Try to import APScheduler, gracefully handle if not available
try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.schedulers.background import BackgroundScheduler
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False
    # Create dummy classes for type hints when APScheduler is not available
    class AsyncIOScheduler:
        def __init__(self):
            pass
        def add_job(self, *args, **kwargs):
            pass
        def start(self):
            pass
        def shutdown(self):
            pass
    
    class BackgroundScheduler:
        def __init__(self):
            pass
        def add_job(self, *args, **kwargs):
            pass
        def start(self):
            pass
        def shutdown(self):
            pass

# Configure logging
logger = logging.getLogger(__name__)

# Global registry for tracking objects that need cleanup
_cleanup_registry: Dict[str, Any] = {
    'file_streams': weakref.WeakSet(),
    'temp_files': set(),  # Use regular set for strings (file paths)
    'conversation_contexts': weakref.WeakSet()
}

class MemoryManager:
    """Central memory management class for handling cleanup operations"""
    
    def __init__(self):
        self.cleanup_stats = {
            'file_cleanups': 0,
            'temp_file_cleanups': 0,
            'gc_collections': 0,
            'conversation_cleanups': 0,
            'last_cleanup': None
        }
    
    def get_cleanup_stats(self) -> Dict[str, Any]:
        """Get current cleanup statistics"""
        return self.cleanup_stats.copy()
    
    def reset_stats(self):
        """Reset cleanup statistics"""
        self.cleanup_stats = {
            'file_cleanups': 0,
            'temp_file_cleanups': 0,
            'gc_collections': 0,
            'conversation_cleanups': 0,
            'last_cleanup': None
        }
        logger.info("[MEMORY] Cleanup statistics reset")

# Global memory manager instance
memory_manager = MemoryManager()

def release_file_memory(file_data: Any, file_stream: Optional[BytesIO] = None, 
                       temp_file_path: Optional[str] = None) -> bool:
    """
    Safely release file-related memory after successful operations
    
    Args:
        file_data: File data to be released (bytes, bytearray, etc.)
        file_stream: Optional file stream to be closed and released
        temp_file_path: Optional temporary file path to be deleted
        
    Returns:
        bool: True if cleanup was successful, False otherwise
    """
    try:
        cleanup_success = True
        
        # Close and clean up file stream
        if file_stream:
            try:
                if hasattr(file_stream, 'close') and not file_stream.closed:
                    file_stream.close()
                    logger.info("[MEMORY] Closed file stream")
                
                # Remove from registry if it was tracked
                _cleanup_registry['file_streams'].discard(file_stream)
                
                # Delete reference
                del file_stream
                logger.info("[MEMORY] Deleted file stream reference")
                
            except Exception as e:
                logger.error(f"[MEMORY] Error closing file stream: {e}")
                cleanup_success = False
        
        # Delete temporary file if provided
        if temp_file_path:
            try:
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
                    logger.info(f"[MEMORY] Deleted temporary file: {temp_file_path}")
                    memory_manager.cleanup_stats['temp_file_cleanups'] += 1
            except Exception as e:
                logger.error(f"[MEMORY] Error deleting temp file {temp_file_path}: {e}")
                cleanup_success = False
        
        # Delete file data reference
        if file_data is not None:
            try:
                del file_data
                logger.info("[MEMORY] Deleted file data reference")
                memory_manager.cleanup_stats['file_cleanups'] += 1
            except Exception as e:
                logger.error(f"[MEMORY] Error deleting file data: {e}")
                cleanup_success = False
        
        # Force garbage collection
        collected = gc.collect()
        memory_manager.cleanup_stats['gc_collections'] += 1
        memory_manager.cleanup_stats['last_cleanup'] = datetime.now()
        
        logger.info(f"[MEMORY] Forced garbage collection: collected {collected} objects")
        
        return cleanup_success
        
    except Exception as e:
        logger.error(f"[MEMORY] Critical error during memory cleanup: {e}")
        return False

def register_file_stream(file_stream: BytesIO) -> BytesIO:
    """
    Register a file stream for tracking and cleanup
    
    Args:
        file_stream: File stream to register
        
    Returns:
        BytesIO: The same file stream (for chaining)
    """
    try:
        _cleanup_registry['file_streams'].add(file_stream)
        logger.debug(f"[MEMORY] Registered file stream for tracking")
        return file_stream
    except Exception as e:
        logger.error(f"[MEMORY] Error registering file stream: {e}")
        return file_stream

def cleanup_temp_files(temp_dir: Optional[str] = None) -> int:
    """
    Remove temporary files and release memory
    
    Args:
        temp_dir: Optional specific temporary directory to clean
        
    Returns:
        int: Number of files cleaned up
    """
    cleaned_count = 0
    
    try:
        # Use system temp directory if none specified
        if temp_dir is None:
            temp_dir = tempfile.gettempdir()
        
        logger.info(f"[MEMORY] Starting temp file cleanup in: {temp_dir}")
        
        # Look for files that might be related to our bot
        temp_path = Path(temp_dir)
        if not temp_path.exists():
            logger.warning(f"[MEMORY] Temp directory does not exist: {temp_dir}")
            return 0
        
        # Clean up files with specific patterns that might be from our bot
        patterns = [
            "telegram_*",
            "kpi_bot_*",
            "photo_upload_*",
            "*.tmp"
        ]
        
        for pattern in patterns:
            try:
                for file_path in temp_path.glob(pattern):
                    if file_path.is_file():
                        # Check if file is older than 1 hour to avoid deleting active files
                        file_age = datetime.now().timestamp() - file_path.stat().st_mtime
                        if file_age > 3600:  # 1 hour in seconds
                            file_path.unlink()
                            cleaned_count += 1
                            logger.debug(f"[MEMORY] Deleted old temp file: {file_path}")
            except Exception as e:
                logger.error(f"[MEMORY] Error cleaning pattern {pattern}: {e}")
        
        # Force garbage collection after cleanup
        collected = gc.collect()
        memory_manager.cleanup_stats['temp_file_cleanups'] += cleaned_count
        memory_manager.cleanup_stats['gc_collections'] += 1
        
        logger.info(f"[MEMORY] Temp file cleanup completed: {cleaned_count} files removed, {collected} objects collected")
        
        return cleaned_count
        
    except Exception as e:
        logger.error(f"[MEMORY] Error during temp file cleanup: {e}")
        return cleaned_count

def force_garbage_collection() -> int:
    """
    Force garbage collection and return number of collected objects
    
    Returns:
        int: Number of objects collected
    """
    try:
        # Run garbage collection multiple times for thorough cleanup
        collected_total = 0
        
        for i in range(3):  # Run 3 cycles
            collected = gc.collect()
            collected_total += collected
            logger.debug(f"[MEMORY] GC cycle {i+1}: collected {collected} objects")
        
        memory_manager.cleanup_stats['gc_collections'] += 1
        memory_manager.cleanup_stats['last_cleanup'] = datetime.now()
        
        logger.info(f"[MEMORY] Forced garbage collection completed: {collected_total} total objects collected")
        
        return collected_total
        
    except Exception as e:
        logger.error(f"[MEMORY] Error during garbage collection: {e}")
        return 0

def cleanup_conversation_context(context_data: Any) -> bool:
    """
    Clean up conversation context data
    
    Args:
        context_data: Conversation context data to clean up
        
    Returns:
        bool: True if cleanup was successful
    """
    try:
        if context_data is None:
            return True
        
        # Clear context data if it's a dictionary
        if isinstance(context_data, dict):
            context_data.clear()
            logger.info("[MEMORY] Cleared conversation context dictionary")
        
        # Delete reference
        del context_data
        
        # Force garbage collection
        collected = gc.collect()
        memory_manager.cleanup_stats['conversation_cleanups'] += 1
        memory_manager.cleanup_stats['gc_collections'] += 1
        
        logger.info(f"[MEMORY] Conversation context cleanup completed: {collected} objects collected")
        
        return True
        
    except Exception as e:
        logger.error(f"[MEMORY] Error during conversation context cleanup: {e}")
        return False

def get_memory_usage_info() -> Dict[str, Any]:
    """
    Get current memory usage information
    
    Returns:
        Dict: Memory usage statistics
    """
    try:
        try:
            import psutil
            
            # Get process memory info
            process = psutil.Process()
            memory_info = process.memory_info()
            
            # Get garbage collection stats
            gc_stats = gc.get_stats()
            
            info = {
                'rss_memory_mb': memory_info.rss / 1024 / 1024,  # Resident Set Size in MB
                'vms_memory_mb': memory_info.vms / 1024 / 1024,  # Virtual Memory Size in MB
                'memory_percent': process.memory_percent(),
                'gc_stats': gc_stats,
                'gc_counts': gc.get_count(),
                'cleanup_stats': memory_manager.get_cleanup_stats(),
                'tracked_objects': {
                    'file_streams': len(_cleanup_registry['file_streams']),
                    'temp_files': len(_cleanup_registry['temp_files']),
                    'conversation_contexts': len(_cleanup_registry['conversation_contexts'])
                }
            }
            
            return info
            
        except ImportError:
            # psutil not available, return basic info
            return {
                'gc_counts': gc.get_count(),
                'cleanup_stats': memory_manager.get_cleanup_stats(),
                'tracked_objects': {
                    'file_streams': len(_cleanup_registry['file_streams']),
                    'temp_files': len(_cleanup_registry['temp_files']),
                    'conversation_contexts': len(_cleanup_registry['conversation_contexts'])
                },
                'note': 'Install psutil for detailed memory information'
            }
        
    except Exception as e:
        logger.error(f"[MEMORY] Error getting memory usage info: {e}")
        return {'error': str(e)}

def emergency_cleanup() -> Dict[str, int]:
    """
    Perform emergency cleanup of all tracked resources
    
    Returns:
        Dict: Cleanup results
    """
    results = {
        'file_streams_cleaned': 0,
        'temp_files_cleaned': 0,
        'gc_objects_collected': 0,
        'errors': 0
    }
    
    try:
        logger.warning("[MEMORY] Starting emergency cleanup")
        
        # Clean up tracked file streams
        file_streams = list(_cleanup_registry['file_streams'])
        for stream in file_streams:
            try:
                if hasattr(stream, 'close') and not stream.closed:
                    stream.close()
                    results['file_streams_cleaned'] += 1
            except Exception as e:
                logger.error(f"[MEMORY] Error closing stream in emergency cleanup: {e}")
                results['errors'] += 1
        
        # Clear registries
        _cleanup_registry['file_streams'].clear()
        _cleanup_registry['temp_files'].clear()
        _cleanup_registry['conversation_contexts'].clear()
        
        # Clean temp files
        results['temp_files_cleaned'] = cleanup_temp_files()
        
        # Force aggressive garbage collection
        results['gc_objects_collected'] = force_garbage_collection()
        
        logger.warning(f"[MEMORY] Emergency cleanup completed: {results}")
        
        return results
        
    except Exception as e:
        logger.error(f"[MEMORY] Error during emergency cleanup: {e}")
        results['errors'] += 1
        return results

def create_managed_temp_file(suffix: str = '.tmp', prefix: str = 'kpi_bot_') -> tuple[str, Any]:
    """
    Create a temporary file that will be tracked for cleanup
    
    Args:
        suffix: File suffix
        prefix: File prefix
        
    Returns:
        tuple: (file_path, file_handle)
    """
    try:
        import tempfile
        
        # Create temporary file
        fd, temp_path = tempfile.mkstemp(suffix=suffix, prefix=prefix)
        temp_file = os.fdopen(fd, 'wb')
        
        # Track for cleanup
        _cleanup_registry['temp_files'].add(temp_path)
        
        logger.debug(f"[MEMORY] Created managed temp file: {temp_path}")
        
        return temp_path, temp_file
        
    except Exception as e:
        logger.error(f"[MEMORY] Error creating managed temp file: {e}")
        raise

def log_memory_status():
    """Log current memory status for monitoring"""
    try:
        memory_info = get_memory_usage_info()
        
        if 'rss_memory_mb' in memory_info:
            logger.info(f"[MEMORY] Status - RSS: {memory_info['rss_memory_mb']:.1f}MB, "
                       f"VMS: {memory_info['vms_memory_mb']:.1f}MB, "
                       f"Usage: {memory_info['memory_percent']:.1f}%")
        
        logger.info(f"[MEMORY] GC Counts: {memory_info['gc_counts']}")
        logger.info(f"[MEMORY] Cleanup Stats: {memory_info['cleanup_stats']}")
        logger.info(f"[MEMORY] Tracked Objects: {memory_info['tracked_objects']}")
        
    except Exception as e:
        logger.error(f"[MEMORY] Error logging memory status: {e}")


# Scheduled garbage collection functionality
class ScheduledMemoryManager:
    """Manager for scheduled memory cleanup operations"""
    
    def __init__(self):
        self.scheduler = None
        self.is_running = False
        self.cleanup_interval_minutes = 15
        self.scheduler_type = 'background'  # 'background' or 'asyncio'
        
        # Register shutdown handler
        atexit.register(self.shutdown)
    
    def setup_scheduler(self, scheduler_type: str = 'background', 
                       cleanup_interval_minutes: int = 15) -> bool:
        """
        Set up the scheduler for periodic memory cleanup
        
        Args:
            scheduler_type: Type of scheduler ('background' or 'asyncio')
            cleanup_interval_minutes: Interval between cleanups in minutes
            
        Returns:
            bool: True if setup was successful
        """
        try:
            if not SCHEDULER_AVAILABLE:
                logger.warning("[MEMORY] APScheduler not available. Install with: pip install apscheduler")
                return False
            
            if self.is_running:
                logger.warning("[MEMORY] Scheduler already running. Stop it first.")
                return False
            
            self.scheduler_type = scheduler_type
            self.cleanup_interval_minutes = cleanup_interval_minutes
            
            # Create appropriate scheduler
            if scheduler_type == 'asyncio':
                self.scheduler = AsyncIOScheduler()
            else:
                self.scheduler = BackgroundScheduler()
            
            # Add periodic cleanup job
            self.scheduler.add_job(
                func=self._scheduled_cleanup,
                trigger="interval",
                minutes=cleanup_interval_minutes,
                id='memory_cleanup',
                name='Periodic Memory Cleanup',
                max_instances=1,  # Prevent overlapping executions
                coalesce=True     # Combine missed executions
            )
            
            logger.info(f"[MEMORY] Scheduler setup complete: {scheduler_type} scheduler, "
                       f"{cleanup_interval_minutes} minute intervals")
            
            return True
            
        except Exception as e:
            logger.error(f"[MEMORY] Error setting up scheduler: {e}")
            return False
    
    def start_scheduled_cleanup(self) -> bool:
        """
        Start the scheduled memory cleanup
        
        Returns:
            bool: True if started successfully
        """
        try:
            if not SCHEDULER_AVAILABLE:
                logger.warning("[MEMORY] APScheduler not available")
                return False
            
            if not self.scheduler:
                logger.error("[MEMORY] Scheduler not set up. Call setup_scheduler() first.")
                return False
            
            if self.is_running:
                logger.warning("[MEMORY] Scheduled cleanup already running")
                return True
            
            self.scheduler.start()
            self.is_running = True
            
            logger.info(f"[MEMORY] Scheduled cleanup started: every {self.cleanup_interval_minutes} minutes")
            
            # Run initial cleanup
            self._scheduled_cleanup()
            
            return True
            
        except Exception as e:
            logger.error(f"[MEMORY] Error starting scheduled cleanup: {e}")
            return False
    
    def stop_scheduled_cleanup(self) -> bool:
        """
        Stop the scheduled memory cleanup
        
        Returns:
            bool: True if stopped successfully
        """
        try:
            if not self.is_running or not self.scheduler:
                logger.info("[MEMORY] Scheduled cleanup not running")
                return True
            
            self.scheduler.shutdown(wait=True)
            self.is_running = False
            
            logger.info("[MEMORY] Scheduled cleanup stopped")
            
            return True
            
        except Exception as e:
            logger.error(f"[MEMORY] Error stopping scheduled cleanup: {e}")
            return False
    
    def shutdown(self):
        """Graceful shutdown of the scheduler"""
        try:
            if self.is_running:
                logger.info("[MEMORY] Shutting down scheduled memory manager")
                self.stop_scheduled_cleanup()
        except Exception as e:
            logger.error(f"[MEMORY] Error during shutdown: {e}")
    
    def _scheduled_cleanup(self):
        """Internal method for scheduled cleanup operations"""
        try:
            logger.info("[MEMORY] Starting scheduled cleanup")
            
            # Log memory status before cleanup
            log_memory_status()
            
            # Perform cleanup operations
            cleanup_results = {
                'temp_files_cleaned': 0,
                'gc_objects_collected': 0,
                'start_time': datetime.now()
            }
            
            # Clean up temporary files
            cleanup_results['temp_files_cleaned'] = cleanup_temp_files()
            
            # Force garbage collection
            cleanup_results['gc_objects_collected'] = force_garbage_collection()
            
            # Calculate cleanup duration
            cleanup_results['duration_seconds'] = (
                datetime.now() - cleanup_results['start_time']
            ).total_seconds()
            
            logger.info(f"[MEMORY] Scheduled cleanup completed: "
                       f"{cleanup_results['temp_files_cleaned']} temp files, "
                       f"{cleanup_results['gc_objects_collected']} objects collected, "
                       f"{cleanup_results['duration_seconds']:.2f}s duration")
            
            # Log memory status after cleanup
            log_memory_status()
            
        except Exception as e:
            logger.error(f"[MEMORY] Error during scheduled cleanup: {e}")
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """
        Get current scheduler status
        
        Returns:
            Dict: Scheduler status information
        """
        try:
            status = {
                'scheduler_available': SCHEDULER_AVAILABLE,
                'is_running': self.is_running,
                'scheduler_type': self.scheduler_type,
                'cleanup_interval_minutes': self.cleanup_interval_minutes,
                'scheduler_state': None,
                'next_run_time': None,
                'job_count': 0
            }
            
            if self.scheduler and self.is_running:
                try:
                    status['scheduler_state'] = str(self.scheduler.state)
                    
                    # Get job information
                    jobs = self.scheduler.get_jobs()
                    status['job_count'] = len(jobs)
                    
                    # Get next run time for memory cleanup job
                    memory_job = self.scheduler.get_job('memory_cleanup')
                    if memory_job:
                        status['next_run_time'] = str(memory_job.next_run_time)
                        
                except Exception as e:
                    logger.debug(f"[MEMORY] Error getting detailed scheduler status: {e}")
            
            return status
            
        except Exception as e:
            logger.error(f"[MEMORY] Error getting scheduler status: {e}")
            return {'error': str(e)}

# Global scheduled memory manager instance
scheduled_memory_manager = ScheduledMemoryManager()

def setup_scheduled_cleanup(scheduler_type: str = 'background', 
                          cleanup_interval_minutes: int = 15) -> bool:
    """
    Set up scheduled memory cleanup
    
    Args:
        scheduler_type: Type of scheduler ('background' or 'asyncio')
        cleanup_interval_minutes: Interval between cleanups in minutes
        
    Returns:
        bool: True if setup was successful
    """
    return scheduled_memory_manager.setup_scheduler(scheduler_type, cleanup_interval_minutes)

def start_scheduled_cleanup() -> bool:
    """
    Start scheduled memory cleanup
    
    Returns:
        bool: True if started successfully
    """
    return scheduled_memory_manager.start_scheduled_cleanup()

def stop_scheduled_cleanup() -> bool:
    """
    Stop scheduled memory cleanup
    
    Returns:
        bool: True if stopped successfully
    """
    return scheduled_memory_manager.stop_scheduled_cleanup()

def get_scheduler_status() -> Dict[str, Any]:
    """
    Get scheduler status information
    
    Returns:
        Dict: Scheduler status
    """
    return scheduled_memory_manager.get_scheduler_status()