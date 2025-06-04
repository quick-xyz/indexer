# cloud_run_app.py
"""
Cloud Run application entry point for the blockchain indexer.
Handles both HTTP health checks and background worker processes.
"""
import os
import sys
import threading
import time
import signal
import logging
from flask import Flask, jsonify
from pathlib import Path

# Add indexer package to path
sys.path.insert(0, str(Path(__file__).parent))

from indexer.core.config import IndexerConfig
from indexer.core.container import IndexerContainer
from indexer import _register_services
from indexer.pipeline.orchestrator import PipelineOrchestrator


class CloudRunIndexer:
    """Cloud Run application wrapper for the blockchain indexer"""
    
    def __init__(self):
        self.app = Flask(__name__)
        self.orchestrator = None
        self.worker_thread = None
        self.shutdown_event = threading.Event()
        self.logger = logging.getLogger("cloud_run_indexer")
        
        # Setup logging for Cloud Run
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Setup Flask routes
        self._setup_routes()
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _setup_routes(self):
        """Setup Flask HTTP endpoints for health checks and monitoring"""
        
        @self.app.route('/health')
        def health_check():
            """Health check endpoint for Cloud Run"""
            try:
                if self.orchestrator:
                    status = self.orchestrator.get_pipeline_status()
                    return jsonify({
                        "status": "healthy",
                        "pipeline_running": True,
                        "queue_stats": status["queue_stats"],
                        "active_workers": status["active_workers"]
                    }), 200
                else:
                    return jsonify({
                        "status": "starting",
                        "pipeline_running": False
                    }), 200
            except Exception as e:
                self.logger.error(f"Health check failed: {e}")
                return jsonify({
                    "status": "unhealthy",
                    "error": str(e)
                }), 503
        
        @self.app.route('/ready')
        def readiness_check():
            """Readiness check endpoint"""
            if self.orchestrator and not self.shutdown_event.is_set():
                return jsonify({"status": "ready"}), 200
            else:
                return jsonify({"status": "not_ready"}), 503
        
        @self.app.route('/metrics')
        def metrics():
            """Metrics endpoint for monitoring"""
            try:
                if self.orchestrator:
                    status = self.orchestrator.get_pipeline_status()
                    return jsonify(status), 200
                else:
                    return jsonify({"error": "Pipeline not initialized"}), 503
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/shutdown', methods=['POST'])
        def shutdown():
            """Graceful shutdown endpoint"""
            self.logger.info("Shutdown requested via HTTP")
            self._shutdown()
            return jsonify({"status": "shutting_down"}), 200
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating shutdown")
        self._shutdown()
    
    def _shutdown(self):
        """Graceful shutdown of the application"""
        self.shutdown_event.set()
        
        if self.orchestrator:
            self.logger.info("Stopping pipeline orchestrator")
            self.orchestrator.stop_processing()
        
        # Give time for workers to finish
        if self.worker_thread and self.worker_thread.is_alive():
            self.logger.info("Waiting for worker thread to finish")
            self.worker_thread.join(timeout=30)
    
    def _worker_main(self):
        """Main worker thread that runs the blockchain processing"""
        try:
            self.logger.info("Starting blockchain indexer worker")
            
            # Load configuration
            config_path = os.getenv('CONFIG_PATH', 'config/config.json')
            config = IndexerConfig.from_file(config_path)
            
            # Create container and services
            container = IndexerContainer(config)
            _register_services(container)
            
            # Create orchestrator
            self.orchestrator = PipelineOrchestrator(container)
            
            # Get number of workers from environment
            num_workers = int(os.getenv('WORKERS', '3'))
            
            self.logger.info(f"Starting continuous processing with {num_workers} workers")
            
            # Enqueue some initial blocks
            self.orchestrator.enqueue_recent_blocks(count=100, priority=1)
            
            # Start continuous processing
            self.orchestrator.start_continuous_processing(num_workers=num_workers)
            
            # Keep worker running until shutdown
            while not self.shutdown_event.is_set():
                time.sleep(10)
                
                # Auto-enqueue more blocks periodically
                try:
                    status = self.orchestrator.get_pipeline_status()
                    pending = status["queue_stats"].get("pending", 0)
                    
                    if pending < 20:  # Low queue threshold
                        self.orchestrator.enqueue_recent_blocks(count=50, priority=1)
                        self.logger.info("Auto-enqueued more blocks due to low queue")
                
                except Exception as e:
                    self.logger.error(f"Error in worker loop: {e}")
            
            self.logger.info("Worker thread shutting down")
            
        except Exception as e:
            self.logger.error(f"Worker thread failed: {e}")
            # Set shutdown event to trigger container restart
            self.shutdown_event.set()
    
    def run(self):
        """Run the Cloud Run application"""
        # Start worker thread
        self.worker_thread = threading.Thread(target=self._worker_main, daemon=False)
        self.worker_thread.start()
        
        # Start Flask server
        port = int(os.getenv('PORT', 8080))
        self.logger.info(f"Starting Flask server on port {port}")
        
        # Use a production WSGI server for Cloud Run
        from waitress import serve
        serve(self.app, host='0.0.0.0', port=port, threads=4)


def main():
    """Main entry point for Cloud Run"""
    app = CloudRunIndexer()
    app.run()


if __name__ == "__main__":
    main()


