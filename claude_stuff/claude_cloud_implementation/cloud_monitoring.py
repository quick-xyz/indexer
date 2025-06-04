

# monitoring/cloud_monitoring.py
"""
Google Cloud Monitoring integration for pipeline metrics
"""
import time
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import TimeSeries, Point, TimeInterval
import logging


class CloudMonitoring:
    """Google Cloud Monitoring client for pipeline metrics"""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = monitoring_v3.MetricServiceClient()
        self.project_name = f"projects/{project_id}"
        self.logger = logging.getLogger("cloud_monitoring")
    
    def send_pipeline_metrics(self, queue_stats: dict, worker_stats: list):
        """Send pipeline metrics to Cloud Monitoring"""
        try:
            # Create time series for queue metrics
            series = []
            now = time.time()
            
            # Queue depth metrics
            for status, count in queue_stats.items():
                if status != 'total':
                    series.append(self._create_time_series(
                        f"custom.googleapis.com/indexer/queue/{status}",
                        count,
                        now
                    ))
            
            # Worker count metric
            active_workers = len(worker_stats)
            series.append(self._create_time_series(
                "custom.googleapis.com/indexer/workers/active",
                active_workers,
                now
            ))
            
            # Send metrics
            if series:
                self.client.create_time_series(
                    name=self.project_name,
                    time_series=series
                )
                self.logger.debug(f"Sent {len(series)} metrics to Cloud Monitoring")
        
        except Exception as e:
            self.logger.error(f"Failed to send metrics: {e}")
    
    def _create_time_series(self, metric_type: str, value: float, timestamp: float):
        """Create a time series for a metric"""
        interval = TimeInterval({
            "end_time": {"seconds": int(timestamp)}
        })
        
        point = Point({
            "interval": interval,
            "value": {"int64_value": int(value)}
        })
        
        return TimeSeries({
            "metric": {"type": metric_type},
            "resource": {
                "type": "generic_node",
                "labels": {
                    "project_id": self.project_id,
                    "location": "us-central1",
                    "namespace": "blockchain-indexer",
                    "node_id": "cloud-run-instance"
                }
            },
            "points": [point]
        })