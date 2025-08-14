#!/usr/bin/env python3
"""
Acropolis Prometheus Exporter

Fetches Acropolis scheduler data and exposes it as Prometheus metrics.
"""

import click
import requests
import time
from flask import Flask, Response
from bs4 import BeautifulSoup
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables
source_url = None
app = Flask(__name__)

class AcropolisMetrics:
    """Class to handle parsing and storing Acropolis metrics"""
    
    def __init__(self):
        self.hosts_data = []
        self.scheduler_internals = []
        self.vms_data = []
    
    def fetch_and_parse(self, url):
        """Fetch the HTML page and parse the metrics"""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Initialize data structures
            self.hosts_data = []
            self.scheduler_internals = []
            self.vms_data = []
            
            # Parse different sections
            self._parse_hosts_table(soup)
            self._parse_scheduler_internals(soup)
            self._parse_vms_tables(soup)
            
            logger.debug("Successfully parsed metrics")
            
        except Exception as e:
            logger.error(f"Failed to fetch/parse data: {e}")
            raise
    
    def _parse_hosts_table(self, soup):
        """Parse the hosts table"""
        hosts_table = soup.find('table', {'id': 'hosts'})
        if not hosts_table:
            hosts_wrapper = soup.find('div', {'id': 'hosts_wrapper'})
            if hosts_wrapper:
                hosts_table = hosts_wrapper.find('table')
        
        if not hosts_table:
            logger.warning("Could not find hosts table")
            return
        
        headers = []
        thead = hosts_table.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                headers = [th.get_text().strip() for th in header_row.find_all('th')]
        
        # Get data rows (handle missing tbody)
        data_rows = []
        tbody = hosts_table.find('tbody')
        if tbody:
            data_rows = tbody.find_all('tr')
        else:
            all_rows = hosts_table.find_all('tr')
            header_rows_count = len(thead.find_all('tr')) if thead else 1
            data_rows = all_rows[header_rows_count:]
        
        for row in data_rows:
            cells = [td.get_text().strip() for td in row.find_all('td')]
            if len(cells) == len(headers) and len(cells) > 0:
                host_data = dict(zip(headers, cells))
                self.hosts_data.append(host_data)
        
        logger.info(f"Parsed {len(self.hosts_data)} hosts")
    
    def _parse_scheduler_internals(self, soup):
        """Parse the scheduler internals table"""
        internals_heading = soup.find('h3', string='Scheduler Internals')
        if not internals_heading:
            logger.warning("Scheduler Internals heading not found")
            return
        
        table = internals_heading.find_next('table')
        if not table:
            logger.warning("Scheduler Internals table not found")
            return
        
        headers = []
        thead = table.find('thead')
        if thead:
            header_row = thead.find('tr')
            if header_row:
                headers = [th.get_text().strip() for th in header_row.find_all('th')]
        
        # Get data rows (handle missing tbody)
        data_rows = []
        tbody = table.find('tbody')
        if tbody:
            data_rows = tbody.find_all('tr')
        else:
            all_rows = table.find_all('tr')
            header_rows_count = len(thead.find_all('tr')) if thead else 1
            data_rows = all_rows[header_rows_count:]
        
        for row in data_rows:
            cells = [td.get_text().strip() for td in row.find_all('td')]
            if len(cells) == len(headers) and len(cells) > 0:
                internal_data = dict(zip(headers, cells))
                self.scheduler_internals.append(internal_data)
        
        logger.info(f"Parsed {len(self.scheduler_internals)} scheduler internal entries")
    
    def _parse_vms_tables(self, soup):
        """Parse VM tables"""
        vm_tables = soup.find_all('table', class_='vms')
        
        # Also look for tables after VM headings
        if not vm_tables:
            vm_headings = soup.find_all('h3', string=lambda text: text and 'VMs on' in text)
            for heading in vm_headings:
                table = heading.find_next('table')
                if table:
                    vm_tables.append(table)
        
        for i, table in enumerate(vm_tables):
            # Extract host IP from heading
            host_ip = 'unknown'
            for heading in soup.find_all('h3'):
                if 'VMs on' in heading.get_text() and heading.find_next('table') == table:
                    heading_text = heading.get_text().strip()
                    if 'VMs on' in heading_text:
                        host_ip = heading_text.replace('VMs on', '').strip()
                    break
            
            headers = []
            thead = table.find('thead')
            if thead:
                header_row = thead.find('tr')
                if header_row:
                    headers = [th.get_text().strip() for th in header_row.find_all('th')]
            
            # Get data rows (handle missing tbody)
            data_rows = []
            tbody = table.find('tbody')
            if tbody:
                data_rows = tbody.find_all('tr')
            else:
                all_rows = table.find_all('tr')
                header_rows_count = len(thead.find_all('tr')) if thead else 1
                data_rows = all_rows[header_rows_count:]
            
            for row in data_rows:
                cells = [td.get_text().strip() for td in row.find_all('td')]
                if len(cells) == len(headers) and len(cells) > 0:
                    vm_data = dict(zip(headers, cells))
                    vm_data['host_ip'] = host_ip
                    self.vms_data.append(vm_data)
        
        logger.info(f"Parsed {len(self.vms_data)} VMs")
    
    def get_prometheus_metrics(self):
        """Generate Prometheus metrics format"""
        try:
            metrics = []
            
            # Add metadata
            metrics.append("# HELP acropolis_scrape_timestamp_seconds Timestamp of the last successful scrape")
            metrics.append("# TYPE acropolis_scrape_timestamp_seconds gauge")
            metrics.append(f"acropolis_scrape_timestamp_seconds {time.time()}")
            metrics.append("")
            
            # Add all metric sections
            self._add_host_metrics(metrics)
            self._add_scheduler_internals_metrics(metrics)
            self._add_vm_metrics(metrics)
            
            # Summary metrics
            hosts_count = len(self.hosts_data) if self.hosts_data else 0
            vms_count = len(self.vms_data) if self.vms_data else 0
            
            metrics.append("# HELP acropolis_hosts_total Total number of hosts")
            metrics.append("# TYPE acropolis_hosts_total gauge")
            metrics.append(f"acropolis_hosts_total {hosts_count}")
            metrics.append("")
            
            metrics.append("# HELP acropolis_vms_total Total number of VMs")
            metrics.append("# TYPE acropolis_vms_total gauge")
            metrics.append(f"acropolis_vms_total {vms_count}")
            metrics.append("")
            
            return "\n".join(metrics)
            
        except Exception as e:
            logger.error(f"Error in get_prometheus_metrics: {e}")
            return f"# ERROR in metrics generation: {str(e)}\nacropolis_scrape_error 1\n"
    
    def _add_host_metrics(self, metrics):
        """Add host-specific metrics"""
        if not self.hosts_data:
            return
        
        # CPU metrics
        metrics.append("# HELP acropolis_host_cpu_cores_total Total CPU cores available on the host")
        metrics.append("# TYPE acropolis_host_cpu_cores_total gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            cpus = self._parse_numeric(host.get('CPUs', '0'))
            metrics.append(f'acropolis_host_cpu_cores_total{{ip="{ip}",uuid="{uuid}"}} {cpus}')
        metrics.append("")
        
        metrics.append("# HELP acropolis_host_cpu_cores_used CPU cores currently in use on the host")
        metrics.append("# TYPE acropolis_host_cpu_cores_used gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            cpu_used = self._parse_numeric(host.get('CPU used', '0'))
            metrics.append(f'acropolis_host_cpu_cores_used{{ip="{ip}",uuid="{uuid}"}} {cpu_used}')
        metrics.append("")
        
        metrics.append("# HELP acropolis_host_cpu_cores_free CPU cores currently free on the host")
        metrics.append("# TYPE acropolis_host_cpu_cores_free gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            cpu_free = self._parse_numeric(host.get('CPU free', '0'))
            metrics.append(f'acropolis_host_cpu_cores_free{{ip="{ip}",uuid="{uuid}"}} {cpu_free}')
        metrics.append("")
        
        # Memory metrics
        memory_fields = [
            ('Memory', 'total'),
            ('Memory Used', 'used'),
            ('Memory Reserved', 'reserved'),
            ('Memory Free', 'free'),
            ('Memory Assigned', 'assigned')
        ]
        
        for field_name, metric_suffix in memory_fields:
            metrics.append(f"# HELP acropolis_host_memory_{metric_suffix}_bytes Memory {metric_suffix} on the host in bytes")
            metrics.append(f"# TYPE acropolis_host_memory_{metric_suffix}_bytes gauge")
            for host in self.hosts_data:
                ip = host.get('IP', 'unknown')
                uuid = host.get('UUID', 'unknown')
                memory_mb = self._parse_memory_mb(host.get(field_name, '0'))
                memory_bytes = memory_mb * 1024 * 1024
                metrics.append(f'acropolis_host_memory_{metric_suffix}_bytes{{ip="{ip}",uuid="{uuid}"}} {memory_bytes}')
            metrics.append("")
        
        # Status metrics
        metrics.append("# HELP acropolis_host_priority Host priority score")
        metrics.append("# TYPE acropolis_host_priority gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            priority = self._parse_numeric(host.get('Priority', '0'))
            metrics.append(f'acropolis_host_priority{{ip="{ip}",uuid="{uuid}"}} {priority}')
        metrics.append("")
        
        status_fields = [
            ('Schedulable', 'schedulable'),
            ('Connected', 'connected'),
            ('GPU Node', 'gpu_node')
        ]
        
        for field_name, metric_suffix in status_fields:
            metrics.append(f"# HELP acropolis_host_{metric_suffix} Whether the host {metric_suffix.replace('_', ' ')} (1) or not (0)")
            metrics.append(f"# TYPE acropolis_host_{metric_suffix} gauge")
            for host in self.hosts_data:
                ip = host.get('IP', 'unknown')
                uuid = host.get('UUID', 'unknown')
                value = 1 if host.get(field_name, '').lower() == 'true' else 0
                metrics.append(f'acropolis_host_{metric_suffix}{{ip="{ip}",uuid="{uuid}"}} {value}')
            metrics.append("")
        
        # Host info
        metrics.append("# HELP acropolis_host_info Host information with Zeus state as label")
        metrics.append("# TYPE acropolis_host_info gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            zeus_state = host.get('ZeusState', 'unknown')
            metrics.append(f'acropolis_host_info{{ip="{ip}",uuid="{uuid}",zeus_state="{zeus_state}"}} 1')
        metrics.append("")
    
    def _add_scheduler_internals_metrics(self, metrics):
        """Add scheduler internals metrics"""
        if not self.scheduler_internals:
            return
        
        fields = [
            ('Actual Memory Used', 'actual_memory_used'),
            ('VM Overheads', 'vm_overheads'),
            ('Host Overheads', 'host_overheads')
        ]
        
        for field_name, metric_suffix in fields:
            metrics.append(f"# HELP acropolis_scheduler_{metric_suffix}_bytes {field_name} by the host in bytes")
            metrics.append(f"# TYPE acropolis_scheduler_{metric_suffix}_bytes gauge")
            for internal in self.scheduler_internals:
                uuid = internal.get('UUID', 'unknown')
                memory_mb = self._parse_memory_mb(internal.get(field_name, '0 MB'))
                memory_bytes = memory_mb * 1024 * 1024
                metrics.append(f'acropolis_scheduler_{metric_suffix}_bytes{{uuid="{uuid}"}} {memory_bytes}')
            metrics.append("")
    
    def _add_vm_metrics(self, metrics):
        """Add VM-specific metrics"""
        if not self.vms_data:
            return
        
        # CPU metrics
        cpu_fields = [
            ('CPUs', 'total'),
            ('CPU used', 'used'),
            ('CPU free', 'free')
        ]
        
        for field_name, metric_suffix in cpu_fields:
            metrics.append(f"# HELP acropolis_vm_cpu_cores_{metric_suffix} CPU cores {metric_suffix} by the VM")
            metrics.append(f"# TYPE acropolis_vm_cpu_cores_{metric_suffix} gauge")
            for vm in self.vms_data:
                vm_name = vm.get('Name', 'unknown')
                vm_uuid = vm.get('UUID', 'unknown')
                host_ip = vm.get('host_ip', 'unknown')
                value = self._parse_numeric(vm.get(field_name, '0'))
                metrics.append(f'acropolis_vm_cpu_cores_{metric_suffix}{{vm_name="{vm_name}",vm_uuid="{vm_uuid}",host_ip="{host_ip}"}} {value}')
            metrics.append("")
        
        # Memory metric
        metrics.append("# HELP acropolis_vm_memory_total_bytes Total memory assigned to the VM in bytes")
        metrics.append("# TYPE acropolis_vm_memory_total_bytes gauge")
        for vm in self.vms_data:
            vm_name = vm.get('Name', 'unknown')
            vm_uuid = vm.get('UUID', 'unknown')
            host_ip = vm.get('host_ip', 'unknown')
            memory_mb = self._parse_memory_mb(vm.get('Memory', '0 GB'))
            memory_bytes = memory_mb * 1024 * 1024
            metrics.append(f'acropolis_vm_memory_total_bytes{{vm_name="{vm_name}",vm_uuid="{vm_uuid}",host_ip="{host_ip}"}} {memory_bytes}')
        metrics.append("")
        
        # VM info
        metrics.append("# HELP acropolis_vm_info VM information with destination as label")
        metrics.append("# TYPE acropolis_vm_info gauge")
        for vm in self.vms_data:
            vm_name = vm.get('Name', 'unknown')
            vm_uuid = vm.get('UUID', 'unknown')
            host_ip = vm.get('host_ip', 'unknown')
            destination = vm.get('Destination', 'none')
            metrics.append(f'acropolis_vm_info{{vm_name="{vm_name}",vm_uuid="{vm_uuid}",host_ip="{host_ip}",destination="{destination}"}} 1')
        metrics.append("")
    
    def _parse_numeric(self, value):
        """Parse numeric value"""
        if not value:
            return 0
        try:
            cleaned = ''.join(c for c in str(value) if c.isdigit() or c == '.')
            return float(cleaned) if cleaned else 0
        except (ValueError, TypeError):
            return 0
    
    def _parse_memory_mb(self, value):
        """Parse memory value in MB format"""
        if not value:
            return 0
        
        try:
            value_str = str(value).upper().strip()
            
            # Extract numeric part
            numeric_part = ''
            for char in value_str:
                if char.isdigit() or char == '.':
                    numeric_part += char
                elif numeric_part:
                    break
            
            if not numeric_part:
                return 0
            
            number = float(numeric_part)
            
            # Convert based on unit
            if 'GB' in value_str:
                return number * 1024
            elif 'TB' in value_str:
                return number * 1024 * 1024
            else:
                return number  # Assume MB
                
        except (ValueError, TypeError):
            return 0

# Global metrics instance
acropolis_metrics = AcropolisMetrics()

@app.route('/metrics')
def metrics():
    """Serve Prometheus metrics"""
    try:
        acropolis_metrics.fetch_and_parse(source_url)
        metrics_output = acropolis_metrics.get_prometheus_metrics()
        
        if metrics_output is None:
            metrics_output = "# Error: metrics generation returned None\nacropolis_scrape_error 1\n"
        
        return Response(metrics_output, mimetype='text/plain')
    except Exception as e:
        logger.error(f"Error generating metrics: {e}")
        error_metrics = [
            "# HELP acropolis_scrape_error Whether there was an error scraping the target",
            "# TYPE acropolis_scrape_error gauge",
            "acropolis_scrape_error 1",
            f"# Error: {str(e)}",
            ""
        ]
        return Response("\n".join(error_metrics), mimetype='text/plain')

@app.route('/health')
def health():
    """Health check endpoint"""
    return {"status": "healthy", "source_url": source_url}

@click.command()
@click.option('--url', envvar="SCHEDULER_URL", required=True, help='URL to fetch Acropolis scheduler data from')
@click.option('--port', envvar="EXPORTER_PORT", default=8080, help='Port to serve metrics on (default: 8080)')
@click.option('--debug', envvar="DEBUG", is_flag=True, help='Enable debug logging')
def main(url, port, debug):
    """Acropolis Prometheus Exporter"""
    global source_url
    
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
        app.debug = True
    
    source_url = url
    
    logger.info(f"Starting Acropolis exporter")
    logger.info(f"Source URL: {url}")
    logger.info(f"Metrics port: {port}")
    logger.info(f"Metrics available at http://localhost:{port}/metrics")
    logger.info(f"Health check available at http://localhost:{port}/health")
    
    try:
        app.run(host='0.0.0.0', port=port)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)

if __name__ == '__main__':
    main()
