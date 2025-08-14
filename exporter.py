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

# Global variables to store configuration
source_url = None

# Flask app for serving metrics
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
            logger.debug(f"Fetching data from {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            logger.debug(f"Received {len(response.content)} bytes")
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Debug: log the page title and some structure info
            title = soup.find('title')
            if title:
                logger.debug(f"Page title: {title.get_text().strip()}")
            
            # Find all tables for debugging
            tables = soup.find_all('table')
            logger.debug(f"Found {len(tables)} tables total")
            for i, table in enumerate(tables):
                table_id = table.get('id', f'table-{i}')
                table_class = table.get('class', [])
                logger.debug(f"Table {i}: id='{table_id}', class={table_class}")
            
            # Parse different sections
            self._parse_hosts_table(soup)
            self._parse_scheduler_internals(soup)
            self._parse_vms_tables(soup)
            
            logger.debug("Successfully parsed metrics")
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data from {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to parse data: {e}")
            raise
    
    def _parse_hosts_table(self, soup):
        """Parse the hosts table"""
        # Find the hosts table
        hosts_table = soup.find('table', {'id': 'hosts'})
        if not hosts_table:
            logger.warning("Hosts table not found, looking for alternative selectors")
            # Try to find table with hosts_wrapper
            hosts_wrapper = soup.find('div', {'id': 'hosts_wrapper'})
            if hosts_wrapper:
                hosts_table = hosts_wrapper.find('table')
                logger.debug("Found hosts table via hosts_wrapper")
            else:
                # Try to find by looking for table after "Hosts" heading
                hosts_heading = soup.find('h3', string='Hosts')
                if hosts_heading:
                    hosts_table = hosts_heading.find_next('table')
                    logger.debug("Found hosts table via Hosts heading")
        
        if not hosts_table:
            logger.error("Could not find hosts table with any method")
            self.hosts_data = []
            return
        
        logger.debug(f"Found hosts table: {hosts_table.get('id', 'no-id')}")
        
        headers = []
        thead = hosts_table.find('thead')
        tbody = hosts_table.find('tbody')
        
        if thead:
            header_row = thead.find('tr')
            if header_row:
                headers = [th.get_text().strip() for th in header_row.find_all('th')]
                logger.debug(f"Found headers: {headers}")
        
        self.hosts_data = []
        
        # Try to find data rows - first try tbody, then direct tr elements
        data_rows = []
        if tbody:
            data_rows = tbody.find_all('tr')
            logger.debug(f"Found {len(data_rows)} rows in tbody")
        else:
            # No tbody, look for tr elements that are not in thead
            all_rows = hosts_table.find_all('tr')
            # Skip header row(s) - usually the first one or ones in thead
            header_rows_count = len(thead.find_all('tr')) if thead else 1
            data_rows = all_rows[header_rows_count:]
            logger.debug(f"No tbody found, using {len(data_rows)} direct tr elements (skipped {header_rows_count} header rows)")
        
        if data_rows:
            for i, row in enumerate(data_rows):
                cells = [td.get_text().strip() for td in row.find_all('td')]
                logger.debug(f"Row {i} cells: {cells}")
                if len(cells) == len(headers) and len(cells) > 0:
                    host_data = dict(zip(headers, cells))
                    self.hosts_data.append(host_data)
                    logger.debug(f"Added host: {host_data.get('IP', 'unknown')}")
                elif len(cells) > 0:
                    logger.warning(f"Row {i} has {len(cells)} cells but expected {len(headers)}")
        else:
            logger.warning("No data rows found in hosts table")
        
        logger.info(f"Parsed {len(self.hosts_data)} hosts")
    
    def _parse_scheduler_internals(self, soup):
        """Parse the scheduler internals table"""
        # Find scheduler internals table (the one after "Scheduler Internals" heading)
        internals_heading = soup.find('h3', string='Scheduler Internals')
        if not internals_heading:
            logger.warning("Scheduler Internals heading not found")
            return
        
        # Find the next table after the heading
        table = internals_heading.find_next('table')
        if not table:
            logger.warning("Scheduler Internals table not found")
            return
        
        headers = []
        tbody = table.find('tbody')
        thead = table.find('thead')
        
        if thead:
            header_row = thead.find('tr')
            headers = [th.get_text().strip() for th in header_row.find_all('th')]
        
        self.scheduler_internals = []
        if tbody:
            for row in tbody.find_all('tr'):
                cells = [td.get_text().strip() for td in row.find_all('td')]
                if len(cells) == len(headers):
                    internal_data = dict(zip(headers, cells))
                    self.scheduler_internals.append(internal_data)
        
        logger.info(f"Parsed {len(self.scheduler_internals)} scheduler internal entries")
    
    def _parse_vms_tables(self, soup):
        """Parse VM tables (there might be multiple, one per host)"""
        # Find all tables with class 'vms'
        vm_tables = soup.find_all('table', class_='vms')
        logger.debug(f"Found {len(vm_tables)} VM tables")
        
        # Also try to find tables that might be VMs but don't have the class
        if not vm_tables:
            # Look for tables after headings that mention VMs
            vm_headings = soup.find_all('h3', string=lambda text: text and 'VMs on' in text)
            for heading in vm_headings:
                table = heading.find_next('table')
                if table:
                    vm_tables.append(table)
                    logger.debug(f"Found VM table via heading: {heading.get_text().strip()}")
        
        self.vms_data = []
        for i, table in enumerate(vm_tables):
            logger.debug(f"Processing VM table {i}")
            headers = []
            thead = table.find('thead')
            tbody = table.find('tbody')
            
            if thead:
                header_row = thead.find('tr')
                if header_row:
                    headers = [th.get_text().strip() for th in header_row.find_all('th')]
                    logger.debug(f"VM table {i} headers: {headers}")
            
            # Try to find data rows - first try tbody, then direct tr elements
            data_rows = []
            if tbody:
                data_rows = tbody.find_all('tr')
                logger.debug(f"VM table {i}: Found {len(data_rows)} rows in tbody")
            else:
                # No tbody, look for tr elements that are not in thead
                all_rows = table.find_all('tr')
                # Skip header row(s) - usually the first one or ones in thead
                header_rows_count = len(thead.find_all('tr')) if thead else 1
                data_rows = all_rows[header_rows_count:]
                logger.debug(f"VM table {i}: No tbody found, using {len(data_rows)} direct tr elements")
            
            if data_rows:
                for j, row in enumerate(data_rows):
                    cells = [td.get_text().strip() for td in row.find_all('td')]
                    logger.debug(f"VM table {i}, row {j} cells: {cells}")
                    if len(cells) == len(headers) and len(cells) > 0:
                        vm_data = dict(zip(headers, cells))
                        self.vms_data.append(vm_data)
                        logger.debug(f"Added VM: {vm_data.get('Name', 'unknown')}")
                    elif len(cells) > 0:
                        logger.warning(f"VM table {i}, row {j} has {len(cells)} cells but expected {len(headers)}")
        
        logger.info(f"Parsed {len(self.vms_data)} VMs")
    
    def get_prometheus_metrics(self):
        """Generate Prometheus metrics format"""
        metrics = []
        
        # Add metadata
        metrics.append("# HELP acropolis_scrape_timestamp_seconds Timestamp of the last successful scrape")
        metrics.append("# TYPE acropolis_scrape_timestamp_seconds gauge")
        metrics.append(f"acropolis_scrape_timestamp_seconds {time.time()}")
        metrics.append("")
        
        # Host metrics
        self._add_host_metrics(metrics)
        
        # Summary metrics
        metrics.append("# HELP acropolis_hosts_total Total number of hosts")
        metrics.append("# TYPE acropolis_hosts_total gauge")
        metrics.append(f"acropolis_hosts_total {len(self.hosts_data)}")
        metrics.append("")
        
        metrics.append("# HELP acropolis_vms_total Total number of VMs")
        metrics.append("# TYPE acropolis_vms_total gauge")
        metrics.append(f"acropolis_vms_total {len(self.vms_data)}")
        metrics.append("")
        
        return "\n".join(metrics)
    
    def _add_host_metrics(self, metrics):
        """Add host-specific metrics to the metrics list"""
        if not self.hosts_data:
            return
        
        # Host CPU cores total
        metrics.append("# HELP acropolis_host_cpu_cores_total Total CPU cores available on the host")
        metrics.append("# TYPE acropolis_host_cpu_cores_total gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            cpus = self._parse_numeric(host.get('CPUs', '0'))
            metrics.append(f'acropolis_host_cpu_cores_total{{ip="{ip}",uuid="{uuid}"}} {cpus}')
        metrics.append("")
        
        # Host CPU cores used
        metrics.append("# HELP acropolis_host_cpu_cores_used CPU cores currently in use on the host")
        metrics.append("# TYPE acropolis_host_cpu_cores_used gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            cpu_used = self._parse_numeric(host.get('CPU used', '0'))
            metrics.append(f'acropolis_host_cpu_cores_used{{ip="{ip}",uuid="{uuid}"}} {cpu_used}')
        metrics.append("")
        
        # Host CPU cores free
        metrics.append("# HELP acropolis_host_cpu_cores_free CPU cores currently free on the host")
        metrics.append("# TYPE acropolis_host_cpu_cores_free gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            cpu_free = self._parse_numeric(host.get('CPU free', '0'))
            metrics.append(f'acropolis_host_cpu_cores_free{{ip="{ip}",uuid="{uuid}"}} {cpu_free}')
        metrics.append("")
        
        # Host memory total (in bytes)
        metrics.append("# HELP acropolis_host_memory_total_bytes Total memory available on the host in bytes")
        metrics.append("# TYPE acropolis_host_memory_total_bytes gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            memory_mb = self._parse_memory_mb(host.get('Memory', '0 MB'))
            memory_bytes = memory_mb * 1024 * 1024
            metrics.append(f'acropolis_host_memory_total_bytes{{ip="{ip}",uuid="{uuid}"}} {memory_bytes}')
        metrics.append("")
        
        # Host memory used (in bytes)
        metrics.append("# HELP acropolis_host_memory_used_bytes Memory currently used on the host in bytes")
        metrics.append("# TYPE acropolis_host_memory_used_bytes gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            memory_mb = self._parse_memory_mb(host.get('Memory Used', '0 MB'))
            memory_bytes = memory_mb * 1024 * 1024
            metrics.append(f'acropolis_host_memory_used_bytes{{ip="{ip}",uuid="{uuid}"}} {memory_bytes}')
        metrics.append("")
        
        # Host memory reserved (in bytes)
        metrics.append("# HELP acropolis_host_memory_reserved_bytes Memory currently reserved on the host in bytes")
        metrics.append("# TYPE acropolis_host_memory_reserved_bytes gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            memory_mb = self._parse_memory_mb(host.get('Memory Reserved', '0'))
            memory_bytes = memory_mb * 1024 * 1024
            metrics.append(f'acropolis_host_memory_reserved_bytes{{ip="{ip}",uuid="{uuid}"}} {memory_bytes}')
        metrics.append("")
        
        # Host memory free (in bytes)
        metrics.append("# HELP acropolis_host_memory_free_bytes Memory currently free on the host in bytes")
        metrics.append("# TYPE acropolis_host_memory_free_bytes gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            memory_mb = self._parse_memory_mb(host.get('Memory Free', '0 MB'))
            memory_bytes = memory_mb * 1024 * 1024
            metrics.append(f'acropolis_host_memory_free_bytes{{ip="{ip}",uuid="{uuid}"}} {memory_bytes}')
        metrics.append("")
        
        # Host memory assigned (in bytes)
        metrics.append("# HELP acropolis_host_memory_assigned_bytes Memory currently assigned on the host in bytes")
        metrics.append("# TYPE acropolis_host_memory_assigned_bytes gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            memory_mb = self._parse_memory_mb(host.get('Memory Assigned', '0'))
            memory_bytes = memory_mb * 1024 * 1024
            metrics.append(f'acropolis_host_memory_assigned_bytes{{ip="{ip}",uuid="{uuid}"}} {memory_bytes}')
        metrics.append("")
        
        # Host priority score
        metrics.append("# HELP acropolis_host_priority Host priority score")
        metrics.append("# TYPE acropolis_host_priority gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            priority = self._parse_numeric(host.get('Priority', '0'))
            metrics.append(f'acropolis_host_priority{{ip="{ip}",uuid="{uuid}"}} {priority}')
        metrics.append("")
        
        # Host schedulable status (1 = True, 0 = False)
        metrics.append("# HELP acropolis_host_schedulable Whether the host is schedulable (1) or not (0)")
        metrics.append("# TYPE acropolis_host_schedulable gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            schedulable = 1 if host.get('Schedulable', '').lower() == 'true' else 0
            metrics.append(f'acropolis_host_schedulable{{ip="{ip}",uuid="{uuid}"}} {schedulable}')
        metrics.append("")
        
        # Host connected status (1 = True, 0 = False)
        metrics.append("# HELP acropolis_host_connected Whether the host is connected (1) or not (0)")
        metrics.append("# TYPE acropolis_host_connected gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            connected = 1 if host.get('Connected', '').lower() == 'true' else 0
            metrics.append(f'acropolis_host_connected{{ip="{ip}",uuid="{uuid}"}} {connected}')
        metrics.append("")
        
        # Host GPU node status (1 = True, 0 = False)
        metrics.append("# HELP acropolis_host_gpu_node Whether the host has GPUs (1) or not (0)")
        metrics.append("# TYPE acropolis_host_gpu_node gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            gpu_node = 1 if host.get('GPU Node', '').lower() == 'true' else 0
            metrics.append(f'acropolis_host_gpu_node{{ip="{ip}",uuid="{uuid}"}} {gpu_node}')
        metrics.append("")
        
        # Host Zeus state as labels
        metrics.append("# HELP acropolis_host_info Host information with Zeus state as label")
        metrics.append("# TYPE acropolis_host_info gauge")
        for host in self.hosts_data:
            ip = host.get('IP', 'unknown')
            uuid = host.get('UUID', 'unknown')
            zeus_state = host.get('ZeusState', 'unknown')
            metrics.append(f'acropolis_host_info{{ip="{ip}",uuid="{uuid}",zeus_state="{zeus_state}"}} 1')
        metrics.append("")
    
    def _parse_numeric(self, value):
        """Parse numeric value, handling various formats"""
        if not value:
            return 0
        try:
            # Remove any non-numeric characters except decimal point
            cleaned = ''.join(c for c in str(value) if c.isdigit() or c == '.')
            return float(cleaned) if cleaned else 0
        except (ValueError, TypeError):
            return 0
    
    def _parse_memory_mb(self, value):
        """Parse memory value in MB format (e.g., '1029176 MB', '16 GB')"""
        if not value:
            return 0
        
        try:
            value_str = str(value).upper().strip()
            
            # Extract numeric part
            numeric_part = ''
            for char in value_str:
                if char.isdigit() or char == '.':
                    numeric_part += char
                elif numeric_part:  # Stop at first non-numeric after we've started collecting digits
                    break
            
            if not numeric_part:
                return 0
            
            number = float(numeric_part)
            
            # Convert based on unit
            if 'GB' in value_str:
                return number * 1024  # Convert GB to MB
            elif 'MB' in value_str or 'TB' in value_str:
                if 'TB' in value_str:
                    return number * 1024 * 1024  # Convert TB to MB
                return number  # Already in MB
            else:
                # Assume MB if no unit specified
                return number
                
        except (ValueError, TypeError):
            return 0

# Global metrics instance
acropolis_metrics = AcropolisMetrics()

@app.route('/metrics')
def metrics():
    """Serve Prometheus metrics"""
    try:
        # Fetch fresh data on each request
        acropolis_metrics.fetch_and_parse(source_url)
        return Response(
            acropolis_metrics.get_prometheus_metrics(),
            mimetype='text/plain'
        )
    except Exception as e:
        logger.error(f"Error generating metrics: {e}")
        # Return error metric instead of failing completely
        error_metrics = [
            "# HELP acropolis_scrape_error Whether there was an error scraping the target",
            "# TYPE acropolis_scrape_error gauge",
            "acropolis_scrape_error 1",
            ""
        ]
        return Response(
            "\n".join(error_metrics),
            mimetype='text/plain'
        )

@app.route('/health')
def health():
    """Health check endpoint"""
    return {"status": "healthy", "source_url": source_url}

@click.command()
@click.option('--url', required=True, help='URL to fetch Acropolis scheduler data from')
@click.option('--port', default=8080, help='Port to serve metrics on (default: 8080)')
@click.option('--debug', is_flag=True, help='Enable debug logging')
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
    
    # Start Flask server
    try:
        app.run(host='0.0.0.0', port=port)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)

if __name__ == '__main__':
    main()