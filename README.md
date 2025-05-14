# Data Pipeline with Error Recovery System

## Overview
This project implements a robust data pipeline system with integrated error handling, recovery mechanisms, and monitoring capabilities. It's designed to work within a Hadoop Hive cluster environment, providing reliable data processing with automatic error recovery and health monitoring.

Key Features:
- Automated error detection and recovery
- Checkpoint-based state management
- Real-time health monitoring
- Email notifications for critical events
- Thread-safe error processing
- Comprehensive logging system

## Architecture

### Core Components

1. **Error Handler**
   - Centralized error management
   - Automatic retry mechanism
   - Email notifications
   - Thread-safe processing
   - Detailed error logging

2. **Checkpoint Manager**
   - State persistence
   - Recovery point management
   - Data consistency verification
   - Automatic checkpoint creation
   - Rollback capabilities

3. **Health Monitor**
   - Pipeline status tracking
   - Resource utilization monitoring
   - Performance metrics collection
   - Alert generation
   - Health check endpoints

4. **Logger**
   - Structured logging
   - Multiple log levels
   - Log rotation
   - Error traceback capture
   - System integration

## Workflow

**Data Processing Flow**
   ```
   Input Data → Validation → Processing → Checkpoint → Output
                    ↑            ↓
                    └── Error Handler
   ```
## Running in Hadoop Hive Cluster

### Prerequisites
- Python 3.8+
- Hadoop Hive cluster access
- SMTP server access
- Required Python packages (see requirements.txt)

### Setup Steps

1. **Environment Configuration**
   ```bash
   # Set Hadoop environment variables
   export HADOOP_HOME=/path/to/hadoop
   export HIVE_HOME=/path/to/hive
   export PATH=$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin
   ```

2. **Python Environment**
   ```bash
   # Create and activate virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   ```

3. **Configuration**
   Create `.env` file:
   ```env
   # SMTP Configuration
   SMTP_SERVER=your.smtp.server
   SMTP_PORT=465
   SENDER_EMAIL=your.email@domain.com
   SENDER_PASSWORD=your_password
   RECIPIENT_EMAIL=recipient@domain.com
   ```

4. **Cluster Preparation**
   ```bash
   # Verify Hadoop connection
   hadoop fs -ls /
   ```

5. **Job Submission**
   ```bash
   # run Python script
   "python main.py" 
   
   ```

## Future Work

### 1. Recovery process
- Implement Point-in-time recovery
- Automated recovery

### 2. Health check
- Monitor the pipline for any failure

### 3. Archiving Mechanism 
- Delete checkpoint subdir according to the retention policy
