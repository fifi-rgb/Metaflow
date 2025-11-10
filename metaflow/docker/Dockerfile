# MetaFlow - Production Dockerfile
# Multi-stage build for optimized image size

# Stage 1: Base image with Spark
FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu AS spark-base

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libpq-dev \
    postgresql-client \
    curl \
    wget \
    vim \
    git \
    && rm -rf /var/lib/apt/lists/*

# Stage 2: Python dependencies
FROM spark-base AS python-deps

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Create virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r /tmp/requirements.txt

# Install JDBC drivers
RUN mkdir -p /opt/spark/jars && \
    cd /opt/spark/jars && \
    # PostgreSQL
    wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    # MySQL
    wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j-8.1.0.jar && \
    # SQL Server
    wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.0.jre11/mssql-jdbc-12.4.0.jre11.jar && \
    # Delta Lake
    wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

# Stage 3: Final application image
FROM spark-base AS final

# Copy virtual environment from python-deps stage
COPY --from=python-deps /opt/venv /opt/venv
COPY --from=python-deps /opt/spark/jars/* /opt/spark/jars/

# Set environment variables
ENV PATH="/opt/venv/bin:$PATH" \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=/opt/venv/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python3

# Create app directory
WORKDIR /app

# Copy application code
COPY metaflow/ /app/metaflow/
COPY setup.py pyproject.toml README.md /app/
COPY examples/ /app/examples/

# Install MetaFlow in development mode
RUN pip install -e .

# Create directories for data and logs
RUN mkdir -p /data /logs /checkpoints && \
    chmod -R 777 /data /logs /checkpoints

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import metaflow; print('OK')" || exit 1

# Switch to non-root user
RUN useradd -m -u 1000 metaflow && \
    chown -R metaflow:metaflow /app /data /logs /checkpoints
USER metaflow

# Expose ports
# 4040: Spark UI
# 8080: Application UI
EXPOSE 4040 8080

# Default command
CMD ["metaflow", "--help"]