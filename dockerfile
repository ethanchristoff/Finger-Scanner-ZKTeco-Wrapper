FROM ghcr.io/astral-sh/uv:alpine

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install system dependencies for MySQL client and build tools
RUN apk add --no-cache build-base mariadb-connector-c-dev gcc

# Copy dependency files first for better caching
COPY pyproject.toml* requirements.txt* uv.lock* ./

# Install Python dependencies using uv
RUN if [ -f uv.lock ]; then uv pip install --system --requirement uv.lock; \
    elif [ -f pyproject.toml ]; then uv pip install --system --requirement pyproject.toml; \
    elif [ -f requirements.txt ]; then uv pip install --system --requirement requirements.txt; fi

# Copy project files
COPY . /app/

# Expose port 8080
EXPOSE 8080

# Run Flask app using uv
CMD ["uv", "pip", "run", "python", "app.py"]
