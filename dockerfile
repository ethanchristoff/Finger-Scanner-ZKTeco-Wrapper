# Use official Python base image with uv (uvicorn + pip + wheel + build tools)
FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Create and activate a virtual environment, then sync dependencies
RUN python -m venv /app/.venv \
    && . /app/.venv/bin/activate \
    && pip install --upgrade pip \
    && pip install uv \
    && if [ -f uv.lock ]; then uv sync; fi

# Set environment variable so uv uses the venv
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy project files
COPY . /app/

# Expose port 8080
EXPOSE 8080

# Run FastAPI app with uvicorn via uv
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]
