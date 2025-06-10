# Use the official uv base image on Alpine
FROM ghcr.io/astral-sh/uv:alpine

# Set environment variables for Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

RUN apk add --no-cache build-base gcc libc-dev mariadb-connector-c-dev

# Copy the pyproject.toml file to define dependencies
COPY pyproject.toml ./

# Create a virtual environment using uv
RUN uv venv

# Install Python dependencies from pyproject.toml into the virtual environment
# Using -r pyproject.toml treats it as a requirements file for this command.
RUN uv pip install --python /app/.venv/bin/python -r pyproject.toml

# Copy the rest of your application code
# Copy specific files and directories to avoid including unwanted local artifacts
COPY app.py ./
COPY adms_wrapper ./adms_wrapper/
COPY static ./static/
COPY templates ./templates/

# Expose the port the Flask app runs on
EXPOSE 5000

# Command to run the Flask application using the Python from the virtual environment
CMD ["uv", "run", "app.py"]
