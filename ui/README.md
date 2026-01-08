# Analitiq Stream Configuration Viewer

A lightweight web application to inspect and manage the configuration files for Analitiq Stream pipelines.

## Features

- Browse example pipelines from the `src/examples` directory
- View pipeline configuration details and field mappings
- Visualize endpoint schemas (API and database)
- Manage host credentials (view, create, edit, delete)
- Easy navigation between connected configuration files

## Installation

This application is included with the Analitiq Stream project. All dependencies are managed via Poetry in the `dev` group.

Make sure you have Poetry installed and run:

```bash
# Install the application dependencies
poetry install --with dev
```

## Usage

To run the application:

```bash
# Activate the Poetry virtual environment
poetry shell

# Start the application
python -m src.ui.app
```

Then open your browser at [http://localhost:5000](http://localhost:5000)

## Application Structure

- `/src/ui/app.py` - Main Flask application
- `/src/ui/templates/` - HTML templates
  - `base.html` - Base template with common styling
  - `index.html` - Main page with pipeline selector
  - `host_*.html` - Templates for host credential management
  - `endpoint_detail.html` - Endpoint schema visualization
  - `pipeline_detail.html` - Pipeline configuration inspection

## Host Credential Management

The application allows you to:

1. View existing host credentials
2. Edit credentials (with environment variable support)
3. Create new host configuration files
4. Delete host configuration files

Environment variables are supported using the `${VAR_NAME}` syntax and will be expanded at runtime.

## Schema Visualization

The application provides:

1. Visual representation of endpoint schemas (both API and database)
2. JSON viewer for exploring complex nested structures
3. Database table schema visualization with field types and constraints
4. API request/response schema visualization

## Pipeline Configuration Inspection

Inspect:

1. Pipeline metadata and basic information
2. Engine configuration details
3. Error handling and monitoring settings
4. Field mappings and transformations
5. Multi-stream configurations

## Development

To modify or extend this application:

1. Add new routes in `app.py`
2. Create or modify templates in the `templates` directory
3. For advanced customization, add CSS/JS files in the `static` directory