import os
import json
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for, jsonify, send_from_directory

app = Flask(__name__, template_folder="templates", static_folder="static")

@app.route('/static/js/dist/<path:filename>')
def serve_static_js(filename):
    return send_from_directory('static/js/dist', filename)

@app.route('/static/js/src/<path:filename>')
def serve_static_css(filename):
    return send_from_directory('static/js/src', filename)

EXAMPLES_DIR = Path(__file__).parent.parent / "examples"

def get_pipelines():
    """Get all available pipeline directories."""
    return [d for d in os.listdir(EXAMPLES_DIR) if os.path.isdir(os.path.join(EXAMPLES_DIR, d))]

def get_pipeline_config(pipeline_name):
    """Get pipeline configuration for a specific pipeline."""
    pipeline_dir = os.path.join(EXAMPLES_DIR, pipeline_name)
    
    # Get pipeline_config.json
    config_path = os.path.join(pipeline_dir, "pipeline_config.json")
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            return json.load(f)
    return None

def get_host_files(pipeline_name):
    """Get all host files for a specific pipeline with their types."""
    pipeline_dir = os.path.join(EXAMPLES_DIR, pipeline_name)
    
    # Find all hst_ files
    host_files = [f for f in os.listdir(pipeline_dir) if f.startswith("hst_") and f.endswith(".json")]
    
    # Get the type for each host file
    result = []
    for file in host_files:
        host_path = os.path.join(pipeline_dir, file)
        try:
            with open(host_path, "r") as f:
                host_data = json.load(f)
                result.append({"file": file, "type": host_data.get("type", "")})  
        except Exception:
            result.append({"file": file, "type": ""})  # If unable to read file or missing type
            
    return result

def get_endpoint_files(pipeline_name):
    """Get all endpoint files for a specific pipeline."""
    pipeline_dir = os.path.join(EXAMPLES_DIR, pipeline_name)
    
    # Find all ep_ files
    return [f for f in os.listdir(pipeline_dir) if f.startswith("ep_") and f.endswith(".json")]

def get_host_config(pipeline_name, host_file):
    """Get the content of a host file."""
    pipeline_dir = os.path.join(EXAMPLES_DIR, pipeline_name)
    host_path = os.path.join(pipeline_dir, host_file)
    
    if os.path.exists(host_path):
        with open(host_path, "r") as f:
            return json.load(f)
    return None

def get_endpoint_config(pipeline_name, endpoint_file):
    """Get the content of an endpoint file."""
    pipeline_dir = os.path.join(EXAMPLES_DIR, pipeline_name)
    endpoint_path = os.path.join(pipeline_dir, endpoint_file)
    
    if os.path.exists(endpoint_path):
        with open(endpoint_path, "r") as f:
            return json.load(f)
    return None

def update_host_config(pipeline_name, host_file, config):
    """Update a host configuration file."""
    pipeline_dir = os.path.join(EXAMPLES_DIR, pipeline_name)
    host_path = os.path.join(pipeline_dir, host_file)
    
    with open(host_path, "w") as f:
        json.dump(config, f, indent=2)
    
    return True

def create_host_config(pipeline_name, host_id, config):
    """Create a new host configuration file."""
    pipeline_dir = os.path.join(EXAMPLES_DIR, pipeline_name)
    host_path = os.path.join(pipeline_dir, f"hst_{host_id}.json")
    
    with open(host_path, "w") as f:
        json.dump(config, f, indent=2)
    
    return True

def delete_host_config(pipeline_name, host_file):
    """Delete a host configuration file."""
    pipeline_dir = os.path.join(EXAMPLES_DIR, pipeline_name)
    host_path = os.path.join(pipeline_dir, host_file)
    
    if os.path.exists(host_path):
        os.remove(host_path)
        return True
    return False

@app.route("/")
def index():
    """Main page with pipeline selector."""
    pipelines = get_pipelines()
    selected = request.args.get("pipeline", pipelines[0] if pipelines else None)
    
    if not selected and pipelines:
        return redirect(url_for("index", pipeline=pipelines[0]))
    
    pipeline_config = get_pipeline_config(selected) if selected else None
    host_files = get_host_files(selected) if selected else []
    endpoint_files = get_endpoint_files(selected) if selected else []
    
    # Get host types for source and destination
    src_host_type = None
    dst_host_type = None
    if pipeline_config:
        src_host_id = pipeline_config.get('src', {}).get('host_id')
        dst_host_id = pipeline_config.get('dst', {}).get('host_id')
        
        for host in host_files:
            if src_host_id and host.get('file') == f'hst_{src_host_id}.json':
                src_host_type = host.get('type')
            if dst_host_id and host.get('file') == f'hst_{dst_host_id}.json':
                dst_host_type = host.get('type')
    
    return render_template(
        "index.html",
        pipelines=pipelines,
        selected=selected,
        pipeline_config=pipeline_config,
        host_files=host_files,
        endpoint_files=endpoint_files,
        src_host_type=src_host_type,
        dst_host_type=dst_host_type
    )

@app.route("/host/<pipeline_name>/<host_file>")
def view_host(pipeline_name, host_file):
    """View host configuration details."""
    host_config = get_host_config(pipeline_name, host_file)
    pipelines = get_pipelines()
    endpoint_files = get_endpoint_files(pipeline_name)
    
    # Find related endpoints for this host
    related_endpoints = []
    pipeline_config = get_pipeline_config(pipeline_name)
    
    if host_config and pipeline_config:
        # Get host ID from filename
        host_id = host_file.replace('hst_', '').replace('.json', '')
        
        # Check if this host is used as source or destination
        is_src = pipeline_config.get('src', {}).get('host_id') == host_id
        is_dst = pipeline_config.get('dst', {}).get('host_id') == host_id
        
        # If host is used in streams, find related endpoints
        if 'streams' in pipeline_config:
            for stream_id, stream in pipeline_config.get('streams', {}).items():
                if is_src and 'src' in stream and 'endpoint_id' in stream['src']:
                    endpoint_id = stream['src']['endpoint_id']
                    endpoint_file = f"ep_{endpoint_id}.json"
                    if endpoint_file in endpoint_files:
                        endpoint_config = get_endpoint_config(pipeline_name, endpoint_file)
                        if endpoint_config:
                            related_endpoints.append({
                                'file': endpoint_file,
                                'config': endpoint_config,
                                'role': 'source'
                            })
                
                if is_dst and 'dst' in stream and 'endpoint_id' in stream['dst']:
                    endpoint_id = stream['dst']['endpoint_id']
                    endpoint_file = f"ep_{endpoint_id}.json"
                    if endpoint_file in endpoint_files:
                        endpoint_config = get_endpoint_config(pipeline_name, endpoint_file)
                        if endpoint_config:
                            related_endpoints.append({
                                'file': endpoint_file,
                                'config': endpoint_config,
                                'role': 'destination'
                            })
    
    return render_template(
        "host_detail.html", 
        pipeline_name=pipeline_name, 
        host_file=host_file, 
        host_config=host_config,
        related_endpoints=related_endpoints,
        pipelines=pipelines,
        selected=pipeline_name
    )

@app.route("/host/<pipeline_name>/<host_file>/edit", methods=["GET", "POST"])
def edit_host(pipeline_name, host_file):
    """Edit host configuration."""
    if request.method == "POST":
        try:
            config = json.loads(request.form.get("config"))
            update_host_config(pipeline_name, host_file, config)
            return redirect(url_for("view_host", pipeline_name=pipeline_name, host_file=host_file))
        except json.JSONDecodeError:
            return "Invalid JSON", 400
    
    host_config = get_host_config(pipeline_name, host_file)
    return render_template("host_edit.html", pipeline_name=pipeline_name, host_file=host_file, host_config=json.dumps(host_config, indent=2))

@app.route("/host/<pipeline_name>/create", methods=["GET", "POST"])
def create_host(pipeline_name):
    """Create a new host configuration."""
    if request.method == "POST":
        try:
            host_id = request.form.get("host_id")
            config = json.loads(request.form.get("config"))
            create_host_config(pipeline_name, host_id, config)
            return redirect(url_for("view_host", pipeline_name=pipeline_name, host_file=f"hst_{host_id}.json"))
        except json.JSONDecodeError:
            return "Invalid JSON", 400
    
    # Provide a template for new host config
    template = {
        "base_url": "https://example.com",
        "type": "api",
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "${API_TOKEN}"
        },
        "timeout": 30,
        "rate_limit": {
            "max_requests": 60,
            "time_window": 60
        }
    }
    
    return render_template(
        "host_create.html", 
        pipeline_name=pipeline_name, 
        template_config=json.dumps(template, indent=2)
    )

@app.route("/host/<pipeline_name>/<host_file>/delete", methods=["POST"])
def delete_host(pipeline_name, host_file):
    """Delete a host configuration."""
    delete_host_config(pipeline_name, host_file)
    return redirect(url_for("index", pipeline=pipeline_name))

@app.route("/endpoint/<pipeline_name>/<endpoint_file>")
def view_endpoint(pipeline_name, endpoint_file):
    """View endpoint configuration details."""
    endpoint_config = get_endpoint_config(pipeline_name, endpoint_file)
    pipelines = get_pipelines()
    return render_template(
        "endpoint_detail.html", 
        pipeline_name=pipeline_name, 
        endpoint_file=endpoint_file, 
        endpoint_config=endpoint_config,
        pipelines=pipelines,
        selected=pipeline_name
    )

@app.route("/pipeline/<pipeline_name>")
def view_pipeline(pipeline_name):
    """View pipeline configuration details."""
    pipeline_config = get_pipeline_config(pipeline_name)
    host_files = get_host_files(pipeline_name)
    endpoint_files = get_endpoint_files(pipeline_name)
    pipelines = get_pipelines()
    
    # Get field type information from endpoint schemas
    field_types = {}
    
    # Data for field mappings
    mapping_data_by_stream = {}
    
    # If we have streams, process them
    if pipeline_config and 'streams' in pipeline_config:
        for stream_id, stream in pipeline_config.get('streams', {}).items():
            # Initialize mapping data for this stream
            mapping_data_by_stream[stream_id] = {
                'source_fields': {},
                'destination_fields': {},
                'mappings': {}
            }
            
            # Get source endpoint schema
            source_endpoint_config = None
            dest_endpoint_config = None
            
            if 'src' in stream and 'endpoint_id' in stream['src']:
                source_endpoint_id = stream['src']['endpoint_id']
                source_endpoint_file = f"ep_{source_endpoint_id}.json"
                
                if source_endpoint_file in endpoint_files:
                    source_endpoint_config = get_endpoint_config(pipeline_name, source_endpoint_file)
                    
                    # For database endpoints with table_schema
                    if source_endpoint_config and 'table_schema' in source_endpoint_config and 'properties' in source_endpoint_config['table_schema']:
                        required_fields = source_endpoint_config['table_schema'].get('required', [])
                        
                        for field_name, field_info in source_endpoint_config['table_schema']['properties'].items():
                            field_type = {
                                'type': field_info.get('database_type') or field_info.get('type', 'unknown'),
                                'format': field_info.get('format', ''),
                                'required': field_name in required_fields or field_info.get('nullable') is False,
                            }
                            field_types[field_name] = field_type
                            mapping_data_by_stream[stream_id]['source_fields'][field_name] = field_type
                    
                    # For API endpoints with response_schema
                    elif source_endpoint_config and 'response_schema' in source_endpoint_config:
                        schema = source_endpoint_config['response_schema']
                        
                        # Handle array schema
                        if schema.get('type') == 'array' and 'items' in schema:
                            schema = schema['items']
                        
                        # Handle object schema with properties
                        if schema.get('type') == 'object' and 'properties' in schema:
                            required_fields = schema.get('required', [])
                            
                            for field_name, field_info in schema['properties'].items():
                                field_type = {
                                    'type': field_info.get('type', 'unknown'),
                                    'format': field_info.get('format', ''),
                                    'required': field_name in required_fields,
                                }
                                field_types[field_name] = field_type
                                mapping_data_by_stream[stream_id]['source_fields'][field_name] = field_type
            
            # Get destination endpoint schema
            if 'dst' in stream and 'endpoint_id' in stream['dst']:
                dest_endpoint_id = stream['dst']['endpoint_id']
                dest_endpoint_file = f"ep_{dest_endpoint_id}.json"
                
                if dest_endpoint_file in endpoint_files:
                    dest_endpoint_config = get_endpoint_config(pipeline_name, dest_endpoint_file)
                    
                    # For database endpoints with table_schema
                    if dest_endpoint_config and 'table_schema' in dest_endpoint_config and 'properties' in dest_endpoint_config['table_schema']:
                        required_fields = dest_endpoint_config['table_schema'].get('required', [])
                        
                        for field_name, field_info in dest_endpoint_config['table_schema']['properties'].items():
                            field_type = {
                                'type': field_info.get('database_type') or field_info.get('type', 'unknown'),
                                'format': field_info.get('format', ''),
                                'required': field_name in required_fields or field_info.get('nullable') is False,
                            }
                            mapping_data_by_stream[stream_id]['destination_fields'][field_name] = field_type
                    
                    # For API endpoints with request schema (in endpoint)
                    elif dest_endpoint_config:
                        # Try to derive destination fields from field mappings
                        if 'mapping' in stream and 'field_mappings' in stream['mapping']:
                            for source_field, mapping in stream['mapping']['field_mappings'].items():
                                target_field = mapping.get('target')
                                if target_field:
                                    mapping_data_by_stream[stream_id]['destination_fields'][target_field] = {
                                        'type': 'string',  # Default type
                                        'required': False
                                    }
            
            # Add mappings from field_mappings
            if 'mapping' in stream and 'field_mappings' in stream['mapping']:
                mapping_data_by_stream[stream_id]['mappings'] = stream['mapping']['field_mappings']
            
            # Add mappings from computed_fields
            if 'mapping' in stream and 'computed_fields' in stream['mapping']:
                mapping_data_by_stream[stream_id]['computed_fields'] = stream['mapping']['computed_fields']
                # Add computed fields to destination fields if not already there
                for field_name in stream['mapping']['computed_fields']:
                    if field_name not in mapping_data_by_stream[stream_id]['destination_fields']:
                        mapping_data_by_stream[stream_id]['destination_fields'][field_name] = {
                            'type': 'computed',
                            'format': '',
                            'required': False
                        }
            
            # Convert mappings to JSON for frontend
            # Prepare field mappings data for display
            # Ensure all components are properly structured
            processed_data = {
                'source_fields': mapping_data_by_stream[stream_id].get('source_fields', {}),
                'destination_fields': mapping_data_by_stream[stream_id].get('destination_fields', {}),
                'mappings': {},
                'computed_fields': mapping_data_by_stream[stream_id].get('computed_fields', {})
            }
            
            # Process mappings to ensure they're properly formatted
            for source_field, mapping in mapping_data_by_stream[stream_id].get('mappings', {}).items():
                if isinstance(mapping, dict) and mapping.get('target'):
                    target = mapping.get('target', '')
                    # Only add mappings with valid target fields and make sure the target exists in destination fields
                    if target in processed_data['destination_fields']:
                        processed_data['mappings'][source_field] = {
                            'target': target,
                            'transformations': mapping.get('transformations', []),
                            'validation': mapping.get('validation', {})
                        }
                        print(f"Adding mapping from {source_field} to {target}")
                    else:
                        print(f"Warning: Target field {target} not found in destination fields, skipping mapping from {source_field}")
            
            # Debug output
            print(f"Stream {stream_id} mapping data:")
            print(f"  Source fields: {len(processed_data['source_fields'])}")
            print(f"  Destination fields: {len(processed_data['destination_fields'])}")
            print(f"  Mappings: {len(processed_data['mappings'])}")
            
            # Store processed data
            mapping_data_by_stream[stream_id].update(processed_data)
    
    return render_template(
        "pipeline_detail.html", 
        pipeline_name=pipeline_name, 
        pipeline_config=pipeline_config,
        host_files=host_files,
        endpoint_files=endpoint_files,
        pipelines=pipelines,
        selected=pipeline_name,
        field_types=field_types,
        mapping_data_by_stream=mapping_data_by_stream
    )

if __name__ == "__main__":
    app.run(debug=True)