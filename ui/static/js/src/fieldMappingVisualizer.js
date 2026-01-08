// Field Mapping Visualizer
// A lightweight visualization component for field mappings

class FieldMappingVisualizer {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
    if (!this.container) {
      console.error(`Container element with ID ${containerId} not found`);
      return;
    }
    this.sourceFields = [];
    this.destFields = [];
    this.mappings = {};
    this.computedFields = {};
    this.init();
  }

  init() {
    // Create container elements
    this.container.innerHTML = '';
    this.container.classList.add('field-mapping-visualizer');
    
    // Create the visualization container
    this.visContainer = document.createElement('div');
    this.visContainer.className = 'mapping-vis-container';
    this.container.appendChild(this.visContainer);
    
    // Create source fields container
    this.sourceContainer = document.createElement('div');
    this.sourceContainer.className = 'source-fields';
    this.visContainer.appendChild(this.sourceContainer);
    
    // Create mapping lines container (SVG)
    this.svgContainer = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    this.svgContainer.setAttribute('class', 'mapping-lines');
    this.visContainer.appendChild(this.svgContainer);
    
    // Create destination fields container
    this.destContainer = document.createElement('div');
    this.destContainer.className = 'dest-fields';
    this.visContainer.appendChild(this.destContainer);
  }

  setData(sourceFields, destFields, mappings, computedFields) {
    this.sourceFields = sourceFields;
    this.destFields = destFields;
    this.mappings = mappings;
    this.computedFields = computedFields || {};
    this.render();
  }

  render() {
    this.renderSourceFields();
    this.renderDestFields();
    // Need to wait for DOM to be updated before drawing lines
    setTimeout(() => this.drawMappingLines(), 100);
  }

  renderSourceFields() {
    this.sourceContainer.innerHTML = '<h5>Source Fields</h5>';
    const fieldList = document.createElement('ul');
    fieldList.className = 'field-list source-field-list';
    
    this.sourceFields.forEach(field => {
      const item = document.createElement('li');
      item.className = 'field-item source-field';
      item.setAttribute('data-field', field.name);
      
      const fieldName = document.createElement('span');
      fieldName.className = 'field-name';
      fieldName.textContent = field.name;
      
      const fieldType = document.createElement('span');
      fieldType.className = 'field-type badge bg-secondary ms-2';
      fieldType.textContent = field.type || 'unknown';
      
      item.appendChild(fieldName);
      item.appendChild(fieldType);
      
      // Add visual indicator if field is mapped
      if (this.mappings[field.name]) {
        item.classList.add('mapped-field');
      }
      
      fieldList.appendChild(item);
    });
    
    this.sourceContainer.appendChild(fieldList);
  }

  renderDestFields() {
    this.destContainer.innerHTML = '<h5>Destination Fields</h5>';
    const fieldList = document.createElement('ul');
    fieldList.className = 'field-list dest-field-list';
    
    this.destFields.forEach(field => {
      const item = document.createElement('li');
      item.className = 'field-item dest-field';
      item.setAttribute('data-field', field.name);
      
      const fieldName = document.createElement('span');
      fieldName.className = 'field-name';
      fieldName.textContent = field.name;
      
      const fieldType = document.createElement('span');
      fieldType.className = 'field-type badge bg-secondary ms-2';
      fieldType.textContent = field.type || 'unknown';
      
      item.appendChild(fieldName);
      item.appendChild(fieldType);
      
      // Check if this field is a target in any mapping
      const isMapped = Object.values(this.mappings).some(targetField => 
        targetField === field.name
      );
      
      if (isMapped) {
        item.classList.add('mapped-field');
      }
      
      fieldList.appendChild(item);
    });
    
    this.destContainer.appendChild(fieldList);
  }

  drawMappingLines() {
    // Clear existing lines
    this.svgContainer.innerHTML = '';
    
    // Set SVG dimensions
    this.svgContainer.setAttribute('width', '100%');
    this.svgContainer.setAttribute('height', '100%');
    
    // Create lines for each mapping
    Object.entries(this.mappings).forEach(([sourceField, destField]) => {
      const sourceEl = this.sourceContainer.querySelector(`li[data-field="${sourceField}"]`);
      const destEl = this.destContainer.querySelector(`li[data-field="${destField}"]`);
      
      if (!sourceEl || !destEl) return;
      
      this.drawLine(sourceEl, destEl, '#007bff');
    });
    
    // Handle computed fields - draw from outside container to destination field
    Object.entries(this.computedFields).forEach(([fieldName, fieldConfig]) => {
      const destEl = this.destContainer.querySelector(`li[data-field="${fieldName}"]`);
      
      if (!destEl) return;
      
      // Create a virtual source element for computed fields
      const virtualSourceEl = document.createElement('div');
      virtualSourceEl.style.position = 'absolute';
      virtualSourceEl.style.left = '0';
      virtualSourceEl.style.top = destEl.getBoundingClientRect().top + 'px';
      virtualSourceEl.style.height = destEl.getBoundingClientRect().height + 'px';
      this.container.appendChild(virtualSourceEl);
      
      // Draw a special line for computed fields
      this.drawLine(virtualSourceEl, destEl, '#28a745', true);
      
      // Remove the virtual element after drawing
      this.container.removeChild(virtualSourceEl);
    });
    
    // Add arrow marker definition
    const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
    const marker = document.createElementNS('http://www.w3.org/2000/svg', 'marker');
    marker.setAttribute('id', 'arrow');
    marker.setAttribute('viewBox', '0 0 10 10');
    marker.setAttribute('refX', '5');
    marker.setAttribute('refY', '5');
    marker.setAttribute('markerWidth', '6');
    marker.setAttribute('markerHeight', '6');
    marker.setAttribute('orient', 'auto');
    
    const arrow = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    arrow.setAttribute('d', 'M 0 0 L 10 5 L 0 10 z');
    arrow.setAttribute('fill', '#007bff');
    
    marker.appendChild(arrow);
    defs.appendChild(marker);
    this.svgContainer.appendChild(defs);
  },
  
  drawLine(sourceEl, destEl, color, isComputed = false) {
      
      // Get positions
      const sourceRect = sourceEl.getBoundingClientRect();
      const destRect = destEl.getBoundingClientRect();
      const containerRect = this.visContainer.getBoundingClientRect();
      
      // Calculate coordinates relative to the SVG container
      const x1 = sourceRect.right - containerRect.left;
      const y1 = sourceRect.top + (sourceRect.height / 2) - containerRect.top;
      const x2 = destRect.left - containerRect.left;
      const y2 = destRect.top + (destRect.height / 2) - containerRect.top;
      
      // Create the path
      const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      
      // Create a bezier curve
      const dx = x2 - x1;
      const bezierX = dx * 0.5;
      const pathData = `M ${x1},${y1} C ${x1 + bezierX},${y1} ${x2 - bezierX},${y2} ${x2},${y2}`;
      
      path.setAttribute('d', pathData);
      path.setAttribute('stroke', color);
      path.setAttribute('stroke-width', '2');
      path.setAttribute('fill', 'none');
      path.setAttribute('marker-end', 'url(#arrow)');
      
      if (isComputed) {
        path.setAttribute('stroke-dasharray', '4,4');
        // Add a label for computed field
        const textElement = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        const midX = x1 + (x2 - x1) / 2;
        const midY = y1 + (y2 - y1) / 2;
        textElement.setAttribute('x', midX);
        textElement.setAttribute('y', midY - 5);
        textElement.setAttribute('font-size', '10px');
        textElement.setAttribute('text-anchor', 'middle');
        textElement.setAttribute('fill', color);
        textElement.textContent = 'computed';
        this.svgContainer.appendChild(textElement);
      }
      
      this.svgContainer.appendChild(path);
  }

  // Handle window resize events to redraw the lines
  initResizeHandler() {
    window.addEventListener('resize', () => {
      this.drawMappingLines();
    });
  }
}

// Export the visualizer
window.FieldMappingVisualizer = FieldMappingVisualizer;