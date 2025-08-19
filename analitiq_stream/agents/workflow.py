#!/usr/bin/env python3
"""
# Agentic Workflow: Research Assistant

This example demonstrates an agentic workflow using Strands agents with web research capabilities.

## Key Features
- Specialized agent roles working in sequence
- Direct passing of information between workflow stages
- Web research using http_request and retrieve tools
- Fact-checking and information synthesis

## How to Run
1. Navigate to example directory
2. Run: python research_assistant.py
3. Enter queries or claims at the prompt

## Example Queries
- "Thomas Edison invented the light bulb"
- "Tuesday comes before Monday in the week"

## Workflow Process
1. Researcher Agent: Gathers web information using multiple tools
2. Analyst Agent: Verifies facts and synthesizes findings
3. Writer Agent: Creates final report
"""

import logging
import re
import json
import uuid

from strands import Agent
from strands_tools import http_request

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

model_id = "eu.anthropic.claude-sonnet-4-20250514-v1:0"
system_template = """
SYSTEM / ROLE
You are an expert API analyst. Read the provided API documentation and consolidate the details for ONE endpoint into a single JSON object. Be precise and conservative: only include fields and behaviors that are explicitly documented.

INPUT
<<<API_DOCS>>>
{{PASTE_OR_SUMMARIZE_THE_RELEVANT_API_DOCUMENTATION_HERE}}
<<<TARGET_ENDPOINT>>>
{{PUT_THE_ENDPOINT_PATH_OR DESCRIPTION HERE, E.G. "/v1/transfers"}}
<<<END_INPUT>>>

INSTRUCTIONS
1) Identify the HTTP method for the target endpoint. If multiple methods exist, use the one that retrieves data (usually GET). If truly unspecified, set "method" to null.
2) Determine if and how the endpoint paginates. Detect common patterns:
   - Offset/Page-based: page, per_page, page_size, offset, limit.
   - Cursor/Token-based: cursor, starting_after, ending_before, next_cursor, nextPageToken, page_token, continuationToken.
   - Time/Window-based: since, until, intervalStart, intervalEnd.
   - Link headers: HTTP Link header with rel="next".
   If no pagination is documented, set "pagination": null. Otherwise, fill the pagination object carefully with the parameter names shown in docs.
3) List ALL response attributes with types in proper JSON Schema (Draft 2020-12) format. 
   - If the endpoint returns a collection, model the full response shape; usually {"type":"array","items":{...}}. 
   - Use nested object properties (do NOT use dot notation like "details.merchant.name"; instead nest "details" → "merchant" → "name").
   - Types: "string", "integer", "number", "boolean", "object", "array", "null".
   - Use "format": "date-time" for ISO-8601 timestamps; "format": "uri", "email", etc., if documented.
   - Use "enum" when the doc lists allowed values.
   - Include "required" only for fields explicitly marked required in the response; otherwise omit from "required".
   - If an attribute can be missing or null, model with "nullable": true (or oneOf with {"type":"null"}).
4) Specify available query parameters usable as filters on the endpoint. For each filter:
   - Include its "type", whether it’s "required" for requests, allowed "operators" if documented (e.g., ["eq","neq","gt","gte","lt","lte","in","like","between","contains"]), and "enum" if constrained to specific values. Add a short "description" if available.
5) Do not invent behavior. If something is unclear or not present in the docs, omit it or set null. Do not include commentary outside of the JSON.

OUTPUT
Return EXACTLY one JSON object with this shape and keys, and nothing else.

{
  "endpoint": "{{STRING — the endpoint path exactly as documented, e.g., \"/v1/transfers\"}}",
  "type": "api",
  "method": "{{STRING | null — e.g., \"GET\"}}",

  "pagination": {{EITHER null OR the object below}},
  "pagination_if_present": {
    "type": "{{\"cursor\" | \"offset\" | \"page\" | \"time\" | \"link\"}}",
    "params": {
      "limit_param": "{{STRING | null}}",
      "max_limit": {{NUMBER | null}},
      "page_param": "{{STRING | null}}",
      "offset_param": "{{STRING | null}}",
      "cursor_param": "{{STRING | null}}",
      "next_cursor_field": "{{STRING | null — name of response field that carries the next cursor, if any}}",
      "time_window_params": {
        "start_param": "{{STRING | null}}",
        "end_param": "{{STRING | null}}"
      },
      "uses_link_header": {{true | false}}
    }
  },

  "response_schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "{{Endpoint Human Title}}",
    "description": "{{Optional short description of what the endpoint returns}}",
    "type": "{{\"object\" | \"array\"}}",
    {{IF type is "array", include:}}
    "items": {
      "type": "object",
      "properties": {
        "{{fieldName}}": {
          "type": "{{string|integer|number|boolean|object|array|null}}",
          "format": "{{optional format like date-time}}",
          "enum": ["{{optional allowed value}}"],
          "description": "{{optional}}",
          "nullable": {{true|false}},
          "items": {{IF array, describe the item schema}},
          "properties": {{IF object, nest properties here}}
        }
        {{, repeat for all fields}}
      },
      "required": ["{{only fields explicitly marked required in the response}}"]
    },
    {{ELSE IF type is "object", include:}}
    "properties": {
      "{{fieldName}}": { ... as above ... }
    },
    "required": ["{{as documented}}"]
  },

  "filters": {
    "{{filter_param_name}}": {
      "type": "{{string|integer|number|boolean|array|object}}",
      "required": {{true|false}},
      "operators": ["{{eq|neq|gt|gte|lt|lte|in|between|contains|like}}"],
      "enum": ["{{optional allowed values}}"],
      "description": "{{optional short help text}}",
      "example": "{{optional example value}}"
    }
    {{, repeat for each documented filterable query parameter}}
  }
}
"""

prompt_template = """
Connect to website {url} to examine the API documentation for specifications required for the user query `{user_input}`.
"""

hint_template = """
The user send you a hint: {hint}
"""


def extract_and_save_content(content, filename_prefix):
    """
    Extract JSON content between ```json and ``` markers and save it to .json file.
    Save the remaining content to .txt file.
    
    Args:
        content: The text content to process
        filename_prefix: Prefix for output files (e.g., 'src' or 'dst')
    """
    json_pattern = r'```json\s*(.*?)\s*```'
    json_matches = re.findall(json_pattern, content, re.DOTALL)

    request_uuid = uuid.uuid4()
    file_name = f"ep_{request_uuid}"

    if json_matches:
        json_content = json_matches[0].strip()
        try:
            parsed_json = json.loads(json_content)
            with open(f"{file_name}.json", 'w', encoding='utf-8') as f:
                json.dump(parsed_json, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved JSON content to {file_name}.json")
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON found, saving as text: {e}")
            with open(f"{file_name}.error", 'w', encoding='utf-8') as f:
                f.write(json_content)
    
    remaining_content = re.sub(json_pattern, '', content, flags=re.DOTALL).strip()
    
    with open(f"{file_name}.txt", 'w', encoding='utf-8') as f:
        f.write(remaining_content)
    logger.info(f"Saved remaining content to {filename_prefix}.txt")


def run_web_agent(name: str, url: str, user_input: str, user_hint:str = None):
    # Agent with enhanced web capabilities
    logger.info(f"{name} Web Agent working...")

    src_agent = Agent(
        model=model_id,
        system_prompt=system_template,
        callback_handler=None,
        tools=[http_request],
    )

    prompt = prompt_template.format(url=url,user_input=user_input)

    if user_hint:
        prompt = prompt + '\n' + hint_template.format(hint=user_hint)

    agent_response = src_agent(prompt)

    # Extract only the relevant content from the researcher response
    findings = str(agent_response)

    # Extract and save JSON/text content from src_findings
    extract_and_save_content(findings, name)

    logger.info("Research complete")

    return findings

def run_research_workflow(user_input, src_url, dst_url):
    """
    Run a three-agent workflow for research and fact-checking with web sources.
    Shows progress logs during execution but presents only the final report to the user.

    Args:
        user_input: User request

    Returns:
        str: The final report from the Writer Agent
    """

    logger.info(f"Processing: '{user_input}'")

    src_response = run_web_agent('src', src_url, user_input, "Do not use the endpoint `/v1/profiles/{{profileId}}/balance-statements/{{balanceId}}/statement.json`, but use endpoint `/v1/transfers`.")
    dst_response = run_web_agent('dst', dst_url, user_input)



    logger.info("Passing source and destination findings to Summary Agent...")

    # Step 3: Writer Agent to create final summary
    logger.info("Step 3: Summary Agent creating final specifications...")

    writer_agent = Agent(
        model=model_id,
        system_prompt=(
            "You are an API Expert Agent that creates clear mapping specifications to stream data from one API to another API."
        ),
    )

    # Execute the Writer Agent with the analysis (output is shown to user)
    final_specs = writer_agent(
        "1. Review user task.\n"
        "2. Compare source API specification to destination API specifications and create a clear mapping to accomplish user task.\n"
        f"Task: '{user_input}'.\n\n"
        f"Source API Specification: '{src_findings}'.\n\n"
        f"Destination API Specification: '{dst_findings}'.\n\n"
    )

    logger.info("Report creation complete")

    # Return the final report
    return final_specs


if __name__ == "__main__":
    # Print welcome message
    print("\nAgentic Workflow: Research Assistant\n")
    print("This demo shows Strands agents in a workflow with web research.")
    print("\nExamples:")
    print('- "I want to export my transactions from my EUR account in Wise and import them into a bank account I set up in SevDesk."')

    # Interactive loop
    while True:
        try:
            user_input = input("\n> ")
            if user_input.lower() == "exit":
                print("\nGoodbye!")
                break

            src_url = 'https://docs.wise.com/api-docs/api-reference'
            dst_url = 'https://api.sevdesk.de/'

            # Process the input through the workflow of agents
            final_report = run_research_workflow(user_input, src_url, dst_url)
        except KeyboardInterrupt:
            print("\n\nExecution interrupted. Exiting...")
            break
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            print("Please try a different request.")
