there is "connector" section with `connector_type` attribute. That attribute will be set as either
one of api|db|s3|file. We can add a type stdout for local testing to see the
stream run in console output. To further specify databases, the connectors
would have a driver attribute. For example, for postgres it will be `"driver":
      "postgresql",`. This `driver` attribute will define the database type and will
give you a hint what king of sqlalchemy dialect to load in this specific case.
For S3 and file I can add file_format attribute that would say `parquet` or
`csv`.  This will be defined when user sets up connection.

Agents need to obtain API time zone from documentation
Agents need to determine tie breakers

Connector types:
- api
- db
- s3
- file
- stdout


Filters are defined in array based format:
```shell
  {                                                                                                                                                                                   
    "filters": [                                                                                                                                                                      
      {"field": "status", "op": "eq", "value": "active"},                                                                                                                             
      {"or": [                                                                                                                                                                        
        {"field": "priority", "op": "eq", "value": "high"},                                                                                                                           
        {"field": "amount", "op": "gt", "value": "1000"}                                                                                                                              
      ]}                                                                                                                                                                              
    ]                                                                                                                                                                                 
  }  
```