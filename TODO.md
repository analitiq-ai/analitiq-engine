Adding API post authentication selections in the pipeline definition so the values can be used in the core.

=====

Schema migration

How does state work and what if state does not match?

wpuld it be best to use factory methods for loading dbs?

This should be in database, probably? Versioned? src/transformations/registry.py
in Destination _prepare_records method 

How is partial failure is treated? If some records failed on destination?

Seem like writing to API is not failing if record cannot be written
=========
