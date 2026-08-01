"""The CDK's stdout connector family: one class, write only.

A package for a single class so every kind the CDK ships reads the same
way -- ``cdk.<kind>.generic`` -- and no consumer has to remember which
kinds got a package and which got a bare module.
"""

from .generic import GenericStdoutConnector

__all__ = ["GenericStdoutConnector"]
