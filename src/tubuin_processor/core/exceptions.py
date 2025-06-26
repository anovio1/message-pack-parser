"""This module defines custom, specific exceptions for the parser application."""
class ParserError(Exception):
    """Base exception class for all errors raised by this parser."""
    pass
class FileIngestionError(ParserError):
    """Raised when there is an error finding or reading input files."""
    pass
class CacheError(ParserError):
    """Base class for cache-related errors."""
    pass
class CacheWriteError(CacheError):
    """Raised when writing to the cache fails."""
    pass
class CacheReadError(CacheError):
    """Raised when reading from the cache fails."""
    pass
class CacheValidationError(CacheError):
    """Raised when cached data is found to be stale or invalid."""
    pass
class DecodingError(ParserError):
    """Raised for errors during the MessagePack decoding stage."""
    pass
class SchemaValidationError(ParserError):
    """Raised when data fails to validate against a Pydantic schema."""
    pass
class TransformationError(ParserError):
    """Raised for errors during the value transformation stage."""
    pass
class AggregationError(ParserError):
    """Raised for errors during the data aggregation stage."""
    pass
class OutputGenerationError(ParserError):
    """Raised for errors during the final output generation stage."""
    pass