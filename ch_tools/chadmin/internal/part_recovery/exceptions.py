"""
Exceptions for the part recovery module.
"""


class CriticalLossError(RuntimeError):
    """
    Raised when a critical file (e.g. columns.txt) is missing and cannot be reconstructed.
    The part cannot be recovered without it.

    Also raised for Compact-format parts when the single ``data.bin`` file has missing S3 blobs,
    since all column data is stored together and cannot be partially recovered.
    """
