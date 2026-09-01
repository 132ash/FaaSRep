class PassiveAbortException(Exception):
    """The lock manager selected this attempt as the Wait-Die victim."""


class ActiveAbortException(Exception):
    """Application code explicitly aborted the workflow."""
