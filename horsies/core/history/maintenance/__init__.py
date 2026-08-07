"""Archive maintenance: the gate now, the transcode executor later.

Normal archive access and offline maintenance are mutually exclusive by
contract: every terminal transition asserts availability inside its own
transaction, and a maintenance session blocks new transitions rather than
racing them. This package owns that gate; the replacement-partition
transcode executor that takes the gate arrives with its own module.
"""
