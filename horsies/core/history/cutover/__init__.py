"""The offline cutover program.

Operator-driven and staged; not part of the ordinary migration chain.
The ordinary chain (through the emission versions) installs state the
old fleet ignores; this package is where programs arrive with their
owners and, later in the program, where the old shapes are tightened
away. Stage order, reversibility, and the point of no return are
governed by the cutover plan; each module here is one stage's body.
"""
