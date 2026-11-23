# Reconcile full sequences and preserve source identifiers

The initial pipeline covers UniProt and PDB. Researchers need a duplicate-free export while retaining the ability to trace a sequence to its sources. Use a 64-bit checksum to group candidate duplicates, compare their full sequences for equality, and retain all contributing source identifiers on each resulting row.

Compute UniProt-compatible CRC64 over the uppercase one-letter sequence with formatting whitespace removed. Preserve residue letters, including X, and export the checksum as 16 hexadecimal digits. Identical sequences merge even when their identifiers differ. Distinct sequences sharing a checksum remain distinct.

Use the deposited protein entity’s canonical one-letter sequence for PDB records. A shorter construct and its longer parent each retain their own sequence result. Preserve their relationship when supported by the source mapping.

Search both sources and take the union of qualifying records. For the first demo, apply the source organism filter and require the literal motif to occur in the exported sequence. A qualifying record from either source can appear alone.
