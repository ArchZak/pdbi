# Preserve the export from each query run

Databanks change between runs. Downloading a saved result returns the original bytes, while running its structured query again searches current data and produces a separate result. Keeping the original export lets a researcher revisit a dataset after upstream records change.

Write fetched records to Parquet during ingestion, starting with the development build. Offer both Parquet and CSV downloads of the same reconciled result. Either format must be available from the saved run without querying the sources again. Repeated downloads of a given format preserve its bytes.

The first CSV download request triggers streaming conversion from the saved Parquet file. Publish the CSV after conversion completes and retain it for subsequent downloads. Show the researcher that the CSV is being prepared while conversion runs.

Both downloads contain crc64, sequence, and source_records. Preserve CRC64 as 16-character hexadecimal text. Parquet stores source_records as a list of records with source and id fields. CSV encodes the same list as JSON within one field. Order rows by CRC64 and then sequence, and order source records consistently across formats. Verify that reading either export yields the same rows and source identifiers.
