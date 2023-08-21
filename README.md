# Dynamic Database Seeding

### Description

Developers often utilize tooling that will seed data. Seeding data is the process of (intelligently) creating structured data in a meaningful way. This is especially useful when the volume of data that needs to be generated is high.

### Acceptance Criteria
 - Create a script that will take all the files from the `source-data` directory that will:
   - Create a table using the filename.
   - Insert all data from that file into the table. Source files contain the column names on the first row. All subsequent rows are the data.
   - Infer the column type from the header row of the files in the `source-data` directory when creating your table.
 - Upload your code in a folder in [Google Drive](https://drive.google.com/drive/u/1/folders/1v123mHUvUbiy39T5oEtDpas4fz9PPYuz).

### Important Notes
 - The source files are pipe-delimited.
 - The data in the source files should not be corrupted in any way (if they are, let me know).
 - It's the dev's discretion on what other column data types should be accounted for if not present in the source files. If the source files do not have any data that warrants handling for the `TEXT` MySQL column data type, the dev will need to determine if they want to futureproof their code for that data type.
