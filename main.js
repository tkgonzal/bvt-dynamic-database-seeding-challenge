const fs = require("fs");
const path = require("path");
const mysql = require("mysql2");
const csv = require("csv-parser");

require("dotenv").config();

// Constants
const SOURCE_DATA_DIR = "./source-data";
const SEPARATOR_CHAR = "|";
const FLOAT_RE = /^\d+\.\d+$/;
const INT_RE = /^\d+$/;
const DATE_RE = /^(1[89]|20)\d\d-(0[1-9]|[12][012])-(0[1-9]|[12][0-9]|3[01])$/;

// MySQL Server Connection
const connection = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
});

// File Processing Function
/**
 * Given a delimited value file of source data for a table, creates 
 * that table in the database using the headers as the columns and inserting
 * each row after as entries for the table
 * @param {string} file A string who's name is for a table and has contents 
 * to create source data for that table
 * @param {number} fileNo The index of the file in the array of files from the
 * source-data
 * @param {number} n The amount of files in source-data. Used to check for when
 * to shut down the connection
 */
function processFile(file, fileNo, n) {
    const tableName = path.parse(file).name;
    let columnNames = [];
    const columnTypes = new Map();
    // To hold the values of each entry
    const values = [];

    const fileStream = fs.createReadStream(`${SOURCE_DATA_DIR}/${file}`)
        .pipe(csv({  separator: SEPARATOR_CHAR }))
        .on("headers", headers => {
            columnNames = headers;
            // Initialize each column's type as null and maxLen as -1
            // columnTypes is used for checking the data type of each column,
            // with dataType denoting the data category for the column (i.e. float, 
            // int, date, varchar) and maxLen specifying the maximum length of a 
            // value of that column (used for specifying if the subtype of 
            // that type i.e. if an int will be a TINYINT, INT, or BIGINT)
            columnNames.forEach(col => 
                columnTypes.set(col, {
                    dataType: null,
                    maxLen: -1
                })
            );
        })
        .on("data", chunk => {
            values.push(chunk);
            checkColumnTypes(columnNames, columnTypes, chunk);
        });

    fileStream.on("end", () => {
        connection.query(`DROP TABLE IF EXISTS ${tableName}`);

        connection.query(`CREATE TABLE ${tableName} (
            ${columnNames.map(col => 
                `${col} ${getColumnDataType(columnTypes.get(col))}`)
            }
        )`);

        values.forEach(row => connection.execute(
            `INSERT INTO ${tableName} (${columnNames.join(", ")})
            VALUES (${Array(columnNames.length).fill("?").join(", ")})`, 
            // Empty strings indicate null values
            columnNames.map(col => row[col] || null)
        ));

        console.log(`Finished processing ${file}`);

        // The last file has been processed, end the database connection
        if (fileNo === n - 1) {
            connection.end();
        }
    });

    fileStream.on("error", error => console.log(error));
}

// Helper Functions
/**
 * A processing function that checks columnNames against a set of rowValues in
 * order to determine relevant statistics for what data type to use when 
 * generating a table given the columnNames and rowValues. Does this by updating
 * a Map called columnTypes, which determines a dataType to use for a column and
 * the maximum amount of characters/bytes to consider for a value of 
 * that column.
 * @param {string[]} columnNames The names of the columns for a table
 * @param {Map} columnTypes A map that links columnNames to a statistic tracking
 * object which contains each column's currently known data type and maximum 
 * length as a string (a rough estimate of how many bytes it would need
 * as a data type)
 * @param {rowValues} rowValues An object where the columnNames map to their
 * values in a given row
 */
function checkColumnTypes(columnNames, columnTypes, rowValues) {
    columnNames.forEach(col => {
        const colVal = rowValues[col];

        // If the value of that column for this row is null skip processing
        if (!colVal) {
            return;
        }

        const columnType = columnTypes.get(col);
        checkColumnDataType(columnType, colVal);
        columnType.maxLen = Math.max(columnType.maxLen, colVal.length);
    });
}

/**
 * Checks a column's data type against a value for that column to either
 * identify what type of value the column should be or validate an already
 * identified column type
 * @param {Object} columnType An object which stores for a column which 
 * category of dataType its values fall under, and the maxLen of all values
 * found for that column in the source data
 * @param {string} colVal A value for a column from a database seeding
 * source file
 */
function checkColumnDataType(columnType, colVal) {
    if (columnType.dataType === null) {
        identifyColumnDataType(columnType, colVal);
    } else {
        validateColumnDataType(columnType, colVal);
    }
}

/**
 * To be called when the columnType's data type is null and a non null value
 * for that column is found, sets the initial data type for the column 
 * based on the structure of the value
 * @param {Object} columnType An object which stores for a column which 
 * category of dataType its values fall under, and the maxLen of all values
 * found for that column in the source data
 * @param {string} colVal A value for a column from a database seeding
 * source file
 */
function identifyColumnDataType(columnType, colVal) {
    if (FLOAT_RE.test(colVal)) {
        columnType.dataType = "FLOAT";
    } else if (INT_RE.test(colVal)) {
        columnType.dataType = "INT";
    } else if (DATE_RE.test(colVal)) {
        columnType.dataType = "DATE";
    } else {
        columnType.dataType = "VARCHAR";
    }
}

/**
 * Checks to validate an identified dataType for a column against a value for it.
 * If any of the values for the column do not follow its expected dataType 
 * structure, reassigns the datatype in order to avoid DATA_TRUNCATED errors.
 * @param {Object} columnType An object which stores for a column which 
 * category of dataType its values fall under, and the maxLen of all values
 * found for that column in the source data
 * @param {string} colVal A value for a column from a database seeding
 * source file
 */
function validateColumnDataType(columnType, colVal) {
    // Strings don't have a specified structure and don't need to 
    // go through the next type check
    if (columnType.dataType === "VARCHAR") {
        return;
    }

    if (columnType.dataType === "FLOAT" && !FLOAT_RE.test(colVal) ||
        columnType.dataType === "INT" && !INT_RE.test(colVal) ||
        columnType.dataType === "DATE" && !DATE_RE.test(colVal)) {
        // If any values of the column after the initial identification
        // do not follow the format of its specified type, it is 
        // generally safe to set the data type as a VARCHAR
        // since any value can be processed as a string
        columnType.dataType = "VARCHAR";
    }
}

/**
 * Based on the columnType, returns which MySQL data type to use for a column
 * @param {Object} columnType An object which stores for a column which 
 * category of dataType its values fall under, and the maxLen of all values
 * found for that column in the source data
 * @returns {string} The data type to use based on the columnType
 */
function getColumnDataType(columnType) {
    // INT data type thresholds
    const SMALLINT_THRESHOLD = 2;
    const INT_THRESHOLD = 3;
    const BIGINT_THRESHOLD = 11;
    // INT checks are structured as a treshold and their corresponding 
    // data type, used for checking which INT data type to use
    const INT_CHECKS = [
        [BIGINT_THRESHOLD, "BIGINT"],
        [INT_THRESHOLD, "INT"],
        [SMALLINT_THRESHOLD, "SMALLINT"]
    ];

    // VARCHAR data type maximums
    const TINY_VARCHAR_MAX = 10;
    const SMALL_VARCHAR_MAX = 50;
    const MEDIUM_VARCHAR_MAX = 255;
    const BIG_VARCHAR_MAX = 500;
    // Used to check how big of a VARCHAR to use for a column
    const VARCHAR_MAXES = [
        TINY_VARCHAR_MAX, 
        SMALL_VARCHAR_MAX, 
        MEDIUM_VARCHAR_MAX, 
        BIG_VARCHAR_MAX
    ];

    switch (columnType.dataType) {
        // If the dataType was null, no values for that column were found,
        // so the returned data type is TEXT since any value of any length
        // can be processed as a TEXT value
        case null:
            return "TEXT";
        case "INT":
            for (const [threshold, intType] of INT_CHECKS) {
                if (columnType.maxLen >= threshold) {
                    return intType;
                }
            }
            return "TINYINT";
        case "VARCHAR":
            for (const varcharMax of VARCHAR_MAXES) {
                if (columnType.maxLen <= varcharMax) {
                    return `VARCHAR(${varcharMax})`;
                }
            }
            return "TEXT";
        default: // For the case of floats and dates
            return columnType.dataType;
    }
}

// --------MAIN DRIVER--------
(() => {
    const files = fs.readdirSync(SOURCE_DATA_DIR);
    const n = files.length;
    files.forEach((file, index) => processFile(file, index, n));
})();