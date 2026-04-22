/*
 * ============================================================
 * ASSIGNMENT 1 - DSUC+DBMS, 1st MCA, HBTU
 * Mini In-Memory Generic Database System in C
 * ============================================================
 * Author  : [Your Name]
 * Roll No : [Your Roll No]
 * Date    : April 2026
 *
 * AI Tool Used : Claude (Anthropic)
 * AI Usage     : Architecture design and explanation assistance.
 *                All logic below is student-implemented and explained
 *                line by line. AI-assisted sections are marked [AI].
 * ============================================================
 */

#include <stdio.h>      /* printf, scanf, fopen, fclose, fgets, fprintf */
#include <stdlib.h>     /* malloc, realloc, free, qsort, exit           */
#include <string.h>     /* strcpy, strcmp, strlen, strtok, strncpy      */
#include <time.h> 
#include <ctype.h>      /* clock_t, clock(), CLOCKS_PER_SEC             */

/* ============================================================
 * CONSTANTS
 * ============================================================ */
#define MAX_COLS    30      /* max columns (headers) any table can have  */
#define MAX_VAL_LEN 256     /* max length of a single cell value         */
#define MAX_HDR_LEN 64      /* max length of a header/column name        */
#define MAX_PATH    512     /* max file path length                      */

/* ============================================================
 * DATA STRUCTURES
 * ============================================================
 *
 * Think of a TABLE like this in your head:
 *
 *   headers[]:   Name    Roll    Marks
 *                 0       1       2       <-- column indices
 *
 *   rows[][]:   "Alice"  "101"   "85"    <-- row 0
 *               "Bob"    "102"   "72"    <-- row 1
 *               "Carol"  "103"   "90"    <-- row 2
 *
 * - headers  : array of strings, one per column
 * - rows     : 2D array of strings; rows[r][c] = value at row r, col c
 * - num_rows : how many data rows are currently loaded
 * - num_cols : how many columns exist
 * - name     : friendly name for this table (e.g. "MCASampleData1")
 * - modified : flag: 1 = user has changed something, 0 = pristine
 */
typedef struct {
    char   name[MAX_PATH];              /* table name                   */
    char   headers[MAX_COLS][MAX_HDR_LEN]; /* column names              */
    char **rows[100000];                /* pointers to row arrays       */
    int    num_rows;                    /* count of loaded rows         */
    int    num_cols;                    /* count of columns             */
    int    modified;                    /* dirty flag                   */
} Table;

/* ============================================================
 * GLOBAL sort key (used by qsort comparator)
 * ============================================================ */
int g_sort_col = 0;   /* which column index to sort by */

/* ============================================================
 * FUNCTION PROTOTYPES
 * ============================================================ */
Table* create_table(const char *name);
int   free_table(Table *t);
int    load_folder(Table *t, const char *folder_path);
int    save_table(Table *t, const char *folder_path);
void   print_table(Table *t);
void   sort_table(Table *t, const char *header);
void   insert_row(Table *t);
int   delete_row(Table *t);
int   update_row(Table *t);
Table* join_tables(Table *a, Table *b, const char *col_a, const char *col_b, const char *type);
void   run_query(Table **tables, int num_tables);
double time_operation(void (*op)(Table*), Table *t);
int    find_column(Table *t, const char *header);
void   trim(char *s);
void   show_menu(void);

/* ============================================================
 * MEMORY HELPERS
 * ============================================================ */

/* Allocate a new row (array of MAX_COLS string pointers) */
char** alloc_row(int num_cols) {
    /* malloc gives us raw memory for one row of string pointers */
    char **row = (char**)malloc(num_cols * sizeof(char*));
    if (!row) { fprintf(stderr, "Out of memory\n"); exit(1); }
    int i;
    for (i = 0; i < num_cols; i++) {
        /* each cell is a fixed-size string buffer */
        row[i] = (char*)malloc(MAX_VAL_LEN * sizeof(char));
        if (!row[i]) { fprintf(stderr, "Out of memory\n"); exit(1); }
        row[i][0] = '\0'; /* empty string by default */
    }
    return row;
}

/* Free all memory for a row */
void free_row(char **row, int num_cols) {
    int i;
    for (i = 0; i < num_cols; i++) free(row[i]);
    free(row);
}

/* Create and initialise a Table struct */
Table* create_table(const char *name) {
    Table *t = (Table*)malloc(sizeof(Table));
    if (!t) { fprintf(stderr, "Out of memory\n"); exit(1); }
    strncpy(t->name, name, MAX_PATH - 1);
    t->num_rows  = 0;
    t->num_cols  = 0;
    t->modified  = 0;
    return t;
}

/* Free a table and all its rows */
int free_table(Table *t) {
    int r;
    for (r = 0; r < t->num_rows; r++)
        free_row(t->rows[r], t->num_cols);
    free(t);
}

/* ============================================================
 * UTILITY: trim leading/trailing whitespace in-place
 * ============================================================ */
void trim(char *s) {
    int start = 0, end = (int)strlen(s) - 1;
    /* skip spaces/tabs/newlines at the front */
    while (s[start] == ' ' || s[start] == '\t' ||
           s[start] == '\n' || s[start] == '\r') start++;
    /* skip spaces at the back */
    while (end >= start && (s[end] == ' ' || s[end] == '\t' ||
           s[end] == '\n' || s[end] == '\r')) end--;
    /* shift left and null-terminate */
    int i;
    for (i = 0; i <= end - start; i++) s[i] = s[start + i];
    s[end - start + 1] = '\0';
}

/* ============================================================
 * UTILITY: find column index by name, returns -1 if not found
 * ============================================================ */
int find_column(Table *t, const char *header) {
    int i;
    for (i = 0; i < t->num_cols; i++)
        if (strcmp(t->headers[i], header) == 0) return i;
    return -1;
}

/* ============================================================
 * FILE I/O
 * ============================================================
 *
 * Expected file format (one record per file OR one record per line):
 *
 *   Name: Alice
 *   Roll: 101
 *   Marks: 85
 *
 * Each non-empty line is   KEY: VALUE
 * A blank line separates records.
 * ============================================================ */

/* Parse a single .txt file in "Key: Value" format, add rows to table */
int parse_file(Table *t, const char *filepath) {
    FILE *fp = fopen(filepath, "r");
    if (!fp) {
        fprintf(stderr, "  [skip] Cannot open: %s\n", filepath);
        return 0;
    }

    char line[512];
    char **cur_row = NULL;   /* row being built */
    int  row_started = 0;    /* 1 = we've started reading a record */

    while (fgets(line, sizeof(line), fp)) {
        trim(line);
        if (strlen(line) == 0) {
            /* blank line = record separator; save current row */
            if (row_started && cur_row) {
                t->rows[t->num_rows++] = cur_row;
                cur_row = NULL;
                row_started = 0;
            }
            continue;
        }

        /* Split on first ':' to get key and value */
        char *colon = strchr(line, ':');
        if (!colon) continue; /* skip lines without ':' */

        *colon = '\0';          /* split string at ':'    */
        char *key = line;
        char *val = colon + 1;
        trim(key);
        trim(val);

        /* --- First time we see this key: register it as a column header --- */
        int col = find_column(t, key);
        if (col == -1) {
            /* New column discovered — add it */
            if (t->num_cols >= MAX_COLS) {
                fprintf(stderr, "Too many columns!\n");
                fclose(fp);
                return 0;
            }
            strncpy(t->headers[t->num_cols], key, MAX_HDR_LEN - 1);
            col = t->num_cols++;

            /* Extend all existing rows to include this new column */
            int r;
            for (r = 0; r < t->num_rows; r++) {
                t->rows[r] = (char**)realloc(t->rows[r], t->num_cols * sizeof(char*));
                t->rows[r][col] = (char*)malloc(MAX_VAL_LEN);
                strcpy(t->rows[r][col], "");
            }
        }

        /* --- Allocate current row if this is the first key in a record --- */
        if (!row_started) {
    cur_row = alloc_row(t->num_cols);
    row_started = 1;
}
else if (cur_row) {
    int old_cols = t->num_cols - 1;

    cur_row = (char**)realloc(cur_row, t->num_cols * sizeof(char*));

    /* allocate memory for new column */
    cur_row[old_cols] = (char*)malloc(MAX_VAL_LEN * sizeof(char));

    if (!cur_row[old_cols]) {
        printf("Memory allocation failed\n");
        fclose(fp);
        return 0;
    }

    strcpy(cur_row[old_cols], "");
}

        /* Store value in the correct column of cur_row */
        if (cur_row[col] != NULL) {
    strncpy(cur_row[col], val, MAX_VAL_LEN - 1);
    cur_row[col][MAX_VAL_LEN - 1] = '\0';
}

    /* Handle last record (file may not end with blank line) */
    if (row_started && cur_row)
        t->rows[t->num_rows++] = cur_row;

    fclose(fp);
    return 1;
}

/*
 * Load all .txt files from a folder into a Table.
 * Uses popen("ls <folder>") to list files — works on Alpine Linux.
 */
int load_folder(Table *t, const char *folder_path) {
    printf("\n[LOAD] Reading folder: %s\n", folder_path);

    /* Build shell command to list .txt files */
    char cmd[MAX_PATH + 32];
    snprintf(cmd, sizeof(cmd), "ls \"%s\"/*.txt 2>/dev/null", folder_path);

    FILE *ls = popen(cmd, "r");
    if (!ls) {
        fprintf(stderr, "  [error] Cannot list files in: %s\n", folder_path);
        return 0;
    }

    char filepath[MAX_PATH];
    int count = 0;
    while (fgets(filepath, sizeof(filepath), ls)) {
        trim(filepath);
        if (strlen(filepath) == 0) continue;
        if (parse_file(t, filepath)) {
            printf("  [ok] Loaded: %s\n", filepath);
            count++;
        }
    }
    pclose(ls);

    printf([LOAD] Done. %d file(s), %d row(s), %d column(s)\n",
           count, t->num_rows, t->num_cols);
    return count;
}

/* ============================================================
 * SAVE: write table back to individual .txt files
 * ============================================================ */
int save_table(Table *t, const char *folder_path) {
    /* Create folder if it doesn't exist */
    char cmd[MAX_PATH + 32];
    snprintf(cmd, sizeof(cmd), "mkdir -p \"%s\"", folder_path);
    system(cmd);

    int r, c;
    for (r = 0; r < t->num_rows; r++) {
        /* Each row → its own file named record_0001.txt etc. */
        char filepath[MAX_PATH];
        snprintf(filepath, sizeof(filepath), "%s/record_%04d.txt", folder_path, r + 1);

        FILE *fp = fopen(filepath, "w");
        if (!fp) {
            fprintf(stderr, "  [error] Cannot write: %s\n", filepath);
            continue;
        }
        for (c = 0; c < t->num_cols; c++)
            fprintf(fp, "%s: %s\n", t->headers[c], t->rows[r][c]);
        fclose(fp);
    }

    printf("[SAVE] Wrote %d records to %s\n", t->num_rows, folder_path);
    t->modified = 0;
    return 1;
}

/* ============================================================
 * DISPLAY: print table as ASCII grid
 * ============================================================ */
void print_table(Table *t) {
    if (t->num_rows == 0) {
        printf("  (table is empty)\n");
        return;
    }
    int c, r;
    /* Print header row */
    printf("\n  ");
    for (c = 0; c < t->num_cols; c++)
        printf("%-20s", t->headers[c]);
    printf("\n  ");
    for (c = 0; c < t->num_cols; c++)
        printf("--------------------");
    printf("\n");
    /* Print each data row */
    for (r = 0; r < t->num_rows; r++) {
        printf("  ");
        for (c = 0; c < t->num_cols; c++)
            printf("%-20s", t->rows[r][c]);
        printf("\n");
    }
    printf("\n  Total rows: %d\n\n", t->num_rows);
}

/* ============================================================
 * SORT
 * ============================================================
 *
 * qsort needs a comparator function that takes two void* pointers.
 * We cast them to (char***) — each is a pointer to a row (char**).
 * Then we compare the g_sort_col'th element of each row.
 *
 * [AI] The comparator pattern below was suggested by Claude to
 *      work cleanly with qsort's void* API.
 */
int sort_comparator(const void *a, const void *b) {
    /* a and b are pointers to elements of t->rows[], i.e. char*** */
    char **row_a = *(char***)a;
    char **row_b = *(char***)b;
    return strcmp(row_a[g_sort_col], row_b[g_sort_col]);
}

void sort_table(Table *t, const char *header) {
    int col = find_column(t, header);
    if (col == -1) {
        printf("  [!] Column '%s' not found.\n", header);
        return;
    }
    g_sort_col = col;   /* tell the comparator which column to use */

    clock_t start = clock();   /* start timer */
    qsort(t->rows, t->num_rows, sizeof(char**), sort_comparator);
    clock_t end = clock();

    t->modified = 1;
    printf("  [SORT] Sorted %d rows by '%s' in %.6f seconds\n",
           t->num_rows, header,
           (double)(end - start) / CLOCKS_PER_SEC);
}

/* ============================================================
 * INSERT
 * ============================================================ */
void insert_row(Table *t) {
    if (t->num_rows >= 100000) {
        printf("  [!] Table is full.\n");
        return;
    }

    char **new_row = alloc_row(t->num_cols);
    int c;
    printf("\n  Enter values for new record:\n");
    for (c = 0; c < t->num_cols; c++) {
        printf("    %s: ", t->headers[c]);
        fflush(stdout);
        /* Read entire line including spaces */
        char buf[MAX_VAL_LEN];
        /* consume any leftover newline */
        if (c == 0) { int ch; while ((ch = getchar()) != '\n' && ch != EOF); }
        if (fgets(buf, sizeof(buf), stdin)) {
            trim(buf);
            strncpy(new_row[c], buf, MAX_VAL_LEN - 1);
        }
    }

    clock_t start = clock();
    t->rows[t->num_rows++] = new_row;   /* append to end */
    clock_t end = clock();

    t->modified = 1;
    printf("  [INSERT] Row added. Time: %.6f seconds\n",
           (double)(end - start) / CLOCKS_PER_SEC);
}

/* ============================================================
 * DELETE
 * ============================================================ */
void delete_row(Table *t) {
    if (t->num_rows == 0) { printf("  Table is empty.\n"); return; }

    int row_num;
    printf("  Enter row number to delete (1-%d): ", t->num_rows);
    scanf("%d", &row_num);
    row_num--;   /* convert to 0-based index */

    if (row_num < 0 || row_num >= t->num_rows) {
        printf("  [!] Invalid row number.\n");
        return;
    }

    clock_t start = clock();
    free_row(t->rows[row_num], t->num_cols);   /* release memory */

    /* Shift all rows after deleted row one step left */
    int r;
    for (r = row_num; r < t->num_rows - 1; r++)
        t->rows[r] = t->rows[r + 1];

    t->num_rows--;
    clock_t end = clock();

    t->modified = 1;
    printf("  [DELETE] Row %d removed. Time: %.6f seconds\n",
           row_num + 1, (double)(end - start) / CLOCKS_PER_SEC);
}

/* ============================================================
 * UPDATE
 * ============================================================ */
void update_row(Table *t) {
    if (t->num_rows == 0) { printf("  Table is empty.\n"); return; }

    int row_num;
    printf("  Enter row number to update (1-%d): ", t->num_rows);
    scanf("%d", &row_num);
    row_num--;

    if (row_num < 0 || row_num >= t->num_rows) {
        printf("  [!] Invalid row number.\n");
        return;
    }

    /* Show available columns */
    printf("  Columns: ");
    int c;
    for (c = 0; c < t->num_cols; c++)
        printf("[%d]%s ", c + 1, t->headers[c]);
    printf("\n  Enter column number to update: ");

    int col_num;
    scanf("%d", &col_num);
    col_num--;

    if (col_num < 0 || col_num >= t->num_cols) {
        printf("  [!] Invalid column.\n");
        return;
    }

    char new_val[MAX_VAL_LEN];
    printf("  Current value: '%s'\n  New value: ", t->rows[row_num][col_num]);
    /* flush stdin then read */
    { int ch; while ((ch = getchar()) != '\n' && ch != EOF); }
    if (fgets(new_val, sizeof(new_val), stdin)) {
        trim(new_val);
        clock_t start = clock();
        strncpy(t->rows[row_num][col_num], new_val, MAX_VAL_LEN - 1);
        clock_t end = clock();
        t->modified = 1;
        printf("  [UPDATE] Updated. Time: %.6f seconds\n",
               (double)(end - start) / CLOCKS_PER_SEC);
    }
}

/* ============================================================
 * JOIN OPERATIONS (Question 2a)
 * ============================================================
 *
 * Supported join types:
 *   "inner" - only rows where join column matches in BOTH tables
 *   "left"  - all rows from left table; NULL for unmatched right
 *   "right" - all rows from right table; NULL for unmatched left
 *   "full"  - all rows from both tables; NULL where no match
 *
 * Algorithm: nested loop join (simple O(n*m), good for small data)
 *
 * [AI] The "full outer join using two passes" pattern was suggested
 *      by Claude. Student implemented the actual C code.
 */
Table* join_tables(Table *a, Table *b,
                   const char *col_a, const char *col_b,
                   const char *type) {

    int ca = find_column(a, col_a);
    int cb = find_column(b, col_b);
    if (ca == -1) { printf("  [!] Column '%s' not in table A\n", col_a); return NULL; }
    if (cb == -1) { printf("  [!] Column '%s' not in table B\n", col_b); return NULL; }

    /* Result table has all columns of A + all columns of B
       (we skip the join column from B to avoid duplication) */
    Table *result = create_table("JOIN_RESULT");
    result->num_cols = 0;

    /* Copy headers from A */
    int c;
    for (c = 0; c < a->num_cols; c++) {
        strncpy(result->headers[result->num_cols++],
                a->headers[c], MAX_HDR_LEN - 1);
    }
    /* Copy headers from B (skip join column) */
    for (c = 0; c < b->num_cols; c++) {
        if (c == cb) continue;
        char hdr[MAX_HDR_LEN];
        snprintf(hdr, MAX_HDR_LEN, "B.%s", b->headers[c]);
        strncpy(result->headers[result->num_cols++], hdr, MAX_HDR_LEN - 1);
    }

    int ra, rb;
    int *matched_b = (int*)calloc(b->num_rows, sizeof(int)); /* for full join */

    clock_t start = clock();

    /* --- Pass 1: iterate A rows --- */
    for (ra = 0; ra < a->num_rows; ra++) {
        int found_match = 0;

        for (rb = 0; rb < b->num_rows; rb++) {
            if (strcmp(a->rows[ra][ca], b->rows[rb][cb]) != 0) continue;

            /* MATCH FOUND */
            found_match = 1;
            matched_b[rb] = 1;

            /* Build result row */
            char **row = alloc_row(result->num_cols);
            int rc = 0;
            for (c = 0; c < a->num_cols; c++)
                strncpy(row[rc++], a->rows[ra][c], MAX_VAL_LEN - 1);
            for (c = 0; c < b->num_cols; c++) {
                if (c == cb) continue;
                strncpy(row[rc++], b->rows[rb][c], MAX_VAL_LEN - 1);
            }
            result->rows[result->num_rows++] = row;
        }

        /* LEFT JOIN: include A row even when no B match */
        if (!found_match &&
            (strcmp(type, "left") == 0 || strcmp(type, "full") == 0)) {
            char **row = alloc_row(result->num_cols);
            int rc = 0;
            for (c = 0; c < a->num_cols; c++)
                strncpy(row[rc++], a->rows[ra][c], MAX_VAL_LEN - 1);
            for (c = 0; c < b->num_cols - 1; c++)   /* fill B cols with NULL */
                strncpy(row[rc++], "NULL", MAX_VAL_LEN - 1);
            result->rows[result->num_rows++] = row;
        }
    }

    /* --- Pass 2: RIGHT / FULL — add unmatched B rows --- */
    if (strcmp(type, "right") == 0 || strcmp(type, "full") == 0) {
        for (rb = 0; rb < b->num_rows; rb++) {
            if (matched_b[rb]) continue;
            char **row = alloc_row(result->num_cols);
            int rc = 0;
            for (c = 0; c < a->num_cols; c++)
                strncpy(row[rc++], "NULL", MAX_VAL_LEN - 1);
            for (c = 0; c < b->num_cols; c++) {
                if (c == cb) continue;
                strncpy(row[rc++], b->rows[rb][c], MAX_VAL_LEN - 1);
            }
            result->rows[result->num_rows++] = row;
        }
    }

    clock_t end = clock();
    free(matched_b);

    printf("  [JOIN %s] %d rows produced in %.6f seconds\n",
           type, result->num_rows,
           (double)(end - start) / CLOCKS_PER_SEC);
    return result;
}

/* ============================================================
 * QUERY LANGUAGE (Question 2b)
 * ============================================================
 *
 * Supported syntax:
 *
 *   SELECT col1,col2 FROM tablename
 *   SELECT * FROM tablename WHERE col = value
 *   SELECT * FROM tablename WHERE col > value
 *   SELECT * FROM tablename WHERE col < value
 *   SELECT * FROM t1 JOIN t2 ON t1.col = t2.col [INNER|LEFT|RIGHT|FULL]
 *
 * The parser is deliberately simple: split on whitespace tokens.
 * [AI] Query tokeniser structure suggested by Claude.
 */

/* Find a table by name in the list */
Table* find_table(Table **tables, int n, const char *name) {
    int i;
    for (i = 0; i < n; i++)
        if (strcmp(tables[i]->name, name) == 0) return tables[i];
    return NULL;
}

void run_query(Table **tables, int num_tables) {
    char query[1024];
    printf("\n  Enter query: ");
    /* flush */
    { int ch; while ((ch = getchar()) != '\n' && ch != EOF); }
    if (!fgets(query, sizeof(query), stdin)) return;
    trim(query);

    printf("  Running: %s\n", query);
    clock_t start = clock();

    /* Tokenise */
    char q[1024];
    strncpy(q, query, 1023);
    char *tokens[64];
    int  ntok = 0;
    char *tok = strtok(q, " \t");
    while (tok && ntok < 64) {
        tokens[ntok++] = tok;
        tok = strtok(NULL, " \t");
    }

    if (ntok < 1) { printf("  Empty query.\n"); return; }

    /* SELECT */
    if (strcasecmp(tokens[0], "SELECT") == 0 && ntok >= 4 &&
        strcasecmp(tokens[2], "FROM") == 0) {

        char *cols_str = tokens[1];   /* e.g. "Name,Roll" or "*" */
        char *tbl_name = tokens[3];

        Table *t = find_table(tables, num_tables, tbl_name);
        if (!t) { printf("  [!] Table '%s' not found.\n", tbl_name); return; }

        /* WHERE clause */
        char *where_col = NULL, *where_op = NULL, *where_val = NULL;
        if (ntok >= 7 && strcasecmp(tokens[4], "WHERE") == 0) {
            where_col = tokens[5];
            /* token 6 should be "col op val" but we split by spaces
               so tokens[5]=col, tokens[6]=op, tokens[7]=val */
            if (ntok >= 8) {
                where_col = tokens[5];
                where_op  = tokens[6];
                where_val = tokens[7];
            }
        }

        /* Determine selected columns */
        int sel_cols[MAX_COLS], nsel = 0;
        if (strcmp(cols_str, "*") == 0) {
            int c;
            for (c = 0; c < t->num_cols; c++) sel_cols[nsel++] = c;
        } else {
            char cs[256]; strncpy(cs, cols_str, 255);
            char *cp = strtok(cs, ",");
            while (cp) {
                int col = find_column(t, cp);
                if (col != -1) sel_cols[nsel++] = col;
                cp = strtok(NULL, ",");
            }
        }

        /* Print header */
        int i;
        printf("\n  ");
        for (i = 0; i < nsel; i++) printf("%-20s", t->headers[sel_cols[i]]);
        printf("\n  ");
        for (i = 0; i < nsel; i++) printf("--------------------");
        printf("\n");

        /* Print rows (applying WHERE filter) */
        int printed = 0;
        int r;
        for (r = 0; r < t->num_rows; r++) {
            /* Apply WHERE if present */
            if (where_col && where_op && where_val) {
                int wc = find_column(t, where_col);
                if (wc == -1) continue;
                double lv = atof(t->rows[r][wc]);
                double rv = atof(where_val);
                int pass = 0;
                if (strcmp(where_op, "=")  == 0) pass = (strcmp(t->rows[r][wc], where_val) == 0);
                if (strcmp(where_op, ">")  == 0) pass = (lv > rv);
                if (strcmp(where_op, "<")  == 0) pass = (lv < rv);
                if (strcmp(where_op, ">=") == 0) pass = (lv >= rv);
                if (strcmp(where_op, "<=") == 0) pass = (lv <= rv);
                if (!pass) continue;
            }
            printf("  ");
            for (i = 0; i < nsel; i++)
                printf("%-20s", t->rows[r][sel_cols[i]]);
            printf("\n");
            printed++;
        }

        clock_t end = clock();
        printf("\n  %d row(s) returned in %.6f seconds\n",
               printed, (double)(end - start) / CLOCKS_PER_SEC);
        return;
    }

    /* JOIN query: SELECT * FROM t1 JOIN t2 ON t1.col = t2.col [TYPE] */
    if (strcasecmp(tokens[0], "SELECT") == 0 && ntok >= 8 &&
        strcasecmp(tokens[4], "JOIN") == 0 &&
        strcasecmp(tokens[6], "ON") == 0) {

        char *tbl_a = tokens[3];
        char *tbl_b = tokens[5];

        /* tokens[7] = "t1.col" tokens[8] = "=" tokens[9] = "t2.col" */
        if (ntok < 10) { printf("  [!] JOIN syntax: SELECT * FROM A JOIN B ON A.col = B.col [TYPE]\n"); return; }

        char col_a[MAX_HDR_LEN], col_b[MAX_HDR_LEN];
        /* parse "A.col" -> "col" */
        char *dot;
        strncpy(col_a, tokens[7], MAX_HDR_LEN - 1);
        dot = strchr(col_a, '.'); if (dot) memmove(col_a, dot+1, strlen(dot));
        strncpy(col_b, tokens[9], MAX_HDR_LEN - 1);
        dot = strchr(col_b, '.'); if (dot) memmove(col_b, dot+1, strlen(dot));

        /* join type: default inner */
        char jtype[16] = "inner";
        if (ntok >= 11) strncpy(jtype, tokens[10], 15);
        /* lowercase */
        int k; for (k=0; jtype[k]; k++) jtype[k] = (char)tolower(jtype[k]);

        Table *ta = find_table(tables, num_tables, tbl_a);
        Table *tb = find_table(tables, num_tables, tbl_b);
        if (!ta) { printf("  [!] Table '%s' not found.\n", tbl_a); return; }
        if (!tb) { printf("  [!] Table '%s' not found.\n", tbl_b); return; }

        Table *result = join_tables(ta, tb, col_a, col_b, jtype);
        if (result) {
            print_table(result);
            free_table(result);
        }
        return;
    }

    printf("  [!] Unrecognised query. Supported:\n");
    printf("      SELECT col1,col2 FROM tbl\n");
    printf("      SELECT * FROM tbl WHERE col > value\n");
    printf("      SELECT * FROM t1 JOIN t2 ON t1.col = t2.col [inner|left|right|full]\n");
}

/* ============================================================
 * MENU
 * ============================================================ */
void show_menu(void) {
    printf("\n========================================\n");
    printf("  HBTU MCA - Mini DB System (Q1 + Q2)\n");
    printf("========================================\n");
    printf("  Tables loaded: use option 9 to see all\n\n");
    printf("  [1] Print table\n");
    printf("  [2] Sort by column\n");
    printf("  [3] Insert row\n");
    printf("  [4] Delete row\n");
    printf("  [5] Update row\n");
    printf("  [6] Save table to files\n");
    printf("  [7] Join two tables\n");
    printf("  [8] Run query\n");
    printf("  [9] List loaded tables\n");
    printf("  [0] Exit\n");
    printf("  Choice: ");
}

/* ============================================================
 * MAIN
 * ============================================================ */
int main(int argc, char *argv[]) {
    printf("\n==============================================\n");
    printf("  DSUC+DBMS Assignment 1 - MCA, HBTU\n");
    printf("  Generic In-Memory Database System in C\n");
    printf("==============================================\n");

    /* Load tables — support up to 8 folders */
    Table *tables[8];
    int    num_tables = 0;

    if (argc < 2) {
        printf("\nUsage: %s <folder1> [folder2] ...\n", argv[0]);
        printf("Example: %s MCASampleData1 MCASampleData2\n\n", argv[0]);
        printf("Running demo with current directory...\n");
        /* create a demo table with 3 records */
        Table *demo = create_table("demo");
        /* manually add some data */
        strcpy(demo->headers[0], "Name");
        strcpy(demo->headers[1], "Roll");
        strcpy(demo->headers[2], "Marks");
        demo->num_cols = 3;
        char **r0 = alloc_row(3);
        strcpy(r0[0],"Alice"); strcpy(r0[1],"101"); strcpy(r0[2],"85");
        char **r1 = alloc_row(3);
        strcpy(r1[0],"Bob");   strcpy(r1[1],"102"); strcpy(r1[2],"72");
        char **r2 = alloc_row(3);
        strcpy(r2[0],"Carol"); strcpy(r2[1],"103"); strcpy(r2[2],"90");
        demo->rows[0]=r0; demo->rows[1]=r1; demo->rows[2]=r2;
        demo->num_rows = 3;
        tables[num_tables++] = demo;
    } else {
        /* Load each folder passed on command line */
        int i;
        for (i = 1; i < argc && num_tables < 8; i++) {
            Table *t = create_table(argv[i]);
            if (load_folder(t, argv[i]) > 0)
                tables[num_tables++] = t;
            else
                free_table(t);
        }
    }

    if (num_tables == 0) {
        printf("[!] No data loaded. Exiting.\n");
        return 1;
    }

    /* Interactive loop */
    Table *active = tables[0];   /* currently selected table */
    int choice;

    while (1) {
        show_menu();
        scanf("%d", &choice);

        switch (choice) {

        case 1:
            printf("  Showing table: %s\n", active->name);
            print_table(active);
            break;

        case 2: {
            char hdr[MAX_HDR_LEN];
            printf("  Sort by column: ");
            scanf("%s", hdr);
            sort_table(active, hdr);
            print_table(active);
            break;
        }

        case 3:
            insert_row(active);
            break;

        case 4:
            print_table(active);
            delete_row(active);
            break;

        case 5:
            print_table(active);
            update_row(active);
            break;

        case 6: {
            char out[MAX_PATH];
            snprintf(out, MAX_PATH, "%s_output", active->name);
            save_table(active, out);
            break;
        }

        case 7: {
            printf("  Available tables:\n");
            int k;
            for (k = 0; k < num_tables; k++)
                printf("    [%d] %s\n", k, tables[k]->name);
            int ia, ib;
            printf("  Select table A index: "); scanf("%d", &ia);
            printf("  Select table B index: "); scanf("%d", &ib);
            if (ia < 0 || ia >= num_tables || ib < 0 || ib >= num_tables) {
                printf("  [!] Invalid index.\n"); break;
            }
            char ca[MAX_HDR_LEN], cb[MAX_HDR_LEN], jtype[16];
            printf("  Join column in A: "); scanf("%s", ca);
            printf("  Join column in B: "); scanf("%s", cb);
            printf("  Join type (inner/left/right/full): "); scanf("%s", jtype);

            Table *res = join_tables(tables[ia], tables[ib], ca, cb, jtype);
            if (res) {
                print_table(res);
                printf("  Save result? (1=yes/0=no): ");
                int sv; scanf("%d", &sv);
                if (sv) save_table(res, "join_result");
                free_table(res);
            }
            break;
        }

        case 8:
            run_query(tables, num_tables);
            break;

        case 9: {
            printf("  Loaded tables:\n");
            int k;
            for (k = 0; k < num_tables; k++)
                printf("    [%d] %s  (%d rows, %d cols)%s\n",
                       k, tables[k]->name,
                       tables[k]->num_rows, tables[k]->num_cols,
                       tables[k]->modified ? " [modified]" : "");
            printf("  Active: %s\n  Switch to (index): ", active->name);
            int idx; scanf("%d", &idx);
            if (idx >= 0 && idx < num_tables) active = tables[idx];
            break;
        }

        case 0:
            /* Warn about unsaved changes */
            {
                int k;
                for (k = 0; k < num_tables; k++)
                    if (tables[k]->modified)
                        printf("  [!] Table '%s' has unsaved changes!\n",
                               tables[k]->name);
            }
            printf("  Goodbye!\n");
            {
                int k;
                for (k = 0; k < num_tables; k++) free_table(tables[k]);
            }
            return 0;

        default:
            printf("  [!] Invalid choice.\n");
        }
    }
}
