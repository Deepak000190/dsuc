/*
 * ============================================================
 * ASSIGNMENT 1 - DSUC+DBMS, 1st MCA, HBTU
 * Mini In-Memory Generic Database System in C
 * ============================================================
 * Author  : [Your Name]
 * Roll No : [Your Roll No]
 * Date    : April 2026
 *
 * SEGFAULT FIX: In parse_file(), after realloc(cur_row, ...),
 * the new cell pointer was never malloc'd  it held garbage,
 * causing a crash when strncpy wrote to it.
 * Fixed by malloc'ing + zeroing every newly added cell.
 * ============================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <ctype.h>

/*  CONSTANTS  */
#define MAX_COLS    30
#define MAX_ROWS    50000
#define MAX_VAL_LEN 256
#define MAX_HDR_LEN 64
#define MAX_PATH    512

/*  DATA STRUCTURE  */
typedef struct {
    char   name[MAX_PATH];
    char   headers[MAX_COLS][MAX_HDR_LEN];
    char **rows[MAX_ROWS];
    int    num_rows;
    int    num_cols;
    int    modified;
} Table;

/*  GLOBAL: sort column used by qsort comparator  */
int g_sort_col = 0;

/*  PROTOTYPES  */
Table* create_table(const char *name);
void   free_table(Table *t);
int    load_folder(Table *t, const char *folder_path);
int    save_table(Table *t, const char *folder_path);
void   print_table(Table *t);
void   sort_table(Table *t, const char *header);
void   insert_row(Table *t);
void   delete_row(Table *t);
void   update_row(Table *t);
Table* join_tables(Table *a, Table *b,
                   const char *col_a, const char *col_b,
                   const char *type);
void   run_query(Table **tables, int num_tables);
int    find_column(Table *t, const char *header);
Table* find_table(Table **tables, int n, const char *name);
void   trim(char *s);
void   show_menu(void);

/* ============================================================
 * MEMORY HELPERS
 * ============================================================ */

/* Allocate one row: an array of num_cols heap strings */
char** alloc_row(int num_cols) {
    int i;
    char **row = (char **)malloc(num_cols * sizeof(char *));
    if (!row) { fprintf(stderr, "malloc failed\n"); exit(1); }
    for (i = 0; i < num_cols; i++) {
        row[i] = (char *)malloc(MAX_VAL_LEN);
        if (!row[i]) { fprintf(stderr, "malloc failed\n"); exit(1); }
        row[i][0] = '\0';
    }
    return row;
}

/* Free every string in a row, then the row pointer itself */
void free_row(char **row, int num_cols) {
    int i;
    for (i = 0; i < num_cols; i++) free(row[i]);
    free(row);
}

/* Allocate and zero-initialise a Table */
Table* create_table(const char *name) {
    Table *t = (Table *)malloc(sizeof(Table));
    if (!t) { fprintf(stderr, "malloc failed\n"); exit(1); }
    strncpy(t->name, name, MAX_PATH - 1);
    t->name[MAX_PATH - 1] = '\0';
    t->num_rows = 0;
    t->num_cols = 0;
    t->modified = 0;
    return t;
}

/* Free every row in a table, then the table itself */
void free_table(Table *t) {
    int r;
    if (!t) return;
    for (r = 0; r < t->num_rows; r++)
        free_row(t->rows[r], t->num_cols);
    free(t);
}

/* ============================================================
 * UTILITIES
 * ============================================================ */

/* Trim leading/trailing whitespace in-place */
void trim(char *s) {
    if (!s) return;
    int start = 0;
    int end   = (int)strlen(s) - 1;
    while (s[start] && (s[start]==' '||s[start]=='\t'||
                        s[start]=='\n'||s[start]=='\r'))
        start++;
    while (end >= start && (s[end]==' '||s[end]=='\t'||
                            s[end]=='\n'||s[end]=='\r'))
        end--;
    int i;
    for (i = 0; i <= end - start; i++) s[i] = s[start + i];
    s[end - start + 1] = '\0';
}

/* Return column index by name, -1 if not found */
int find_column(Table *t, const char *header) {
    int i;
    for (i = 0; i < t->num_cols; i++)
        if (strcmp(t->headers[i], header) == 0) return i;
    return -1;
}

/* Return table pointer by name, NULL if not found */
Table* find_table(Table **tables, int n, const char *name) {
    int i;
    for (i = 0; i < n; i++)
        if (strcmp(tables[i]->name, name) == 0) return tables[i];
    return NULL;
}

/* ============================================================
 * FILE PARSER
 * ============================================================
 * Format expected inside each .txt file:
 *
 *   Name: Alice
 *   Roll: 101
 *   Marks: 85
 *
 *   Name: Bob
 *   Roll: 102
 *   Marks: 72
 *
 * A blank line ends one record and starts the next.
 * Headers are discovered dynamically  no hardcoding needed.
 *
 *  BUG FIX (segfault root cause) 
 * When a NEW column is discovered while cur_row is already
 * being built (row_started == 1), the old code did:
 *
 *   cur_row = realloc(cur_row, num_cols * sizeof(char*));
 *   // BUG: cur_row[new_col] is now a GARBAGE pointer!
 *   if (cur_row[col]) strncpy(...);  // writes to garbage  CRASH
 *
 * The fix adds the two lines that were missing:
 *   cur_row[col] = malloc(MAX_VAL_LEN);
 *   cur_row[col][0] = '\0';
 * ============================================================ */
int parse_file(Table *t, const char *filepath) {
    FILE *fp = fopen(filepath, "r");
    if (!fp) {
        fprintf(stderr, "  [skip] cannot open: %s\n", filepath);
        return 0;
    }

    char  line[512];
    char **cur_row    = NULL;
    int   row_started = 0;

    while (fgets(line, sizeof(line), fp)) {
        trim(line);

        /*  Blank line = record boundary  */
        if (strlen(line) == 0) {
            if (row_started && cur_row) {
                if (t->num_rows < MAX_ROWS)
                    t->rows[t->num_rows++] = cur_row;
                else {
                    fprintf(stderr, "  [warn] MAX_ROWS reached, row dropped\n");
                    free_row(cur_row, t->num_cols);
                }
                cur_row    = NULL;
                row_started = 0;
            }
            continue;
        }

        /*  Split on first ':'  */
        char *colon = strchr(line, ':');
        if (!colon) continue;

        *colon = '\0';
        char *key = line;
        char *val = colon + 1;
        trim(key);
        trim(val);

        if (strlen(key) == 0) continue; /* skip malformed lines */

        /*  Discover new column header  */
        int col = find_column(t, key);
        if (col == -1) {
            if (t->num_cols >= MAX_COLS) {
                fprintf(stderr, "  [warn] MAX_COLS reached, column '%s' ignored\n", key);
                continue;
            }
            strncpy(t->headers[t->num_cols], key, MAX_HDR_LEN - 1);
            t->headers[t->num_cols][MAX_HDR_LEN - 1] = '\0';
            col = t->num_cols++;

            /*  Extend every already-saved row with an empty cell  */
            int r;
            for (r = 0; r < t->num_rows; r++) {
                char **tmp = (char **)realloc(t->rows[r],
                                              t->num_cols * sizeof(char *));
                if (!tmp) { fprintf(stderr, "realloc failed\n"); exit(1); }
                t->rows[r]      = tmp;
                t->rows[r][col] = (char *)malloc(MAX_VAL_LEN);
                if (!t->rows[r][col]) { fprintf(stderr, "malloc failed\n"); exit(1); }
                t->rows[r][col][0] = '\0';
            }

            /*  Extend cur_row if we are mid-record 
             * THIS WAS THE SEGFAULT:
             *   old code did realloc but never malloc'd the new cell,
             *   so cur_row[col] was a garbage pointer. Writing to it
             *   caused the crash. Now we properly malloc + zero it.
             *  */
            if (row_started && cur_row) {
                char **tmp = (char **)realloc(cur_row,
                                              t->num_cols * sizeof(char *));
                if (!tmp) { fprintf(stderr, "realloc failed\n"); exit(1); }
                cur_row      = tmp;
                cur_row[col] = (char *)malloc(MAX_VAL_LEN); /*  THE FIX */
                if (!cur_row[col]) { fprintf(stderr, "malloc failed\n"); exit(1); }
                cur_row[col][0] = '\0';                     /*  THE FIX */
            }
        }

        /*  Start a new row on the first key of a record  */
        if (!row_started) {
            cur_row = alloc_row(t->num_cols); /* allocates ALL current cols */
            row_started = 1;
        }

        /*  Store value (cur_row[col] is always valid here)  */
        strncpy(cur_row[col], val, MAX_VAL_LEN - 1);
        cur_row[col][MAX_VAL_LEN - 1] = '\0';
    }

    /*  Save the last record if file doesn't end with blank line  */
    if (row_started && cur_row) {
        if (t->num_rows < MAX_ROWS)
            t->rows[t->num_rows++] = cur_row;
        else
            free_row(cur_row, t->num_cols);
    }

    fclose(fp);
    return 1;
}

/* ============================================================
 * LOAD FOLDER
 * Uses popen("ls") to enumerate .txt files in a directory.
 * Works on Alpine Linux and any POSIX shell.
 * ============================================================ */
int load_folder(Table *t, const char *folder_path) {
    printf("\n[LOAD] Reading folder: %s\n", folder_path);

    char cmd[MAX_PATH + 40];
    snprintf(cmd, sizeof(cmd), "ls \"%s\"/*.txt 2>/dev/null", folder_path);

    FILE *ls = popen(cmd, "r");
    if (!ls) {
        fprintf(stderr, "  [error] popen failed for: %s\n", folder_path);
        return 0;
    }

    char filepath[MAX_PATH];
    int  count = 0;
    while (fgets(filepath, sizeof(filepath), ls)) {
        trim(filepath);
        if (strlen(filepath) == 0) continue;
        if (parse_file(t, filepath)) {
            printf("  [ok] %s\n", filepath);
            count++;
        }
    }
    pclose(ls);

    if (count == 0)
        fprintf(stderr, "  [warn] No .txt files found in '%s'\n", folder_path);

    printf("[LOAD] Done: %d file(s), %d row(s), %d col(s)\n",
           count, t->num_rows, t->num_cols);
    return count;
}

/* ============================================================
 * SAVE TABLE  writes each row as its own .txt file
 * ============================================================ */
int save_table(Table *t, const char *folder_path) {
    char cmd[MAX_PATH + 32];
    snprintf(cmd, sizeof(cmd), "mkdir -p \"%s\"", folder_path);
    system(cmd);

    int r, c;
    for (r = 0; r < t->num_rows; r++) {
        char filepath[MAX_PATH];
        snprintf(filepath, sizeof(filepath),
                 "%s/record_%04d.txt", folder_path, r + 1);
        FILE *fp = fopen(filepath, "w");
        if (!fp) {
            fprintf(stderr, "  [error] cannot write: %s\n", filepath);
            continue;
        }
        for (c = 0; c < t->num_cols; c++)
            fprintf(fp, "%s: %s\n", t->headers[c], t->rows[r][c]);
        fprintf(fp, "\n");
        fclose(fp);
    }
    printf("[SAVE] %d records written to '%s'\n", t->num_rows, folder_path);
    t->modified = 0;
    return 1;
}

/* ============================================================
 * PRINT TABLE as ASCII grid
 * ============================================================ */
void print_table(Table *t) {
    if (!t || t->num_rows == 0) {
        printf("  (table is empty)\n");
        return;
    }
    int r, c;
    printf("\n  ");
    for (c = 0; c < t->num_cols; c++)
        printf("%-20s", t->headers[c]);
    printf("\n  ");
    for (c = 0; c < t->num_cols; c++)
        printf("--------------------");
    printf("\n");
    for (r = 0; r < t->num_rows; r++) {
        printf("  ");
        for (c = 0; c < t->num_cols; c++)
            printf("%-20s", t->rows[r][c]);
        printf("\n");
    }
    printf("\n  Total: %d row(s)\n\n", t->num_rows);
}

/* ============================================================
 * SORT  (qsort, O(n log n))
 * ============================================================ */
int sort_comparator(const void *a, const void *b) {
    char **row_a = *(char ***)a;
    char **row_b = *(char ***)b;
    return strcmp(row_a[g_sort_col], row_b[g_sort_col]);
}

void sort_table(Table *t, const char *header) {
    int col = find_column(t, header);
    if (col == -1) {
        printf("  [!] Column '%s' not found.\n", header);
        printf("  Available columns: ");
        int i; for (i = 0; i < t->num_cols; i++) printf("%s ", t->headers[i]);
        printf("\n");
        return;
    }
    g_sort_col = col;
    clock_t start = clock();
    qsort(t->rows, t->num_rows, sizeof(char **), sort_comparator);
    clock_t end = clock();
    t->modified = 1;
    printf("  [SORT] %d rows by '%s' in %.6f sec\n",
           t->num_rows, header,
           (double)(end - start) / CLOCKS_PER_SEC);
}

/* ============================================================
 * INSERT  (O(1) append)
 * ============================================================ */
void insert_row(Table *t) {
    if (t->num_rows >= MAX_ROWS) {
        printf("  [!] Table full (MAX_ROWS=%d).\n", MAX_ROWS);
        return;
    }
    char **new_row = alloc_row(t->num_cols);
    int c;
    printf("\n  Enter values for new record:\n");
    /* consume leftover newline from previous scanf */
    { int ch; while ((ch = getchar()) != '\n' && ch != EOF); }
    for (c = 0; c < t->num_cols; c++) {
        printf("    %s: ", t->headers[c]);
        fflush(stdout);
        char buf[MAX_VAL_LEN];
        if (fgets(buf, sizeof(buf), stdin)) {
            trim(buf);
            strncpy(new_row[c], buf, MAX_VAL_LEN - 1);
        }
    }
    clock_t start = clock();
    t->rows[t->num_rows++] = new_row;
    clock_t end = clock();
    t->modified = 1;
    printf("  [INSERT] Row added in %.6f sec\n",
           (double)(end - start) / CLOCKS_PER_SEC);
}

/* ============================================================
 * DELETE  (O(n) shift)
 * ============================================================ */
void delete_row(Table *t) {
    if (t->num_rows == 0) { printf("  Table is empty.\n"); return; }

    int row_num;
    printf("  Row number to delete (1-%d): ", t->num_rows);
    if (scanf("%d", &row_num) != 1) return;
    row_num--;

    if (row_num < 0 || row_num >= t->num_rows) {
        printf("  [!] Invalid row number.\n");
        return;
    }
    clock_t start = clock();
    free_row(t->rows[row_num], t->num_cols);
    int r;
    for (r = row_num; r < t->num_rows - 1; r++)
        t->rows[r] = t->rows[r + 1];
    t->num_rows--;
    clock_t end = clock();
    t->modified = 1;
    printf("  [DELETE] Row removed in %.6f sec\n",
           (double)(end - start) / CLOCKS_PER_SEC);
}

/* ============================================================
 * UPDATE  (O(1) direct write)
 * ============================================================ */
void update_row(Table *t) {
    if (t->num_rows == 0) { printf("  Table is empty.\n"); return; }

    int row_num, col_num;
    printf("  Row number to update (1-%d): ", t->num_rows);
    if (scanf("%d", &row_num) != 1) return;
    row_num--;

    printf("  Columns: ");
    int i;
    for (i = 0; i < t->num_cols; i++)
        printf("[%d]%s ", i + 1, t->headers[i]);
    printf("\n  Column number: ");
    if (scanf("%d", &col_num) != 1) return;
    col_num--;

    if (row_num < 0 || row_num >= t->num_rows ||
        col_num < 0 || col_num >= t->num_cols) {
        printf("  [!] Invalid row or column.\n");
        return;
    }
    printf("  Current value: '%s'\n  New value: ", t->rows[row_num][col_num]);
    { int ch; while ((ch = getchar()) != '\n' && ch != EOF); }
    char buf[MAX_VAL_LEN];
    if (fgets(buf, sizeof(buf), stdin)) {
        trim(buf);
        clock_t start = clock();
        strncpy(t->rows[row_num][col_num], buf, MAX_VAL_LEN - 1);
        t->rows[row_num][col_num][MAX_VAL_LEN - 1] = '\0';
        clock_t end = clock();
        t->modified = 1;
        printf("  [UPDATE] Done in %.6f sec\n",
               (double)(end - start) / CLOCKS_PER_SEC);
    }
}

/* ============================================================
 * JOIN OPERATIONS  (nested-loop, O(n x m))
 * Supports: inner | left | right | full
 * ============================================================ */
Table* join_tables(Table *a, Table *b,
                   const char *col_a, const char *col_b,
                   const char *type) {
    int ca = find_column(a, col_a);
    int cb = find_column(b, col_b);
    if (ca == -1) { printf("  [!] Column '%s' not in table A\n", col_a); return NULL; }
    if (cb == -1) { printf("  [!] Column '%s' not in table B\n", col_b); return NULL; }

    /* Result has all cols of A + all cols of B except the join col of B */
    Table *res = create_table("JOIN_RESULT");
    int c;
    for (c = 0; c < a->num_cols; c++)
        strncpy(res->headers[res->num_cols++], a->headers[c], MAX_HDR_LEN - 1);
    for (c = 0; c < b->num_cols; c++) {
        if (c == cb) continue;
        char hdr[MAX_HDR_LEN];
        snprintf(hdr, sizeof(hdr)-1, "B."); strncat(hdr, b->headers[c], sizeof(hdr)-3);
        strncpy(res->headers[res->num_cols++], hdr, MAX_HDR_LEN - 1);
    }

    int *matched_b = (int *)calloc(b->num_rows, sizeof(int));
    int ra, rb;

    clock_t start = clock();

    /* Pass 1: iterate A, find matches in B */
    for (ra = 0; ra < a->num_rows; ra++) {
        int found = 0;
        for (rb = 0; rb < b->num_rows; rb++) {
            if (strcmp(a->rows[ra][ca], b->rows[rb][cb]) != 0) continue;
            found = 1;
            matched_b[rb] = 1;
            if (res->num_rows >= MAX_ROWS) continue;
            char **row = alloc_row(res->num_cols);
            int rc = 0;
            for (c = 0; c < a->num_cols; c++)
                strncpy(row[rc++], a->rows[ra][c], MAX_VAL_LEN - 1);
            for (c = 0; c < b->num_cols; c++) {
                if (c == cb) continue;
                strncpy(row[rc++], b->rows[rb][c], MAX_VAL_LEN - 1);
            }
            res->rows[res->num_rows++] = row;
        }
        /* LEFT / FULL: include A row even when no B match */
        if (!found && (strcmp(type,"left")==0 || strcmp(type,"full")==0)) {
            if (res->num_rows < MAX_ROWS) {
                char **row = alloc_row(res->num_cols);
                int rc = 0;
                for (c = 0; c < a->num_cols; c++)
                    strncpy(row[rc++], a->rows[ra][c], MAX_VAL_LEN - 1);
                for (c = 0; c < b->num_cols - 1; c++)
                    strncpy(row[rc++], "NULL", MAX_VAL_LEN - 1);
                res->rows[res->num_rows++] = row;
            }
        }
    }

    /* Pass 2: RIGHT / FULL  add unmatched B rows */
    if (strcmp(type,"right")==0 || strcmp(type,"full")==0) {
        for (rb = 0; rb < b->num_rows; rb++) {
            if (matched_b[rb]) continue;
            if (res->num_rows >= MAX_ROWS) continue;
            char **row = alloc_row(res->num_cols);
            int rc = 0;
            for (c = 0; c < a->num_cols; c++)
                strncpy(row[rc++], "NULL", MAX_VAL_LEN - 1);
            for (c = 0; c < b->num_cols; c++) {
                if (c == cb) continue;
                strncpy(row[rc++], b->rows[rb][c], MAX_VAL_LEN - 1);
            }
            res->rows[res->num_rows++] = row;
        }
    }

    clock_t end = clock();
    free(matched_b);
    printf("  [JOIN %s] %d row(s) in %.6f sec\n",
           type, res->num_rows,
           (double)(end - start) / CLOCKS_PER_SEC);
    return res;
}

/* ============================================================
 * MINI QUERY LANGUAGE
 * Supports:
 *   SELECT col1,col2 FROM tbl
 *   SELECT * FROM tbl WHERE col = value
 *   SELECT * FROM tbl WHERE col > value
 *   SELECT * FROM t1 JOIN t2 ON t1.col = t2.col [inner|left|right|full]
 * ============================================================ */
void run_query(Table **tables, int num_tables) {
    char query[1024];
    printf("\n  Enter query:\n  > ");
    fflush(stdout);
    { int ch; while ((ch = getchar()) != '\n' && ch != EOF); }
    if (!fgets(query, sizeof(query), stdin)) return;
    trim(query);
    if (strlen(query) == 0) return;

    printf("  Running: %s\n", query);
    clock_t start = clock();

    char q[1024];
    strncpy(q, query, 1023);
    char *tokens[64];
    int   ntok = 0;
    char *tok  = strtok(q, " \t");
    while (tok && ntok < 63) { tokens[ntok++] = tok; tok = strtok(NULL," \t"); }
    tokens[ntok] = NULL;

    if (ntok < 1) return;

    /*  SELECT ... FROM ... [WHERE]  */
    if (strcasecmp(tokens[0], "SELECT") == 0 &&
        ntok >= 4 &&
        strcasecmp(tokens[2], "FROM") == 0) {

        char *cols_str = tokens[1];
        char *tbl_name = tokens[3];

        Table *t = find_table(tables, num_tables, tbl_name);
        if (!t) { printf("  [!] Table '%s' not found.\n", tbl_name); return; }

        /* WHERE */
        char *where_col = NULL, *where_op = NULL, *where_val = NULL;
        if (ntok >= 8 && strcasecmp(tokens[4], "WHERE") == 0) {
            where_col = tokens[5];
            where_op  = tokens[6];
            where_val = tokens[7];
        }

        /* Build column projection list */
        int sel[MAX_COLS], nsel = 0;
        if (strcmp(cols_str, "*") == 0) {
            int c; for (c = 0; c < t->num_cols; c++) sel[nsel++] = c;
        } else {
            char cs[256]; strncpy(cs, cols_str, 255);
            char *cp = strtok(cs, ",");
            while (cp) {
                int c = find_column(t, cp);
                if (c != -1) sel[nsel++] = c;
                else printf("  [!] Column '%s' not found, skipped.\n", cp);
                cp = strtok(NULL, ",");
            }
        }
        if (nsel == 0) { printf("  [!] No valid columns.\n"); return; }

        /* Print header */
        int i;
        printf("\n  ");
        for (i = 0; i < nsel; i++) printf("%-20s", t->headers[sel[i]]);
        printf("\n  ");
        for (i = 0; i < nsel; i++) printf("--------------------");
        printf("\n");

        /* Print rows */
        int printed = 0, r;
        for (r = 0; r < t->num_rows; r++) {
            if (where_col && where_op && where_val) {
                int wc = find_column(t, where_col);
                if (wc == -1) continue;
                double lv = atof(t->rows[r][wc]);
                double rv = atof(where_val);
                int pass = 0;
                if (strcmp(where_op,"=")  == 0) pass = strcmp(t->rows[r][wc], where_val) == 0;
                if (strcmp(where_op,"!=") == 0) pass = strcmp(t->rows[r][wc], where_val) != 0;
                if (strcmp(where_op,">")  == 0) pass = (lv >  rv);
                if (strcmp(where_op,"<")  == 0) pass = (lv <  rv);
                if (strcmp(where_op,">=") == 0) pass = (lv >= rv);
                if (strcmp(where_op,"<=") == 0) pass = (lv <= rv);
                if (!pass) continue;
            }
            printf("  ");
            for (i = 0; i < nsel; i++) printf("%-20s", t->rows[r][sel[i]]);
            printf("\n");
            printed++;
        }
        clock_t end = clock();
        printf("\n  %d row(s) in %.6f sec\n",
               printed, (double)(end - start) / CLOCKS_PER_SEC);
        return;
    }

    /*  SELECT * FROM t1 JOIN t2 ON t1.col = t2.col [TYPE]  */
    if (strcasecmp(tokens[0], "SELECT") == 0 &&
        ntok >= 10 &&
        strcasecmp(tokens[4], "JOIN") == 0 &&
        strcasecmp(tokens[6], "ON")   == 0) {

        char *tbl_a = tokens[3];
        char *tbl_b = tokens[5];

        char col_a[MAX_HDR_LEN], col_b[MAX_HDR_LEN];
        strncpy(col_a, tokens[7], MAX_HDR_LEN - 1); col_a[MAX_HDR_LEN-1]='\0';
        strncpy(col_b, tokens[9], MAX_HDR_LEN - 1); col_b[MAX_HDR_LEN-1]='\0';
        char *dot;
        dot = strchr(col_a, '.'); if (dot) memmove(col_a, dot+1, strlen(dot));
        dot = strchr(col_b, '.'); if (dot) memmove(col_b, dot+1, strlen(dot));

        char jtype[16] = "inner";
        if (ntok >= 11) { strncpy(jtype, tokens[10], 15); jtype[15]='\0'; }
        int k; for (k = 0; jtype[k]; k++) jtype[k] = (char)tolower(jtype[k]);

        Table *ta = find_table(tables, num_tables, tbl_a);
        Table *tb = find_table(tables, num_tables, tbl_b);
        if (!ta) { printf("  [!] Table '%s' not found.\n", tbl_a); return; }
        if (!tb) { printf("  [!] Table '%s' not found.\n", tbl_b); return; }

        Table *result = join_tables(ta, tb, col_a, col_b, jtype);
        if (result) { print_table(result); free_table(result); }
        return;
    }

    printf("  [!] Unrecognised query.\n");
    printf("      SELECT col1,col2 FROM tbl\n");
    printf("      SELECT * FROM tbl WHERE col > value\n");
    printf("      SELECT * FROM t1 JOIN t2 ON t1.col=t2.col [inner|left|right|full]\n");
}

/* ============================================================
 * MENU
 * ============================================================ */
void show_menu(void) {
    printf("\n========================================\n");
    printf("  HBTU MCA - Mini DB System (Q1 + Q2)  \n");
    printf("========================================\n");
    printf("  [1] Print table\n");
    printf("  [2] Sort by column\n");
    printf("  [3] Insert row\n");
    printf("  [4] Delete row\n");
    printf("  [5] Update row\n");
    printf("  [6] Save table to files\n");
    printf("  [7] Join two tables\n");
    printf("  [8] Run query (SELECT/WHERE/JOIN)\n");
    printf("  [9] List / switch tables\n");
    printf("  [0] Exit\n");
    printf("  Choice: ");
    fflush(stdout);
}

/* ============================================================
 * MAIN
 * ============================================================ */
int main(int argc, char *argv[]) {
    printf("\n==============================================\n");
    printf("  DSUC+DBMS Assignment 1 - MCA, HBTU\n");
    printf("  Generic In-Memory Database System in C\n");
    printf("==============================================\n");

    Table *tables[8];
    int    num_tables = 0;

    if (argc < 2) {
        /* No folder given  create a built-in demo */
        printf("\nUsage: %s <folder1> [folder2] ...\n", argv[0]);
        printf("Example: %s MCASampleData1 MCASampleData2\n\n", argv[0]);
        printf("No folder given. Loading built-in demo data...\n");

        Table *demo = create_table("demo");
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
        demo->rows[0] = r0;
        demo->rows[1] = r1;
        demo->rows[2] = r2;
        demo->num_rows = 3;
        tables[num_tables++] = demo;

    } else {
        int i;
        for (i = 1; i < argc && num_tables < 8; i++) {
            Table *t = create_table(argv[i]);
            if (load_folder(t, argv[i]) > 0) {
                tables[num_tables++] = t;
                printf("  Loaded table '%s': %d rows, %d cols\n",
                       t->name, t->num_rows, t->num_cols);
            } else {
                printf("  [warn] No data loaded from '%s'\n", argv[i]);
                free_table(t);
            }
        }
    }

    if (num_tables == 0) {
        printf("[!] No data loaded. Check folder names and .txt files.\n");
        return 1;
    }

    Table *active = tables[0];
    int    choice;

    while (1) {
        show_menu();
        if (scanf("%d", &choice) != 1) { choice = -1; }

        switch (choice) {

        case 1:
            printf("  Table: %s\n", active->name);
            print_table(active);
            break;

        case 2: {
            char hdr[MAX_HDR_LEN];
            printf("  Sort by column name: ");
            scanf("%63s", hdr);
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
                printf("    [%d] %s  (%d rows)\n",
                       k, tables[k]->name, tables[k]->num_rows);
            int ia, ib;
            printf("  Table A index: "); scanf("%d", &ia);
            printf("  Table B index: "); scanf("%d", &ib);
            if (ia < 0 || ia >= num_tables ||
                ib < 0 || ib >= num_tables) {
                printf("  [!] Invalid index.\n"); break;
            }
            char ca[MAX_HDR_LEN], cb[MAX_HDR_LEN], jtype[16];
            printf("  Join column in A: "); scanf("%63s", ca);
            printf("  Join column in B: "); scanf("%63s", cb);
            printf("  Join type (inner/left/right/full): "); scanf("%15s", jtype);
            int k2; for (k2=0;jtype[k2];k2++) jtype[k2]=(char)tolower(jtype[k2]);

            Table *res = join_tables(tables[ia], tables[ib], ca, cb, jtype);
            if (res) {
                print_table(res);
                printf("  Save result to disk? (1=yes, 0=no): ");
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
                       tables[k]->modified ? " [unsaved changes]" : "");
            printf("  Active: %s\n  Switch to index: ", active->name);
            int idx; scanf("%d", &idx);
            if (idx >= 0 && idx < num_tables) {
                active = tables[idx];
                printf("  Now using: %s\n", active->name);
            }
            break;
        }

        case 0: {
            int k;
            for (k = 0; k < num_tables; k++)
                if (tables[k]->modified)
                    printf("  [!] '%s' has unsaved changes!\n",
                           tables[k]->name);
            printf("  Goodbye!\n");
            for (k = 0; k < num_tables; k++) free_table(tables[k]);
            return 0;
        }

        default:
            printf("  [!] Invalid choice. Enter 0-9.\n");
        }
    }
}
