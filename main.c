#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_FIELDS 20
#define MAX_HEADER 50
#define MAX_VALUE 100
#define MAX_LINE 200

typedef struct {
    char header[MAX_HEADER];
    char value[MAX_VALUE];
} Field;

typedef struct Record {
    Field fields[MAX_FIELDS];
    int fieldCount;
    struct Record *next;
} Record;

/* ---------- Create Record ---------- */
Record* createRecord() {
    Record *newRecord = (Record*)malloc(sizeof(Record));
    newRecord->fieldCount = 0;
    newRecord->next = NULL;
    return newRecord;
}

/* ---------- Add Field ---------- */
void addField(Record *record, char *header, char *value) {
    strcpy(record->fields[record->fieldCount].header, header);
    strcpy(record->fields[record->fieldCount].value, value);
    record->fieldCount++;
}

/* ---------- Display ---------- */
void displayRecords(Record *head) {
    Record *temp = head;
    int count = 1;

    while (temp != NULL) {
        printf("\n===== Record %d =====\n", count);

        for (int i = 0; i < temp->fieldCount; i++) {
            printf("%s : %s\n",
                   temp->fields[i].header,
                   temp->fields[i].value);
        }

        temp = temp->next;
        count++;
    }
}

/* ---------- Insert New Record ---------- */
void insertRecord(Record **head) {
    Record *newRecord = createRecord();
    int n;

    printf("Enter number of fields: ");
    scanf("%d", &n);

    for (int i = 0; i < n; i++) {
        char header[MAX_HEADER];
        char value[MAX_VALUE];

        printf("Enter Header: ");
        scanf("%s", header);

        printf("Enter Value: ");
        scanf("%s", value);

        addField(newRecord, header, value);
    }

    if (*head == NULL) {
        *head = newRecord;
    } else {
        Record *temp = *head;
        while (temp->next != NULL)
            temp = temp->next;

        temp->next = newRecord;
    }

    printf("\nRecord Inserted Successfully!\n");
}

/* ---------- Delete Record by Header Value ---------- */
void deleteRecord(Record **head, char *header, char *value) {
    Record *temp = *head;
    Record *prev = NULL;

    while (temp != NULL) {
        for (int i = 0; i < temp->fieldCount; i++) {
            if (strcmp(temp->fields[i].header, header) == 0 &&
                strcmp(temp->fields[i].value, value) == 0) {

                if (prev == NULL)
                    *head = temp->next;
                else
                    prev->next = temp->next;

                free(temp);
                printf("\nRecord Deleted Successfully!\n");
                return;
            }
        }

        prev = temp;
        temp = temp->next;
    }

    printf("\nRecord Not Found!\n");
}

/* ---------- Update Record ---------- */
void updateRecord(Record *head, char *searchHeader,
                  char *searchValue,
                  char *updateHeader,
                  char *newValue) {
    Record *temp = head;

    while (temp != NULL) {
        int found = 0;

        for (int i = 0; i < temp->fieldCount; i++) {
            if (strcmp(temp->fields[i].header, searchHeader) == 0 &&
                strcmp(temp->fields[i].value, searchValue) == 0) {
                found = 1;
                break;
            }
        }

        if (found) {
            for (int i = 0; i < temp->fieldCount; i++) {
                if (strcmp(temp->fields[i].header, updateHeader) == 0) {
                    strcpy(temp->fields[i].value, newValue);
                    printf("\nRecord Updated Successfully!\n");
                    return;
                }
            }
        }

        temp = temp->next;
    }

    printf("\nRecord Not Found!\n");
}

/* ---------- Save Back to File ---------- */
void saveToFile(Record *head, const char *filename) {
    FILE *fp = fopen(filename, "w");

    if (fp == NULL) {
        printf("Cannot open file for writing!\n");
        return;
    }

    Record *temp = head;

    while (temp != NULL) {
        for (int i = 0; i < temp->fieldCount; i++) {
            fprintf(fp, "%s: %s\n",
                    temp->fields[i].header,
                    temp->fields[i].value);
        }

        fprintf(fp, "\n");
        temp = temp->next;
    }

    fclose(fp);
    printf("\nData Saved Successfully to File!\n");
}

/* ---------- Main ---------- */
int main() {
    Record *head = NULL;
    int choice;

    while (1) {
        printf("\n===== MENU =====\n");
        printf("1. Insert Record\n");
        printf("2. Delete Record\n");
        printf("3. Update Record\n");
        printf("4. Display Records\n");
        printf("5. Save to File\n");
        printf("6. Exit\n");

        printf("Enter choice: ");
        scanf("%d", &choice);

        if (choice == 1) {
            insertRecord(&head);
        }

        else if (choice == 2) {
            char h[MAX_HEADER], v[MAX_VALUE];

            printf("Enter Header to search: ");
            scanf("%s", h);

            printf("Enter Value to delete: ");
            scanf("%s", v);

            deleteRecord(&head, h, v);
        }

        else if (choice == 3) {
            char sh[MAX_HEADER], sv[MAX_VALUE];
            char uh[MAX_HEADER], nv[MAX_VALUE];

            printf("Search Header: ");
            scanf("%s", sh);

            printf("Search Value: ");
            scanf("%s", sv);

            printf("Update Header: ");
            scanf("%s", uh);

            printf("New Value: ");
            scanf("%s", nv);

            updateRecord(head, sh, sv, uh, nv);
        }

        else if (choice == 4) {
            displayRecords(head);
        }

        else if (choice == 5) {
            char filename[100];

            printf("Enter filename: ");
            scanf("%s", filename);

            saveToFile(head, filename);
        }

        else if (choice == 6) {
            break;
        }

        else {
            printf("Invalid Choice!\n");
        }
    }

    return 0;
}
