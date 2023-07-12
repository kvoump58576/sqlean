#include <stdio.h>
// #include <sqlite3.h>
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
typedef struct {
  int count;
  int window;
  double sum;
  double *values;
} WindowData;

static void smaStep(sqlite3_context *context, int argc, sqlite3_value **argv) {
  if (argc != 2) {
    sqlite3_result_error(context, "Invalid number of arguments. Two arguments are required.", -1);
    return;
  }

  WindowData *windowData = sqlite3_aggregate_context(context, sizeof(WindowData));

  if (windowData->values == NULL) {
    windowData->window = sqlite3_value_int(argv[1]);
    windowData->count = 0;
    windowData->sum = 0;
    windowData->values = sqlite3_malloc(sizeof(double) * windowData->window);
  }

  double value = sqlite3_value_double(argv[0]);

  if (windowData->count < windowData->window) {
    windowData->values[windowData->count] = value;
    windowData->sum += value;
    windowData->count++;
  } else {
    int index=windowData->count%windowData->window;
    windowData->sum -= windowData->values[index];
    windowData->values[index] = value;
    windowData->sum += value; 

    double sma = windowData->sum / windowData->window;

    sqlite3_result_double(context, sma);
  }
}


static void smaFinalize(sqlite3_context *context) {
  WindowData *windowData = sqlite3_aggregate_context(context, sizeof(WindowData));
  sqlite3_free(windowData->values);
}



int sqlite3_extension_init(sqlite3 *db, char **err, const sqlite3_api_routines *api) {
  SQLITE_EXTENSION_INIT2(api);
  int rc = sqlite3_create_window_function(
    db,
    "sma",
    2,
    SQLITE_UTF8 | SQLITE_DETERMINISTIC,
    NULL,
    smaStep,
    smaFinalize,
    NULL,
    NULL,
    NULL
  );
  if (rc != SQLITE_OK) {
    return rc;
  }

  return SQLITE_OK;
}
