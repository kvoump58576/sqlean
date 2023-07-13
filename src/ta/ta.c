#include <sqlite3ext.h>

SQLITE_EXTENSION_INIT1

static void emaFinalize(sqlite3_context *context) {}
static void inverse(sqlite3_context *context, int argc, sqlite3_value **value) {}
static void destroy(void *context) {}

typedef struct {
    int count;
    int window;
    double sum;
    double *values;
} SmaContext;

static inline double keep3digits(double value) { return (int)(value * 1000) / 1000.0; }

static void smaStep(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "Invalid number of arguments. Two arguments are required.",
                             -1);
        return;
    }

    SmaContext *smaContext = (SmaContext *)sqlite3_aggregate_context(context, sizeof(SmaContext));

    if (smaContext->values == 0) {
        smaContext->window = sqlite3_value_int(argv[1]);
        smaContext->count = 0;
        smaContext->sum = 0;
        smaContext->values = (double *)sqlite3_malloc(sizeof(double) * smaContext->window);
    }

    double value = sqlite3_value_double(argv[0]);

    if (smaContext->count < smaContext->window) {
        smaContext->values[smaContext->count] = value;
        smaContext->sum += value;
        smaContext->count++;
    } else {
        int index = smaContext->count % smaContext->window;
        smaContext->sum -= smaContext->values[index];
        smaContext->values[index] = value;
        smaContext->sum += value;
        smaContext->count++;
    }
}
static void smaValue(sqlite3_context *context) {
    SmaContext *smaContext = (SmaContext *)sqlite3_aggregate_context(context, sizeof(SmaContext));
    int count=smaContext->count>smaContext->window?smaContext->window:smaContext->count;
    sqlite3_result_double(context, keep3digits(smaContext->sum / count));
}
static void smaFinalize(sqlite3_context *context) {
    SmaContext *smaContext = (SmaContext *)sqlite3_aggregate_context(context, sizeof(SmaContext));
    sqlite3_free(smaContext->values);
}

typedef struct {
    double alpha;
    double ema;
    int step;
} EmaContext;
static void emaStep(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "Invalid number of arguments. Expected 3.", -1);
        return;
    }

    double currentValue = sqlite3_value_double(argv[0]);
    EmaContext *emaCtx = (EmaContext *)sqlite3_aggregate_context(context, sizeof(EmaContext));
    if (emaCtx->step == 0) {
        double alpha = 2 / (sqlite3_value_double(argv[1]) + 1);
        emaCtx->alpha = alpha;
        emaCtx->ema = currentValue;
        emaCtx->step++;
    } else {
        double alpha = emaCtx->alpha;
        double ema = (alpha * currentValue) + ((1 - alpha) * emaCtx->ema);
        emaCtx->ema = ema;
    }
}

static void emaValue(sqlite3_context *context) {
    EmaContext *emaCtx = (EmaContext *)sqlite3_aggregate_context(context, sizeof(EmaContext));
    sqlite3_result_double(context, keep3digits(emaCtx->ema));
}

typedef struct {
    sqlite3_value *value;
} SelectContext;

static void firstStep(sqlite3_context *context, int argc, sqlite3_value **argv) {
    SelectContext *selectContext =
        (SelectContext *)sqlite3_aggregate_context(context, sizeof(SelectContext));
    if (selectContext->value == 0) {
        selectContext->value = sqlite3_value_dup(argv[0]);
    }
}
static void lastStep(sqlite3_context *context, int argc, sqlite3_value **argv) {
    SelectContext *selectContext =
        (SelectContext *)sqlite3_aggregate_context(context, sizeof(SelectContext));
    sqlite3_value_free(selectContext->value);
    selectContext->value = sqlite3_value_dup(argv[0]);
}
static void selectFinalize(sqlite3_context *context) {
    SelectContext *selectContext =
        (SelectContext *)sqlite3_aggregate_context(context, sizeof(SelectContext));
    sqlite3_result_value(context, selectContext->value);
    sqlite3_value_free(selectContext->value);
}

int sqlite3_extension_init(sqlite3 *db, char **err, const sqlite3_api_routines *api) {
    SQLITE_EXTENSION_INIT2(api);
    sqlite3_create_window_function(db, "sma", 2, SQLITE_UTF8, 0, smaStep, smaFinalize, smaValue,
                                   inverse, destroy);
    sqlite3_create_window_function(db, "ema", 2, SQLITE_UTF8, 0, emaStep, emaFinalize, emaValue,
                                   inverse, destroy);
    sqlite3_create_function(db, "first", 1, SQLITE_UTF8, 0, 0, firstStep, selectFinalize);
    sqlite3_create_function(db, "last", 1, SQLITE_UTF8, 0, 0, lastStep, selectFinalize);
    return SQLITE_OK;
}

int sqlite3_ta_init(sqlite3 *db, char **err, const sqlite3_api_routines *api) { return SQLITE_OK; }