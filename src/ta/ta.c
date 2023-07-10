
#include <math.h>
#include <stddef.h>
#undef SQLITE_OMIT_LOAD_EXTENSION
#undef SQLITE_CORE
#include <sqlite3ext.h>




typedef struct MovingAvgContext {
  int windowSize;
  int count;
  double sum;
  double *values;
} MovingAvgContext;

typedef struct EMACtx {
  double alpha;
  double prevEMA;
  int count;
} EMACtx;

static void smaStep(sqlite3_context *context, int argc, sqlite3_value **argv) {
  MovingAvgContext *maCtx;

  if (sqlite3_user_data(context) == NULL) {
    // 初始化窗口上下文
    maCtx = sqlite3_malloc(sizeof(MovingAvgContext));
    maCtx->windowSize = 0;
    maCtx->count = 0;
    maCtx->sum = 0;
    maCtx->values = NULL;

    sqlite3_result_pointer(context, maCtx, "MovingAvgContext", NULL);
  } else {
    maCtx = (MovingAvgContext *)sqlite3_user_data(sqlite3_context_db_handle(context));

    if (maCtx->count < maCtx->windowSize) {
      double currentValue = sqlite3_value_double(argv[0]);

      maCtx->sum += currentValue;
      maCtx->values[maCtx->count] = currentValue;
      maCtx->count++;
    } else {
      double currentValue = sqlite3_value_double(argv[0]);
      double oldestValue = maCtx->values[maCtx->count % maCtx->windowSize];

      maCtx->sum = maCtx->sum - oldestValue + currentValue;
      maCtx->values[maCtx->count % maCtx->windowSize] = currentValue;
      maCtx->count++;
    }
  }
}

static void smaFinal(sqlite3_context *context) {
  MovingAvgContext *maCtx = (MovingAvgContext *)sqlite3_user_data(sqlite3_context_db_handle(context));

  if (maCtx != NULL && maCtx->count > 0) {
    sqlite3_result_double(context, maCtx->sum / maCtx->count);
  }
}

static void smaValueDestructor(void *p) {
  MovingAvgContext *maCtx = (MovingAvgContext *)p;

  if (maCtx != NULL) {
    sqlite3_free(maCtx->values);
    sqlite3_free(maCtx);
  }
}

static void emaStep(sqlite3_context *context, int argc, sqlite3_value **argv) {
  EMACtx *emaCtx;

  if (sqlite3_user_data(context) == NULL) {
    // 初始化上下文
    emaCtx = sqlite3_malloc(sizeof(EMACtx));
    emaCtx->alpha = 2.0 / (sqlite3_value_double(argv[0]) + 1.0);
    emaCtx->prevEMA = 0.0;
    emaCtx->count = 0;

    sqlite3_result_pointer(context, emaCtx, "EMACtx", NULL);
  } else {
    emaCtx = (EMACtx *)sqlite3_user_data(sqlite3_context_db_handle(context));

    if (emaCtx->count == 0) {
      // 第一个值为初始EMA
      emaCtx->prevEMA = sqlite3_value_double(argv[1]);
    } else {
      double currentValue = sqlite3_value_double(argv[1]);

      // 计算当前EMA
      double ema = (currentValue * emaCtx->alpha) + (emaCtx->prevEMA * (1 - emaCtx->alpha));
      emaCtx->prevEMA = ema;
    }

    emaCtx->count++;
  }
}

static void emaFinal(sqlite3_context *context) {
  EMACtx *emaCtx = (EMACtx *)sqlite3_user_data(sqlite3_context_db_handle(context));

  if (emaCtx != NULL && emaCtx->count > 0) {
    sqlite3_result_double(context, emaCtx->prevEMA);
  }
}

static void emaValueDestructor(void *p) {
  EMACtx *emaCtx = (EMACtx *)p;

  if (emaCtx != NULL) {
    sqlite3_free(emaCtx);
  }
}

int sqlite3_extension_init(sqlite3 *db, char **errmsg, const sqlite3_api_routines *api) {
  SQLITE_EXTENSION_INIT2(api);

  sqlite3_create_window_function_v2(db, "sma", 1, SQLITE_UTF8, NULL, smaStep, smaFinal, NULL, smaValueDestructor);
  sqlite3_create_window_function_v2(db, "ema", 2, SQLITE_UTF8, NULL, emaStep, emaFinal, NULL, emaValueDestructor);

  return SQLITE_OK;
}
