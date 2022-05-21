################################################################################
# TODO:
################################################################################

setwd("~/Documents/stock_price_forecast/analysis/")

################################################################################

library(fs)
library(glue)
library(tidyverse)
library(lubridate)

`%||%` <- rlang::`%||%`

################################################################################

data_dir <- fs::path_wd("data")
temp_dir <- fs::dir_create(fs::path(data_dir, "temp"))

################################################################################

market_data <- DBI::dbConnect(
  RSQLite::SQLite(), dbname=path(data_dir, "market_data.db"), 
  extended_types=TRUE
)

################################################################################

n_stocks <- 2
n_lags <- 15
n_train <- 1500
n_test <- 50
n_batch <- 50

y_name <- "adj_close_log_return_class"
x_lstm_vars <- vars(c(adj_close_log_return, adj_close, volume))
x_single_vars <- vars(c(day_obs, ends_with("_lag1")))

################################################################################
################################################################################
################################################################################


stock_ts <-
  market_data %>% 
  tbl("bars_day") %>% 
  filter(!is.na(volume)) %>% 
  group_by(symbol) %>% 
  filter(n() >=  n_lags + n_train + n_lags + n_test + 26) %>% 
  filter(sum(close <= 0, na.rm=TRUE) == 0) %>% 
  ungroup() %>% 
  collect() %>% 
  mutate(date = as_date(time)) %>% 
  select(-time) %>% 
  nest(bars_day=-symbol) %>% 
  mutate(volume_recent = map_dbl(bars_day, function(.bars_day){
    sum(pull(slice_max(.bars_day, order_by=date, n=100), volume))
  })) %>% 
  slice_max(volume_recent, n=n_stocks) %>% 
  select(-volume_recent) %>% 
  unnest(bars_day) %>% 
  select(sym=symbol, everything()) %>% 
  mutate(adj_close = close) %>% 
  complete(sym, date) %>% 
  group_by(date) %>% 
  filter(!any(is.na(c_across(c(open, close, volume))))) %>% 
  group_by(sym) %>% 
  arrange(date) %>% 
  mutate(
    adj_close_return = `/`(
      adj_close - lag(adj_close, n=1),
      lag(adj_close, n=1)
    ),
    adj_close_log_return = log(adj_close/lag(adj_close, n=1)),
    rsi = TTR::RSI(price=adj_close),
    bband = apply(
      TTR::BBands(cbind(high, close, low))[, c("up", "dn")], 1, as.list
    ),
    macd = apply(TTR::MACD(x=close, maType="EMA"), 1, as.list),
    date_days_ago = lubridate::time_length(date %--% max(date), unit="days"),
    date_wday = fct_drop(fct_relabel(
      lubridate::wday(date, label=TRUE, locale="en_GB.UTF-8"), 
      str_to_lower
    ))
  ) %>% 
  unnest_wider(bband, names_sep="_") %>% 
  unnest_wider(macd, names_sep="_") %>% 
  slice_max(order(date), n=n_train + n_test + 2 * n_lags) %>% 
  mutate(day_obs = order(date)) %>% 
  ungroup() %>% 
  arrange(sym, date) %>% 
  mutate(
    sample_group = case_when(
      `&`(day_obs > n_lags, 
          day_obs <= n_lags + n_train) ~ 
        "train",
      `&`(day_obs > n_lags + n_train + n_lags,
          day_obs <= n_lags + n_train + n_lags + n_test) ~ 
        "test",
      TRUE ~ 
        NA_character_
    ),
    adj_close_log_return_class = cut.default(
      x=adj_close_log_return, 
      breaks=
        adj_close_log_return %>% 
        `[`(!is.na(adj_close_log_return) & sample_group=="train") %>% 
        abs() %>% 
        quantile(probs=c(0.25, 0.5, 0.75), na.rm=TRUE) %>% 
        c(Inf) %>% 
        (function(.breaks_pos){c(-.breaks_pos, +.breaks_pos)}),
      include.lowest=TRUE, ordered_result=TRUE
    ),
    adj_close_log_return_pos = as.integer(adj_close_return>0),
  ) %>% 
  fastDummies::dummy_cols(
    select_columns="date_wday", remove_selected_columns=TRUE
  ) %>% 
  group_by(sym) %>% 
  mutate(across(
    c(all_of(y_name), matches("^((rsi|bband|macd)|date_(wday|days))")), 
    lag, n=1, .names="{.col}_lag1"
  )) %>% 
  (function(.df){
    .GlobalEnv[["scale_weight_store"]] <- tibble(); return(.df)
  }) %>% 
  mutate(across(c(where(is.numeric), -day_obs), function(.vec){

    if(all(.vec %in% c(0L, 1L, NA_integer_))){return(.vec)}
    
    .vec_ref <- .vec[map_lgl(.data$sample_group == "train", isTRUE)]
    .vec_ref_mean <- mean(.vec_ref)
    .vec_ref_sd <- sd(.vec_ref)
    
    if(is.infinite(.vec_ref_mean)){stop("vcds")}
    
    .vec_scaled <- (.vec - .vec_ref_mean) / .vec_ref_sd
    
    .scale_weight <- cur_group() %>% mutate(
      column=cur_column(), mean=.vec_ref_mean, sd=.vec_ref_sd
    )

    .GlobalEnv[["scale_weight_store"]] <- bind_rows(
      .GlobalEnv[["scale_weight_store"]], .scale_weight
    )
    
    return(.vec_scaled)
    
  })) %>% 
  ungroup() %>% 
  mutate(across(
    adj_close_log_return_class_lag1, function(.vec){as.integer(.vec)-1L}
  )) %>%
  mutate(across(
    adj_close_log_return_class, function(.vec){as.integer(.vec)-1L}
  )) %>% 
  mutate(across(!!!x_lstm_vars, function(.vec){
    
    .mat_lag <-
      map_dfc(1:n_lags, function(..lag){
        as_tibble_col(
          dplyr::lag(.vec, n=..lag), column_name=as.character(..lag)
        )
      }) %>% 
      rowwise() %>% 
      summarise(lags=list(c_across(everything()))) %>% 
      pull(lags)
    
    return(.mat_lag)
    
  }, .names="{.col}_lstm")) %>% 
  ungroup() %>% 
  drop_na(sample_group)

################################################################################

stock_ts %>% 
  select(-where(is.list)) %>% 
  mutate(across(where(is.character), as.factor)) %>% 
  summary()

################################################################################

stock_ts %>% 
  ggplot(aes(x=adj_close_log_return, fill=factor(adj_close_log_return_class))) +
  geom_histogram(aes(y=..count..)) +
  lims(x=c(-5, 5))

################################################################################
################################################################################
################################################################################

long_df_to_ndim_array <- function(.long_df, .value_col, .dim_cols){
  
  stopifnot(nrow(.long_df) == nrow(distinct(.long_df, across({{.dim_cols}}))))
  
  .long_df_prep <<- 
    .long_df %>% 
    select({{.value_col}}, {{.dim_cols}}) %>% 
    arrange(across(c(last_col():1, -{{.value_col}})))
  
  .ndim_array <<- array(
    data=pull(select(.long_df_prep, {{.value_col}})),
    dim=map_int(select(.long_df_prep, -{{.value_col}}), n_distinct),
    dimnames=map(select(.long_df_prep, -{{.value_col}}), kit::funique)
  )
  
  return(.ndim_array)
  
}

model_data <- 
  stock_ts %>% 
  group_by(sym, sample_group) %>% 
  group_modify(function(.gdata, .gkeys){#.gdata <<- .gdata; .gkeys <<- .gkeys; stop()
    .model_data <- tibble(model_data = list(list(
      y =
        .gdata %>%
        select(day_obs, all_of(y_name)) %>% 
        pivot_longer(-day_obs, names_to="feature") %>%
        long_df_to_ndim_array(value, c(day_obs, feature)),
      x_lstm =
        .gdata %>%
        select(day_obs, where(is.list) & ends_with("lstm")) %>%
        pivot_longer(-day_obs, names_to="feature") %>%
        unnest_longer(value, indices_to="lag") %>%
        long_df_to_ndim_array(value, c(day_obs, lag, feature)),
      x_single =
        .gdata %>%
        select(!!!x_single_vars) %>% 
        pivot_longer(-day_obs, names_to="feature") %>%
        long_df_to_ndim_array(value, c(day_obs, feature))
    )))
    return(.model_data)
  }) %>% 
  group_by(sym) %>% 
  summarise(
    model_data = list(`names<-`(model_data, sample_group)), .groups="drop"
  ) %>% 
  summarise(
    model_data = list(`names<-`(model_data, sym))
  ) %>% 
  pluck("model_data", 1)

################################################################################

input_data_train <- flatten(imap(model_data, function(.model_data, .sym){
  .input_data <- .model_data$train[c("x_lstm", "x_single")]
  .input_data <- .input_data %>% set_names(
    str_replace(names(.input_data), "^x", str_c("input_", .sym))
  )
}))
input_data_test <- flatten(imap(model_data, function(.model_data, .sym){
  .input_data <- .model_data$test[c("x_lstm", "x_single")]
  .input_data <- .input_data %>% set_names(
    str_replace(names(.input_data), "^x", str_c("input_", .sym))
  )
}))
output_data_train <- flatten(imap(model_data, function(.model_data, .sym){
  .input_data <- .model_data$train[c("y")]
  .input_data <- .input_data %>% set_names(
    str_replace(names(.input_data), "^y$", str_c("output_", .sym))
  )
}))
output_data_test <- flatten(imap(model_data, function(.model_data, .sym){
  .input_data <- .model_data$test[c("y")]
  .input_data <- .input_data %>% set_names(
    str_replace(names(.input_data), "^y$", str_c("output_", .sym))
  )
}))

