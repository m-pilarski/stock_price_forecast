################################################################################
# TODO:
################################################################################

setwd("~/Documents/stock_price_forecast/analysis/")

################################################################################

library(fs)
library(glue)
library(tidyverse)
library(lubridate)
library(RSQLite)
library(DBI)

source("./resources/best_map.R")

################################################################################

conda_name <- "stock_price_forecast"
py_deps <- c("alpaca-trade-api")

if(!conda_name %in% reticulate::conda_list()$name){
  reticulate::conda_create(envname=conda_name)
}

reticulate::use_condaenv(condaenv=conda_name)

local({
  py_deps_miss <- discard(
    py_deps, `%in%`, reticulate::py_list_packages(envname=conda_name)$package
  )
  if(length(py_deps_miss) > 0){
    reticulate::conda_install(
      envname=conda_name, packages=py_deps_miss, pip=TRUE
    )
  }
})

################################################################################

data_dir <- fs::path_wd("data")
temp_dir <- fs::dir_create(fs::path(data_dir, "temp"))

################################################################################

market_data <- DBI::dbConnect(
  RSQLite::SQLite(), dbname=path(data_dir, "market_data.db"),
  extended_types=TRUE
)

walk(c("day", "hour"), function(.resolution){
  DBI::dbWriteTable(
    conn=market_data, name=str_c("bars_", .resolution), append=TRUE,
    value=tibble(
      symbol=character(), time=POSIXct(), open=double(), high=double(),
      low=double(), close=double(), volume=double(), trade_count=integer(),
      vwap=double()
    )
  )
})

################################################################################

py_bif <- reticulate::import_builtins()
alp_api <- reticulate::import(module="alpaca_trade_api")

################################################################################

alp_api_rest <- alp_api$REST(
  key_id=Sys.getenv()[["ALPACA_KEY"]],
  secret_key=Sys.getenv()[["ALPACA_SECRET"]],
  base_url="https://paper-api.alpaca.markets",
  api_version="v2"
)

################################################################################

format_RFC3339 <- function(.POSIXct){
  stopifnot(tz(.POSIXct) == "UTC")
  format.POSIXct(.POSIXct, format="%Y-%Om-%0dT%H:%M:%SZ")
}

################################################################################

update_bars <- function(.symbol, .start, .end, .resolution){

  # .symbol <<- .symbol; .start <<- .start; .end <<- .end; .resolution <<- .resolution#; stop()

  .table_name <- str_c("bars_", .resolution)

  stopifnot(.resolution %in% c("day", "hour"))
  stopifnot(is(market_data, "SQLiteConnection"))
  stopifnot(.table_name %in% DBI::dbListTables(market_data))

  .start <- floor_date(as_datetime(.start, tz="UTC"), unit=.resolution)
  .end <- ceiling_date(as_datetime(.end, tz="UTC"), unit=.resolution)

  if(.resolution=="day"){
    .timestep <- days(1)
    .timeframe <- alp_api$TimeFrame$Day
    .end_query_offset <- days(1)-seconds(1)
  }else if(.resolution=="hour"){
    .timestep <- hours(1)
    .timeframe <- alp_api$TimeFrame$Hour
    .end_query_offset <- hours(1)-seconds(1)
  }

  .end <- min(
    .end, floor_date(now(tzone="UTC")-minutes(15), unit=.resolution)-.timestep
  )

  .time_have <-
    pull(filter(tbl(market_data, .table_name), symbol==.symbol), time)
  .time_want <-
    seq.POSIXt(from=.start, to=.end, by=.resolution)
  .time_miss <-
    .time_want[!.time_want %in% .time_have]
  .time_miss_split_idx <-
    cumsum(c(TRUE, time_length(diff(.time_miss), unit=.resolution)!=1))
  .time_miss_split <-
    compact(split(.time_miss, .time_miss_split_idx))

  .bars_new <- map_dfr(.time_miss_split, function(..time_miss){

    ..bars_chunk <-
      alp_api_rest$get_bars(
        symbol=.symbol, timeframe=.timeframe, limit=10000L,
        start=format_RFC3339(min(..time_miss)),
        end=format_RFC3339(max(..time_miss)+.end_query_offset)
      ) %>%
      pluck(
        "df", .default=data.frame(row.names=format_RFC3339(..time_miss))
      ) %>%
      as_tibble(rownames="time") %>%
      mutate(across(time, function(...time_str){
        floor_date(ymd_hms(...time_str, tz="UTC"), unit=.resolution)
      })) %>%
      complete(time = ..time_miss) %>%
      add_column(symbol=.symbol, .before=1)

    return(..bars_chunk)

  })

  DBI::dbWriteTable(
    conn=market_data, name=.table_name, append=TRUE, value=.bars_new
  )

  gc()

  return(NULL)

}

################################################################################
################################################################################
################################################################################

sp500 <-
  str_c("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies") %>%
  rvest::read_html() %>%
  rvest::html_nodes("table") %>%
  keep(function(.node){xml2::xml_attr(.node, "id")=="constituents"}) %>%
  chuck(1) %>%
  rvest::html_table() %>%
  janitor::clean_names()

best_map(
  chuck(sp500, "symbol"), update_bars,
  .start=today(tzone="UTC")-years(30),
  .end=today(tzone="UTC"),
  .resolution="day"
)
best_map(
  chuck(sp500, "symbol"), update_bars,
  .start=today(tzone="UTC")-years(5),
  .end=now(tzone="UTC"),
  .resolution="hour"
)

market_data %>%
  tbl("bars_day") %>%
  filter(symbol=="AAPL" & !is.na(volume)) %>%
  collect() %>%
  distinct(time, .keep_all=TRUE) %>%
  mutate(date = (time)) %>%
  arrange(date) %>%
  mutate(bband = apply(TTR::BBands(cbind(high, close, low)), 1, as.list)) %>%
  unnest_wider(bband, names_sep="_") %>%
  filter(date > max(date) - weeks(15)) %>%
  slice_max(order_by=date, n=100) %>%
  ggplot(aes(x=date, y=close)) +
  geom_ribbon(aes(ymin=bband_dn, ymax=bband_up), alpha=0.1, fill="gray96") +
  geom_line(aes(y=bband_mavg), size=1, color="gray96") +
  geom_rect(
    aes(ymin=low, ymax=high, xmin=date-hours(10), xmax=date+hours(10)),
    size=0, fill="gray80"
  )  +
  geom_rect(
    aes(ymin=open, ymax=close, xmin=date-hours(10), xmax=date+hours(10),
        fill=close>open),
    size=0
  ) +
  scale_x_datetime() +
  scale_fill_manual(values=c("#F58E8E", "#A9D3AB")) +
  theme(
    plot.margin=unit(rep(1.5, 4), "mm"),
    plot.background=element_rect(fill="#383C4A", size=0),
    panel.background=element_rect(fill="#383C4A"),
    panel.grid=element_line(color="#5C6370"),
    panel.grid.minor=element_blank(),
    strip.background=element_rect(fill="#5C6370"),
    legend.background=element_rect(fill="#383C4A"),
    legend.key=element_rect(fill="#383C4A"),
    legend.title=element_text(color="#FFFFFF"),
    legend.text=element_text(color="#FFFFFF"),
    legend.position="bottom",
    plot.title=element_text(
      color="#FFFFFF", hjust=0.5, vjust=0, margin=margin(0,0,3,0, unit="mm")
    ),
    axis.title=element_text(color="#FFFFFF"),
    axis.title.x=element_text(margin=margin(3,0,0,0, unit="mm")),
    axis.title.y.left=element_text(margin=margin(0,3,0,0, unit="mm")),
    axis.title.y.right=element_text(margin=margin(0,0,0,3, unit="mm")),
    axis.text=element_text(color="#FFFFFF"),
    axis.ticks=element_blank()
  )
