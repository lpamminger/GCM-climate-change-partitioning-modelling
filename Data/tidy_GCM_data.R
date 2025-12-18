# Tidy GCM outputs provided by Xinynang
# What this file does:
# - turns many csv results from GCM into a single parquet file

# Import libraries -------------------------------------------------------------
library(tidyverse)
library(arrow)


# Import files -----------------------------------------------------------------

## Get file paths for hist and hist nat rainfall for mass import ===============
hist_precip_file_names <- dir(
  "./Data/Raw/NCI_CMIP6_CMIP_Amon_pr_20211112/",
  full.names = TRUE
)

hist_nat_precip_file_names <- dir(
  "./Data/Raw/NCI_CMIP6_DAMIP_Amon_pr_20211112/",
  full.names = TRUE
) # remove this



### Use gauge id's replace V1 to V533 ##########################################
catchment_ids <- read_csv(
  file = "./Data/Raw/catchment_ID_keys.csv",
  show_col_types = FALSE
) |>
  pull(ID)




# Functions for tidying data ---------------------------------------------------
get_GCM_details_from_file_name <- function(file_name) {
  # file names are provide the entire path
  # just get the name of the file
  removed_csv <- str_split(
    file_name,
    pattern = "\\."
  ) |>
    unlist()

  removed_csv[2] |> # 2nd index is where the data is
    str_split_i(
      pattern = "/",
      i = 6
    ) |>
    str_split(
      pattern = "_"
    ) |>
    unlist() |>
    `names<-`(c("variable", "time_step", "GCM", "experiment", "ensemble_id", "realisation", "date_range"))
}


add_columns_using_GCM_details <- function(x) {
  cbind(
    read_csv(x, show_col_types = FALSE),
    GCM = get_GCM_details_from_file_name(x)["GCM"],
    ensemble_id = get_GCM_details_from_file_name(x)["ensemble_id"],
    experiment = get_GCM_details_from_file_name(x)["experiment"],
    realisation = get_GCM_details_from_file_name(x)["realisation"],
    date_range = get_GCM_details_from_file_name(x)["date_range"]
  )
}


get_origin_date_range <- function(date_range) {
  date_range |>
    # if not unique throw error because something is not right
    unique() |>
    str_split(
      pattern = "-"
    ) |>
    unlist() |>
    ym() |>
    min() |>
    as_date()
}

convert_from_kg_per_m2_per_s_to_mm_month <- function(precipitation_kg_per_m2_per_s, date_object) {
  precipitation_kg_per_m2_per_s * 60 * 60 * 24 * days_in_month(date_object)
}




cool_seasons <- seq(from = 4, to = 9, by = 1)
warm_seasons <- c(10, 11, 12, 1, 2, 3)

simplify_GCM_data <- function(file_names, chunk) {
  gc()

  do.call(
    rbind,
    c(lapply(file_names, add_columns_using_GCM_details), row.names = NULL)
    # I don't know how to make the discard row names warning not show - leave it
  ) |>
    as_tibble() |>
    relocate(
      GCM,
      ensemble_id,
      experiment,
      realisation,
      date_range,
      .after = 1
    ) |>
    # convert time_sum to YYYY-MM-DD
    mutate(
      date = as_date(time_sum, origin = get_origin_date_range(date_range)), # origin must change by file name
      .after = 1,
      .by = c(GCM, ensemble_id, experiment, realisation, date_range)
    ) |>
    # drop time_sum and date range
    select(!c(time_sum, date_range)) |>
    # extract year and month into other columns
    mutate(
      year = year(date),
      month = month(date),
      .after = 1
    ) |>
    # convert precip from kg m-2 s-1 to mm/month
    mutate(
      across(V1:V533, \(x) convert_from_kg_per_m2_per_s_to_mm_month(x, date))
    ) |>
    # convert V1 to V533 into gauge id's
    rename_with(
      ~catchment_ids,
      starts_with("V")
    ) |>
    pivot_longer(
      cols = all_of(catchment_ids),
      names_to = "gauge",
      values_to = "precipitation_mm"
    ) |>
    # aggregate into seasons (warm/cool)
    mutate(
      season = case_when(
        month %in% cool_seasons ~ "cool_season",
        month %in% warm_seasons ~ "warm_season",
        .default = NA
      )
    ) |>
    summarise(
      season_precipitation_mm = sum(precipitation_mm),
      .by = c(year, GCM, ensemble_id, experiment, realisation, gauge, season)
    ) |>
    write_parquet(
      sink = paste0("./Data/Tidy/GCMs/precip_chunk_", chunk, ".parquet")
    )
}


# Tidy and save GCM outputs results --------------------------------------------
precip_file_names <- c(hist_precip_file_names, hist_nat_precip_file_names)

## Chunk files for repetition ==================================================
# all paths = 676,  388 too much,  169 is too much, 85 = 0.8 Mb
chunk_length <- 28L # 1064 / 14 ~ 76 per chunk is good
chunks <- seq(from = 1, to = chunk_length) |>
  rep(each = length(precip_file_names) / chunk_length) |>
  as.factor()

chunked_file_names <- split(x = precip_file_names, f = chunks)


## Step 1. Save chunked files ==================================================
iwalk(
  .x = chunked_file_names,
  .f = simplify_GCM_data
)

## Step 2. Combine chunked files and save a larger file ========================
### and Remove chunked files
### Limited RAM means I cannot do step 1 and 2 together

tidy_GCM_data <- open_dataset(
  sources = "./Data/Tidy/GCMs",
  format = "parquet"
) |>
  collect() |>
  pivot_wider(
    names_from = experiment,
    values_from = season_precipitation_mm
  )

glimpse(tidy_GCM_data)

write_dataset(
  dataset = tidy_GCM_data,
  path = "./Data/Tidy",
  format = "parquet"
)

# Remove GCMs folder manually
