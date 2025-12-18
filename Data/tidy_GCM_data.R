# Tidy GCM outputs provided by Xinynang
# Objective:
# - Combine .csv files by GCM
# - Save as .parquet file with files partitioned by GCM

# Import libraries -------------------------------------------------------------
library(tidyverse)
library(arrow)

# get it prepared for linux
# - get all rainfall
# - run sensitivity on n window for everything with save files
# - pick n
# - save as parquet




# Import files -----------------------------------------------------------------

## Get file paths for hist and hist nat rainfall for mass import ===============
hist_precip_file_names <- dir(
  "./Data/Raw/NCI_CMIP6_CMIP_Amon_pr_20211112/",
  full.names = TRUE
)[1] # remove this

# hist_nat_precip_file_names <- dir(
#  "./Data/Raw/NCI_CMIP6_CMIP_Amon_pr_20211112/",
#  full.names = TRUE
# )

hist_nat_precip_test <- "./Data/Raw/NCI_CMIP6_DAMIP_Amon_pr_20211112/pr_Amon_ACCESS-CM2_hist-nat_r1i1p1f1_gn_185001-202012.csv" # remove this

### Use gauge id's replace V1 to V533 ##########################################
catchment_ids <- read_csv(
  file = "./Data/Raw/catchment_lon_lat_data_LP.csv",
  show_col_types = FALSE
) |>
  pull(ID)



## Observed precipitation data =================================================
CAMELS_precip <- read_csv(
  "./Data/Raw/precipitation_AGCD.csv",
  show_col_types = FALSE
  )


## best CO2 and non CO2 gauges from previous paper =============================
### extract gauges with evidence ratio > 10 (moderate) #########################
best_CO2_non_CO2_per_gauge <- read_csv(
  "./Data/best_CO2_non_CO2_per_catchment_CMAES.csv",
  show_col_types = FALSE
)

select_gauges <- best_CO2_non_CO2_per_gauge |>
  select(gauge, contains_CO2, AIC) |>
  distinct() |>
  pivot_wider(
    names_from = contains_CO2,
    values_from = AIC
  ) |>
  mutate(
    CO2_model = `TRUE`,
    non_CO2_model = `FALSE`,
    .keep = "unused"
  ) |>
  mutate(
    AIC_difference = CO2_model - non_CO2_model # CO2 is smaller than non-CO2 then negative and CO2 is better
  ) |>
  mutate(
    evidence_ratio = case_when(
      AIC_difference < 0 ~ exp(0.5 * abs(AIC_difference)), # when CO2 model is better
      AIC_difference > 0 ~ -exp(0.5 * abs(AIC_difference)) # when non-CO2 model is better
    )
  ) |>
  arrange(evidence_ratio) |> 
  filter(evidence_ratio > 10) |>  # moderately strong > 100, moderate is > 10
  pull(gauge)






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
      i = 5
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


rollapply <- function(x, n, f, ...) {
  
  offset <- trunc(n - 1)
  out <- rep(NA, length(x) - offset)
  
  for (i in (offset + 1):length(x)) {
    out[i] <- f(x[(i - offset):i], ...)
  }
  
  return(out)
}


# Hist precip tidy data --------------------------------------------------------
hist_precip_tidy_data <- do.call(
  rbind,
  c(lapply(hist_precip_file_names, add_columns_using_GCM_details), row.names = NULL)
  # I don't know how to make the discard row names warning not show - leave it
) |>
  as_tibble() |>
  relocate(
    GCM,
    ensemble_id,
    realisation,
    date_range,
    .after = 1
  ) |>
  # convert time_sum to YYYY-MM-DD
  mutate(
    date = as_date(time_sum, origin = get_origin_date_range(date_range)), # origin must change by file name
    .after = 1,
    .by = c(GCM, ensemble_id, realisation, date_range)
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
    values_to = "hist_precipitation_mm"
  ) 



# Hist nat precip tidy data ----------------------------------------------------
## repeat the code from hist precip tidy data

hist_nat_precip_tidy_data <- do.call(
  rbind,
  c(lapply(hist_nat_precip_test, add_columns_using_GCM_details), row.names = NULL)
  # I don't know how to make the discard row names warning not show - leave it
) |>
  as_tibble() |>
  relocate(
    GCM,
    ensemble_id,
    realisation,
    date_range,
    .after = 1
  ) |>
  # convert time_sum to YYYY-MM-DD
  mutate(
    date = as_date(time_sum, origin = get_origin_date_range(date_range)), # origin must change by file name, needs a .by in here
    .after = 1,
    .by = c(GCM, ensemble_id, realisation, date_range)
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
    values_to = "hist_nat_precipitation_mm"
  ) 




# Join precipitation datasets --------------------------------------------------
cool_seasons <- seq(from = 4, to = 9, by = 1)
warm_seasons <- c(10, 11, 12, 1, 2, 3)

GCM_precip_data <- hist_nat_precip_tidy_data |> 
  left_join(
    hist_precip_tidy_data,
    by = join_by(date, year, month, GCM, ensemble_id, realisation, gauge)
  ) |> 
  mutate(
    season = case_when(
      month %in% cool_seasons ~ "cool_season",
      month %in% warm_seasons ~ "warm_season",
      .default = NA
    )
  ) |> 
  summarise(
    sum_hist_season_precipitation = sum(hist_precipitation_mm),
    sum_hist_nat_season_precipitation = sum(hist_nat_precipitation_mm),
    .by = c(year, GCM, ensemble_id, realisation, gauge, season)
  ) |> 
  mutate(
    scale_term = sum_hist_nat_season_precipitation / sum_hist_season_precipitation
  ) 
  



# Observed data -----------------------------------------------------------------
seasonal_precip_obs <- CAMELS_precip |> 
  pivot_longer(
    cols = !c(year, month, day),
    names_to = "gauge",
    values_to = "precipitation_mm"
  ) |>
  # aggregate to monthly
  summarise(
    monthly_precipitation_mm = sum(precipitation_mm),
    .by = c(year, month, gauge)
  ) |> 
  # add seasonality tag
  mutate(
    season = case_when(
      month %in% cool_seasons ~ "cool_season",
      month %in% warm_seasons ~ "warm_season",
      .default = NA
    )
  ) |>
  # aggregated based on seasonality tag
  summarise(
    sum_season_obs_precipitation = sum(monthly_precipitation_mm),
    .by = c(year, gauge, season)
  ) 





# Apply moving window to smooth out scaling term -------------------------------


# Probably should write function that plots things to test how n impacts the results

# how does this account for all the GCMs?
# plotting all GCM seems unrealistic - take the median of ensembles, then median of GCMs
moving_window_length <- c(1, 5, 10, 20, 40)


moving_window_sensitivity <- function(moving_window_length) {
  
  
  GCM_precip_data_smooth <- GCM_precip_data |> 
    mutate(
      smoothed_scaled_term = rollapply(scale_term, n = moving_window_length, f = mean),
      # try different n terms for sensitivity test
      .by = c(GCM, ensemble_id, realisation, gauge)
    ) |> 
    arrange(gauge, year)
  
  
  all_precip_data <- GCM_precip_data_smooth |> 
    left_join(
      seasonal_precip_obs,
      by = join_by(year, gauge, season)
    ) |> 
    # GCM data goes back further than obs
    drop_na() |> 
    arrange(year, gauge) |> 
    # add row numbers for plotting
    group_by(
      GCM, ensemble_id, realisation, gauge
    ) |> 
    mutate(row_number = row_number()) |> 
    ungroup() |> 
    drop_na() |> # this is more hist_nat than hist - if missing drop
    mutate(
      scaled_hist_nat_rainfall = scale_term * sum_season_obs_precipitation,
      scaled_hist_nat_rainfall_smooth = smoothed_scaled_term * sum_season_obs_precipitation
    ) |> 
    arrange(desc(smoothed_scaled_term))
  
  # Plot observed vs. scaled nat historical rainfall
  smoothed_precip_data_plotting <- all_precip_data |> 
    pivot_longer(
      cols = c(sum_season_obs_precipitation, scaled_hist_nat_rainfall_smooth),
      names_to = "obs_vs_scaled",
      values_to = "seasonal_precipitation_mm"
    )
  
  smoothed_factor_rainfall_timeseries_plot <- smoothed_precip_data_plotting |> 
    filter(gauge %in% select_gauges) |> 
    ggplot(aes(x = row_number, y = seasonal_precipitation_mm, colour = obs_vs_scaled)) +
    geom_line() +
    theme_bw() +
    facet_wrap(~gauge, scales = "free_y")
  
  
  ggsave(
    file = paste0("moving_window_length_", moving_window_length, "smoothed_rainfall_timeseries_scaled_plot_comparison.pdf"),
    path = "./Figures/moving_window_sensitivity_precip_scale",
    plot = smoothed_factor_rainfall_timeseries_plot,
    device = "pdf",
    width = 1189,
    height = 841,
    units = "mm"
  )
  
}





# From the sensitivity select window, plot and save results --------------------
## Select ======================================================================
selected_window <- 10
GCM_precip_data_smooth <- GCM_precip_data |> 
  mutate(
    smoothed_scaled_term = rollapply(scale_term, n = selected_window, f = mean),
    # try different n terms for sensitivity test
    .by = c(GCM, ensemble_id, realisation, gauge)
  ) |> 
  arrange(gauge, year)


all_precip_data <- GCM_precip_data_smooth |> 
  left_join(
    seasonal_precip_obs,
    by = join_by(year, gauge, season)
  ) |> 
  # GCM data goes back further than obs
  drop_na() |> 
  arrange(year, gauge) |> 
  # add row numbers for plotting
  group_by(
    GCM, ensemble_id, realisation, gauge
  ) |> 
  mutate(row_number = row_number()) |> 
  ungroup() |> 
  drop_na() |> # this is more hist_nat than hist - if missing drop
  mutate(
    scaled_hist_nat_rainfall = scale_term * sum_season_obs_precipitation,
    scaled_hist_nat_rainfall_smooth = smoothed_scaled_term * sum_season_obs_precipitation
  ) |> 
  arrange(desc(smoothed_scaled_term))



## Save ========================================================================
# parquet files
# partition by GCM because that is how I will extract the data later









# testing - remove
precip_data_plotting <- all_precip_data |> 
  pivot_longer(
    cols = c(sum_season_obs_precipitation, scaled_hist_nat_rainfall),
    names_to = "obs_vs_scaled",
    values_to = "seasonal_precipitation_mm"
  )

rainfall_timeseries_plot <- precip_data_plotting |> 
  ggplot(aes(x = row_number, y = seasonal_precipitation_mm, colour = obs_vs_scaled)) +
  geom_line() +
  theme_bw() +
  facet_wrap(~gauge, scales = "free_y")


ggsave(
  file = "rainfall_timeseries_scaled_plot_comparison.pdf",
  plot = rainfall_timeseries_plot,
  device = "pdf",
  width = 1189,
  height = 841,
  units = "mm"
)



smoothed_precip_data_plotting <- all_precip_data |> 
  pivot_longer(
    cols = c(sum_season_obs_precipitation, scaled_hist_nat_rainfall_smooth),
    names_to = "obs_vs_scaled",
    values_to = "seasonal_precipitation_mm"
  )

smoothed_factor_rainfall_timeseries_plot <- smoothed_precip_data_plotting |> 
  filter(gauge %in% select_gauges) |> 
  ggplot(aes(x = row_number, y = seasonal_precipitation_mm, colour = obs_vs_scaled)) +
  geom_line() +
  theme_bw() +
  facet_wrap(~gauge, scales = "free_y")


ggsave(
  file = "smoothed_rainfall_timeseries_scaled_plot_comparison.pdf",
  plot = smoothed_factor_rainfall_timeseries_plot,
  device = "pdf",
  width = 1189,
  height = 841,
  units = "mm"
)


# from the plot unscaled is unacceptable for some catchments
# unrealistic rainfall - hit all catchments scaling factor with a moving window


scale_term_timeseries <- all_precip_data |>
  ggplot(aes(x = row_number, y = scale_term)) + 
  geom_line() +
  theme_bw() +
  facet_wrap(~gauge, scales = "free_y")


ggsave(
  file = "scale_term_timeseries.pdf",
  plot = scale_term_timeseries,
  device = "pdf",
  width = 1189,
  height = 841,
  units = "mm"
)

# Save precipitation data
# - something is wrong with some catchments - looks like they are plotting double - wiggle as expected - due to large changes in seasonal rainfall

# TODO:
# - clean/tidy/organise R script, file and save locations
# - prep script for all files - do on linux pc
# - sensitivity on window for smoothing
# - save as a parquet file
# - determine partitioning - most likely by GCM
