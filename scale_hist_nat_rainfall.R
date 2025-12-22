# Scale hist-nat rainfall
# What this file does:
# - calculate hist-nat rainfall scale
# - perform smoothing on rainfall scale
# - save hist nat rainfall



# Import libraries -------------------------------------------------------------
library(tidyverse)
library(arrow)

# Import data ------------------------------------------------------------------

## Get gauges I am interested in  ==============================================
### From previous paper get evidence ratio > 10 (moderate)
### This will be used to filter observed and GCM precipitation
best_CO2_non_CO2_per_gauge <- read_csv(
  "./Previous/Results/best_CO2_non_CO2_per_catchment_CMAES.csv",
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
  filter(evidence_ratio > 10) |> # moderately strong > 100, moderate is > 10
  pull(gauge)



## Observed precipitation data =================================================
CAMELS_precip <- read_csv(
  "./Data/Raw/CAMELSAUS_v2_precipitation_AGCD.csv",
  show_col_types = FALSE
)


## GCM precipitation data ======================================================
GCM_precip <- open_dataset(
  sources = "./Data/Tidy/part-0.parquet"
) |>
  rename(
    hist_nat = `hist-nat`
  ) |>
  # remove gauges not interested in
  filter(gauge %in% select_gauges) |> 
  collect() #|> 
  #arrange(year)






# Tidy Observed Precip ---------------------------------------------------------
cool_seasons <- seq(from = 4, to = 9, by = 1)
warm_seasons <- c(10, 11, 12, 1, 2, 3)


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
    season_obs_precipitation = sum(monthly_precipitation_mm),
    .by = c(year, gauge, season)
  ) |>
  filter(gauge %in% select_gauges)





# Calculate scale_term ---------------------------------------------------------
all_precipitation_data <- GCM_precip |>
  # remove GCMs without a hist and hist-nat equivalent - must be `collected` to do this
  drop_na() |>
  mutate(
    naive_scale_term = hist_nat / historical
  )



## Plot naive scaling term =====================================================
naive_scale_term <- all_precipitation_data |>
  # need to account for GCM and ensemble members
  # get median of ensemble_id and realisation for each GCM, gauge, year and season
  summarise(
    median_naive_scale_term = median(naive_scale_term),
    max_naive_scale_term = max(naive_scale_term),
    .by = c(GCM, gauge, year, season)
  ) |>
  # get median of GCM ensembles
  summarise(
    median_GCM_naive_scale_term = median(median_naive_scale_term),
    max_GCM_naive_scale_term = max(max_naive_scale_term),
    .by = c(gauge, year, season)
  ) |>
  left_join(
    seasonal_precip_obs,
    by = join_by(year, gauge, season)
  ) |>
  # GCM length > obs length --> drop missing
  drop_na()


# Quick numerical check
naive_scale_term |>
  select(median_GCM_naive_scale_term, max_GCM_naive_scale_term) |>
  summary()

# I will be applying the median in the models focus on that
# The large maximum scaling factors suggest GCM ensemble variability will be high


naive_scale_term_plot <- naive_scale_term |>
  # scale obs
  mutate(
    median_naive_scaled_hist_nat = median_GCM_naive_scale_term * season_obs_precipitation
  ) |>
  group_by(
    gauge
  ) |>
  mutate(index = row_number()) |>
  ungroup() |>
  pivot_longer(
    cols = c(median_naive_scaled_hist_nat, season_obs_precipitation),
    names_to = "obs_vs_scaled",
    values_to = "seasonal_precipitation_mm"
  ) |>
  ggplot(aes(x = index, y = seasonal_precipitation_mm, colour = obs_vs_scaled)) +
  geom_line() +
  theme_bw() +
  facet_wrap(~gauge, scales = "free_y")


ggsave(
  file = "naive_median_GCM_scaled_hist_nat_timeseries.pdf",
  path = "./Figures/scaling_rainfall",
  plot = naive_scale_term_plot,
  device = "pdf",
  width = 1189,
  height = 841,
  units = "mm"
)



# The median GCM outputs look reasonable without scaling
# The maximum GCM outputs do not look reasonable
# To be on the safe side smooth the scaling terms
# The maximum GCM scaling terms cannot be helped with scaling - leave for now
# Do I do it for all GCM and ensemble members?

# Apply moving window to smooth out scaling term -------------------------------

rollapply <- function(x, n, f, ...) {
  offset <- trunc(n - 1)
  out <- rep(NA, length(x) - offset)

  for (i in (offset + 1):length(x)) {
    out[i] <- f(x[(i - offset):i], ...)
  }

  return(out)
}

moving_window_sensitivity_test <- function(moving_window_length) {
  smooth_scale_term <- all_precipitation_data |>
    mutate(
      smooth_scale_term = rollapply(naive_scale_term, n = moving_window_length, f = mean),
      .by = c(GCM, ensemble_id, realisation, gauge)
    ) |>
    summarise(
      median_smooth_scale_term = median(smooth_scale_term),
      p90_smooth_scale_term = quantile(smooth_scale_term, probs = 0.9, na.rm = T),
      .by = c(GCM, gauge, year, season)
    ) |>
    # get median of GCM ensembles
    summarise(
      median_GCM_smooth_scale_term = median(median_smooth_scale_term),
      median_p90_GCM_smooth_scale_term = median(p90_smooth_scale_term),
      .by = c(gauge, year, season)
    ) |>
    left_join(
      seasonal_precip_obs,
      by = join_by(year, gauge, season)
    ) |>
    # GCM length > obs length --> drop missing
    drop_na()


  smooth_scale_term_plot <- smooth_scale_term |>
    # scale obs using either median or max
    mutate(
      median_p90_smooth_scaled_hist_nat = median_p90_GCM_smooth_scale_term * season_obs_precipitation
    ) |>
    group_by(
      gauge
    ) |>
    mutate(index = row_number()) |>
    ungroup() |>
    pivot_longer(
      cols = c(median_p90_smooth_scaled_hist_nat, season_obs_precipitation),
      names_to = "obs_vs_scaled",
      values_to = "seasonal_precipitation_mm"
    ) |>
    ggplot(aes(x = index, y = seasonal_precipitation_mm, colour = obs_vs_scaled)) +
    geom_line() +
    theme_bw() +
    facet_wrap(~gauge, scales = "free_y")


  ggsave(
    file = paste0("median_p90_smooth_scaled_window_", moving_window_length, "_timeseries.pdf"),
    path = "./Figures/scaling_rainfall",
    plot = smooth_scale_term_plot,
    device = "pdf",
    width = 1189,
    height = 841,
    units = "mm"
  )
}

walk(
  .x = c(5, 10, 20), # 5 year, 10 year, 20 year, 40 year - double because cool/warm season
  .f = moving_window_sensitivity_test
)




# Double check the scaling term for each GCM and ensemble combination ----------
# Based of the sensitivity graphs n = 10 looks good
smooth_scale_term <- all_precipitation_data |>
  # order of years matter when smoothing
  arrange(gauge, GCM, ensemble_id, year) |> 
  mutate(
    smooth_scale_term = rollapply(naive_scale_term, n = 5, f = mean),
    .by = c(GCM, ensemble_id, realisation, gauge)
  ) |>
  left_join(
    seasonal_precip_obs,
    by = join_by(year, gauge, season)
  ) |>
  # GCM length > obs length --> drop missing
  drop_na() |>
  mutate(
    smooth_hist_nat = smooth_scale_term * season_obs_precipitation
  )


# some years removed after smoothing
#x <- smooth_scale_term |> 
#  filter(GCM == "BCC-CSM2-MR") |> 
#  filter(ensemble_id == "r1i1p1f1") |> 
#  filter(gauge == "224213") |> 
#  arrange(year)

# Quick numbers check
smooth_scale_term |>
  ggplot(aes(x = smooth_scale_term)) +
  geom_histogram() +
  theme_bw()

ecdf_smooth_scale_function <- smooth_scale_term |>
  pull(smooth_scale_term) |>
  ecdf()


x <- seq(from = 0, to = 8.3, length.out = 100)
y <- ecdf_smooth_scale_function(x)

ecdf_scaled_term_plot <- tibble(
  x = x,
  y = y
) |>
  ggplot(aes(x = x, y = y)) +
  geom_line() +
  theme_bw() +
  labs(
    x = "Scaling Term",
    y = "F(Scaling Term)",
    title = "Empirical CDF of hist-nat scaling term",
    subtitle = "Moving window of 5 years"
  ) +
  theme(
    plot.title = element_text(hjust = 0.5),
    plot.subtitle = element_text(hjust = 0.5)
  )

ggsave(
  file = "ecdf_window_5_scale_term.pdf",
  path = "./Figures/scaling_rainfall",
  plot = ecdf_scaled_term_plot,
  device = "pdf",
  width = 297,
  height = 210,
  units = "mm"
)

# Pretty much all values are less than (> 4000 are not)
# Not perfect, I think it should be fine

# Save the results -------------------------------------------------------------
smooth_scale_term |>
  select(!naive_scale_term) |>
  write_parquet(
    sink = "./Results/scale_term/scaled_hist_nat_rainfall.parquet"
  )
