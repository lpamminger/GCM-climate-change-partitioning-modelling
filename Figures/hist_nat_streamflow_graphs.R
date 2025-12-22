# Produce graphs using hist_nat_streamflow results -----------------------------


# Import libraries -------------------------------------------------------------
pacman::p_load(tidyverse, truncnorm, sloop, arrow, furrr)


# Import data ------------------------------------------------------------------

## hist nat streamflow =========================================================
## Data set contains GCM = observed and ensemble_id = observed
### --> this uses observed rainfall with the CO2 component turned off

### Remove gauges with a time of activation > 2014
### HARD CODED FROM RQ2
gauge_ToE_greater_than_2014 <- c("401210", "410156", "411003", "603007", "204036", "224213")

hist_nat_streamflow_data <- open_dataset(
  source = "./Results/hist_nat_streamflow_data.parquet"
) |>
  collect() |>
  filter(!gauge %in% gauge_ToE_greater_than_2014)


## cmaes streamflow ============================================================
high_evidence_ratio_gauges <- hist_nat_streamflow_data |>
  pull(gauge) |>
  unique()

best_CO2_model_per_catchment_CMAES <- read_csv(
  "Previous/Results/best_CO2_non_CO2_per_catchment_CMAES.csv",
  show_col_types = FALSE
) |>
  filter(contains_CO2) |>
  filter(gauge %in% high_evidence_ratio_gauges) |>
  select(gauge, streamflow_model)

cmaes_streamflow <- read_csv(
  "./Previous/Results/cmaes_streamflow_results.csv",
  show_col_types = FALSE
) |>
  semi_join(
    best_CO2_model_per_catchment_CMAES,
    by = join_by(gauge, streamflow_model)
  ) |>
  filter(!gauge %in% gauge_ToE_greater_than_2014)



# GCM meta information ---------------------------------------------------------
GCM_meta_info <- hist_nat_streamflow_data |>
  select(GCM, ensemble_id) |>
  distinct() |>
  summarise(
    n = n(),
    .by = GCM
  )



# Comparing streamflow timeseries ----------------------------------------------

## hist_nat ====================================================================
hist_nat_plotting_data <- hist_nat_streamflow_data |>
  filter(GCM != "observed") |>
  # Ensemble median per GCM
  summarise(
    median_ensemble_realspace_streamflow = median(realspace_streamflow),
    .by = c(year, GCM, gauge)
  ) |>
  # GCM median
  summarise(
    median_GCM_realspace_streamflow = median(median_ensemble_realspace_streamflow),
    max_GCM_realspace_streamflow = max(median_ensemble_realspace_streamflow),
    min_GCM_realspace_streamflow = min(median_ensemble_realspace_streamflow),
    .by = c(year, gauge)
  ) |>
  add_column(
    type = "Counterfactual - Hist Nat Precipitation"
  )

## obs precip CO2 off ==========================================================
obs_CO2_off_plotting_data <- hist_nat_streamflow_data |>
  filter(GCM == "observed") |>
  select(year, gauge, realspace_streamflow) |>
  rename(median_GCM_realspace_streamflow = realspace_streamflow) |>
  add_column(
    max_GCM_realspace_streamflow = NA,
    min_GCM_realspace_streamflow = NA,
    type = "Counterfactual - Observed Precipitation"
  )

# obs precip CO2 on ============================================================
obs_CO2_on_plotting_data <- cmaes_streamflow |>
  # GCM data stops at 2014 - make end year same
  filter(year <= 2014) |>
  select(year, gauge, realspace_modelled_streamflow) |>
  rename(median_GCM_realspace_streamflow = realspace_modelled_streamflow) |>
  add_column(
    max_GCM_realspace_streamflow = NA,
    min_GCM_realspace_streamflow = NA,
    type = "CO2 Model - Observed Precipitation"
  )


## join them up for plotting ===================================================
all_plotting_data <- rbind(hist_nat_plotting_data, obs_CO2_off_plotting_data, obs_CO2_on_plotting_data) |>
  # factor for order
  mutate(
    type = factor(type, levels = c("CO2 Model - Observed Precipitation", "Counterfactual - Observed Precipitation", "Counterfactual - Hist Nat Precipitation"))
  )

## plot and save ===============================================================
plot <- all_plotting_data |>
  ggplot(aes(x = year, y = median_GCM_realspace_streamflow, colour = type, fill = type)) +
  geom_ribbon(
    aes(x = year, ymin = min_GCM_realspace_streamflow, ymax = max_GCM_realspace_streamflow),
    alpha = 0.1,
    inherit.aes = FALSE,
    data = hist_nat_plotting_data,
    fill = "#7570b3"
  ) +
  geom_line() +
  theme_bw() +
  labs(
    y = "Streamflow (mm)",
    x = "Year",
    colour = "Modelled Streamflow Method"
  ) +
  scale_colour_brewer(palette = "Dark2") +
  facet_wrap(~gauge, scales = "free_y")


ggsave(
  file = "modelled_streamflow_timeseries.pdf",
  path = "./Figures",
  plot = plot,
  device = "pdf",
  width = 1189,
  height = 841,
  units = "mm"
)
